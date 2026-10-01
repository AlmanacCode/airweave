"""Unit tests for FileService — save_bytes, restore_from_arf, cleanup, validation."""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest

from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService


def _make_service(tmpdir: str) -> FileService:
    """Create FileService with a real temp dir and mock storage backend."""
    sync_job_id = uuid4()
    storage = MagicMock()
    storage.read_file = AsyncMock()
    storage.write_file = AsyncMock()
    storage.delete_directory = AsyncMock()

    with patch("airweave.domains.storage.file_service.paths.temp_sync_dir", return_value=tmpdir):
        svc = FileService(sync_job_id=sync_job_id, storage_backend=storage)

    return svc, storage


class TestSaveBytes:
    @pytest.mark.asyncio
    async def test_saves_pdf_bytes_and_sets_local_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, _ = _make_service(tmpdir)
            entity = MagicMock()
            logger = MagicMock()

            result = await svc.save_bytes(
                entity=entity,
                content=b"%PDF-1.4 test content",
                filename_with_extension="report.pdf",
                logger=logger,
            )

            assert result is entity
            assert entity.local_path is not None
            assert entity.local_path.endswith(".pdf")
            assert os.path.exists(entity.local_path)

    @pytest.mark.asyncio
    async def test_raises_file_skipped_for_unsupported_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, _ = _make_service(tmpdir)

            with pytest.raises(FileSkippedException, match="Unsupported"):
                await svc.save_bytes(
                    entity=MagicMock(),
                    content=b"binary",
                    filename_with_extension="file.xyz_unsupported",
                    logger=MagicMock(),
                )

    @pytest.mark.asyncio
    async def test_raises_value_error_when_no_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, _ = _make_service(tmpdir)

            with pytest.raises(ValueError, match="must include file extension"):
                await svc.save_bytes(
                    entity=MagicMock(),
                    content=b"data",
                    filename_with_extension="no_extension",
                    logger=MagicMock(),
                )

    @pytest.mark.asyncio
    async def test_raises_file_skipped_for_oversized_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, _ = _make_service(tmpdir)
            huge = b"x" * (FileService.MAX_FILE_SIZE_BYTES + 1)

            with pytest.raises(FileSkippedException, match="too large"):
                await svc.save_bytes(
                    entity=MagicMock(),
                    content=huge,
                    filename_with_extension="giant.pdf",
                    logger=MagicMock(),
                )


class TestRestoreFromArf:
    @pytest.mark.asyncio
    async def test_restores_file_to_temp_and_returns_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, storage = _make_service(tmpdir)
            storage.read_file = AsyncMock(return_value=b"file content here")

            path = await svc.restore_from_arf(
                arf_file_path="raw/sync-123/files/entity.pdf",
                filename="report.pdf",
                logger=MagicMock(),
            )

            assert path.startswith(tmpdir)
            assert path.endswith(".pdf")
            assert os.path.exists(path)
            storage.read_file.assert_awaited_once_with("raw/sync-123/files/entity.pdf")


class TestCleanupSyncDirectory:
    @pytest.mark.asyncio
    async def test_removes_existing_temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, _ = _make_service(tmpdir)
            # Create a file inside to confirm removal
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")

            await svc.cleanup_sync_directory(logger=MagicMock())

            assert not os.path.exists(tmpdir)

    @pytest.mark.asyncio
    async def test_does_not_raise_when_dir_already_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            svc, _ = _make_service(tmpdir)

        # tmpdir has been deleted by the context manager exit
        await svc.cleanup_sync_directory(logger=MagicMock())


class TestCanonicalBlobs:
    @pytest.mark.asyncio
    async def test_canonical_capture_keeps_original_bytes_and_cleans_temp(self, tmp_path):
        from airweave.domains.sources.token_providers.static import StaticTokenProvider

        svc, storage = _make_service(str(tmp_path))
        svc.sync_id = uuid4()
        content = b"original unsupported format"

        async def download(client, url, headers, destination, logger, *, follow_redirects=True):
            with open(destination, "wb") as output:
                output.write(content)

        svc._stream_download = download
        blob = await svc.capture_canonical_url(
            "https://example.com/file.unknown",
            MagicMock(),
            StaticTokenProvider("token"),
            MagicMock(),
        )
        assert blob.key.startswith(f"canonical/{svc.sync_id}/blobs/sha256/")
        storage.write_file.assert_awaited_once_with(blob.key, content)
        assert not list(tmp_path.iterdir())

    @pytest.mark.asyncio
    async def test_storage_failure_does_not_publish_reference(self, tmp_path):
        svc, storage = _make_service(str(tmp_path))
        svc.sync_id = uuid4()
        storage.write_file.side_effect = OSError("storage down")
        with pytest.raises(OSError, match="storage down"):
            await svc.store_canonical_blob(b"bytes")


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical", [True, False])
@pytest.mark.parametrize("signed", [True, False])
async def test_401_refresh_requires_original_bearer_request(tmp_path, canonical, signed):
    from airweave.domains.sources.token_providers.protocol import TokenProviderProtocol
    from airweave.platform.http_client.airweave_client import AirweaveHttpClient

    auth = MagicMock(spec=TokenProviderProtocol)
    auth.supports_refresh = True
    auth.get_token = AsyncMock(return_value="fixture-old")
    auth.force_refresh = AsyncMock(return_value="fixture-new")
    requests = []

    async def download(request):
        if request.method == "HEAD":
            return httpx.Response(200)
        requests.append(request.headers.get("Authorization"))
        return httpx.Response(401 if len(requests) == 1 else 200, content=b"original")

    service, storage = _make_service(str(tmp_path))
    service.sync_id = uuid4()
    url = "https://files.example/original.pdf"
    if signed:
        url += "?X-Amz-Algorithm=fixture"
    async with httpx.AsyncClient(transport=httpx.MockTransport(download)) as raw:
        client = AirweaveHttpClient(raw, uuid4(), "fixture", feature_flag_enabled=False)
        if canonical:
            operation = service.capture_canonical_url(url, client, auth, MagicMock())
        else:
            entity = MagicMock(name="entity")
            entity.name, entity.url = "original.pdf", url
            operation = service.download_from_url(entity, client, auth, MagicMock())
        if signed:
            with pytest.raises(httpx.HTTPStatusError):
                await operation
            auth.force_refresh.assert_not_awaited()
            assert requests == [None]
            storage.write_file.assert_not_awaited()
            assert not list(tmp_path.iterdir())
        else:
            await operation
            auth.force_refresh.assert_awaited_once()
            assert requests == ["Bearer fixture-old", "Bearer fixture-new"]


@pytest.mark.asyncio
async def test_canonical_can_reject_redirect_without_storing_its_body(tmp_path):
    from airweave.domains.sources.token_providers.static import StaticTokenProvider
    from airweave.platform.http_client.airweave_client import AirweaveHttpClient

    service, storage = _make_service(str(tmp_path))
    service.sync_id = uuid4()
    calls = []

    async def download(request):
        if request.method == "HEAD":
            return httpx.Response(200)
        calls.append(request.url.path)
        if request.url.path == "/original.pdf":
            return httpx.Response(302, headers={"location": "/target.pdf"}, content=b"redirect")
        return httpx.Response(200, content=b"original")

    async with httpx.AsyncClient(transport=httpx.MockTransport(download)) as raw:
        client = AirweaveHttpClient(raw, uuid4(), "fixture", feature_flag_enabled=False)
        with pytest.raises(httpx.HTTPStatusError) as error:
            await service.capture_canonical_url(
                "https://files.example/original.pdf",
                client,
                StaticTokenProvider("fixture"),
                MagicMock(),
                follow_redirects=False,
            )
        assert error.value.response.status_code == 302
        assert calls == ["/original.pdf"]
        storage.write_file.assert_not_awaited()
        assert not list(tmp_path.iterdir())
        # Existing callers retain their redirect behavior without a new argument.
        blob = await service.capture_canonical_url(
            "https://files.example/original.pdf",
            client,
            StaticTokenProvider("fixture"),
            MagicMock(),
        )
        assert calls == ["/original.pdf", "/original.pdf", "/target.pdf"]
        storage.write_file.assert_awaited_once_with(blob.key, b"original")

        entity = MagicMock(name="entity")
        entity.name, entity.url = "original.pdf", "https://files.example/original.pdf"
        downloaded = await service.download_from_url(
            entity, client, StaticTokenProvider("fixture"), MagicMock()
        )
        assert calls[-2:] == ["/original.pdf", "/target.pdf"]
        with open(downloaded.local_path, "rb") as original:
            assert original.read() == b"original"
