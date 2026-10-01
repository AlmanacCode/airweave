"""Synthetic native interpretation; SQL page durability is tested by the shared engine suite."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import BlobReference, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceEntityNotFoundError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import OutlookMailConfig
from airweave.platform.sources.outlook_graph import OutlookBoundaryError, OutlookGraphClient
from airweave.platform.sources.outlook_mail_capture import OutlookMailCapture
from airweave.platform.sources.outlook_mail_models import OutlookMailContinuation

MIME = b"From: from@example.com\r\nTo: to@example.com\r\nSubject: Original\r\n\r\nFull body\r\n"
MESSAGE = {
    "id": "immutable-message",
    "changeKey": "version-one",
    "parentFolderId": "folder-b",
    "subject": "Original",
    "from": {"emailAddress": {"address": "from@example.com"}},
    "sender": {"emailAddress": {"address": "delegate@example.com"}},
    "body": {"contentType": "text", "content": "Full body"},
    "bodyPreview": "Full",
    "hasAttachments": False,
    "nativeExtension": {"keep": [1, True]},
}
ROOT = CompletedScope(record_type="message")


def files(maximum=10000):
    result = MagicMock()
    result.MAX_FILE_SIZE_BYTES = maximum
    result.store_canonical_blob = AsyncMock(
        return_value=BlobReference(
            key="original", sha256="a" * 64, size_bytes=len(MIME), media_type="message/rfc822"
        )
    )
    return result


async def capture(client, *, config=None):
    return await OutlookMailCapture.create(
        graph=OutlookGraphClient(
            StaticTokenProvider("secret"), client, "outlook_mail", "principal"
        ),
        config=config
        or OutlookMailConfig(
            expected_principal_id="principal", included_folders=[], excluded_folders=[]
        ),
    )


def initial(source):
    return ScanContinuation(
        value=OutlookMailContinuation(fingerprint=source.fingerprint).model_dump(mode="json")
    )


@pytest.mark.asyncio
async def test_full_page_retains_exact_json_and_mime_and_resumes_pending_id():
    requested = []
    fail_second = True

    def graph(request):
        requested.append(request.url.path)
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        assert request.headers["Prefer"] == 'IdType="ImmutableId"'
        if request.url.path == "/v1.0/me/messages":
            return httpx.Response(
                200, json={"value": [{"id": "immutable-message"}, {"id": "second"}]}
            )
        if request.url.path.endswith("/$value"):
            if fail_second and "/second/" in request.url.path:
                raise httpx.ReadError("private-url-failure")
            return httpx.Response(200, content=MIME, headers={"content-type": "message/rfc822"})
        mid = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json={**MESSAGE, "id": mid})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        retained = files()
        first = await source.capture_page(ROOT, initial(source), files=retained)
        assert first.records[0].payload == MESSAGE
        assert first.records[0].parent is None
        assert first.records[0].completeness == "partial"
        assert first.records[0].blobs[0].source_path is None
        assert not first.final
        retained.store_canonical_blob.assert_awaited_once_with(MIME, media_type="message/rfc822")
        saved = first.continuation.model_copy(deep=True)
        with pytest.raises(httpx.RequestError) as error:
            await source.capture_page(ROOT, saved, files=retained)
        assert "private-url-failure" not in str(error.value)
        assert saved == first.continuation
        fail_second = False
        recreated = await capture(client)
        second = await recreated.capture_page(ROOT, saved, files=retained)
        assert second.final and second.records[0].identity.native_id == "second"
        assert requested.count("/v1.0/me/messages") == 1


@pytest.mark.asyncio
async def test_current_folder_decides_move_membership_and_ambiguous_404_stops():
    message = dict(MESSAGE)
    unavailable = False

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if "/mailFolders/" in request.url.path:
            return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})
        if unavailable:
            return httpx.Response(404, json={"error": {"code": "ErrorItemNotFound"}})
        if request.url.path.endswith("/$value"):
            return httpx.Response(200, content=MIME, headers={"content-type": "text/plain"})
        return httpx.Response(200, json=message)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(
            client,
            config=OutlookMailConfig(
                expected_principal_id="principal",
                included_folders=["folder-a", "folder-b"],
                excluded_folders=["excluded"],
            ),
        )
        retained = files()
        # Duplicate discoveries before/after a folder move keep the same root identity.
        first = await source.message(message["id"], files=retained)
        message["parentFolderId"] = "folder-a"
        second = await source.message(message["id"], files=retained)
        assert first.identity == second.identity and first.kind == second.kind == "upsert"
        assert first.parent is second.parent is None
        message["parentFolderId"] = "excluded"
        outside = await source.message(message["id"], files=retained)
        assert outside.removal_reason == "scope_removed"
        assert retained.store_canonical_blob.await_count == 2
        unavailable = True
        previous = SourceRecord(
            id=uuid4(),
            sync_id=uuid4(),
            identity=first.identity,
            revision=1,
            payload=first.payload,
            payload_schema_version=1,
            capture_hash="a" * 64,
            content_hash=None,
            completeness=first.completeness,
            observed_at=first.observed_at,
            source_created_at=None,
            source_updated_at=None,
            deleted_at=None,
            removal_reason=None,
            blobs=first.blobs,
            indexed_revision=None,
            indexed_pipeline_version=None,
        )
        with pytest.raises(SourceEntityNotFoundError):
            await source.refresh_known(previous, files=retained)


@pytest.mark.asyncio
async def test_cursor_binding_and_wrong_collection_fail_before_message_reads():
    requested = []
    next_link = "https://graph.microsoft.com/v1.0/me/messages?$skip=next"

    def graph(request):
        requested.append(request.url.path)
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        return httpx.Response(200, json={"value": [], "@odata.nextLink": next_link})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        wrong = initial(source).model_copy(update={"value": {"fingerprint": "a" * 64}})
        with pytest.raises(OutlookBoundaryError, match="another capture"):
            await source.capture_page(ROOT, wrong, files=files())
        assert requested == ["/v1.0/me"]
        next_link = "https://graph.microsoft.com/v1.0/me/events"
        with pytest.raises(OutlookBoundaryError, match="changed the message collection"):
            await source.capture_page(ROOT, initial(source), files=files())
        next_link = "https://graph.microsoft.com/v1.0/me/messages?token=" + "x" * 66000
        with pytest.raises(ValueError, match="64 KiB"):
            await source.capture_page(ROOT, initial(source), files=files())


@pytest.mark.asyncio
async def test_mime_size_omission_is_explicit_and_change_key_race_cannot_commit():
    changed = False

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if request.url.path.endswith("/$value"):
            return httpx.Response(
                200, content=MIME, headers={"content-type": "Message/RFC822; charset=utf-8"}
            )
        value = dict(MESSAGE)
        if changed and "$select" in request.url.params:
            value["changeKey"] = "new-version"
        return httpx.Response(200, json=value)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        bounded = files(maximum=8)
        original = await source.message(MESSAGE["id"], files=bounded)
        assert original.completeness == "metadata_only" and original.blobs == ()
        assert original.payload == MESSAGE
        bounded.store_canonical_blob.assert_not_awaited()
        changed = True
        with pytest.raises(OutlookBoundaryError, match="changed during"):
            await source.message(MESSAGE["id"], files=files())


@pytest.mark.asyncio
async def test_missing_received_date_cannot_broaden_date_selection():
    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        assert not request.url.path.endswith("/$value")
        return httpx.Response(200, json=MESSAGE)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(
            client,
            config=OutlookMailConfig(
                expected_principal_id="principal",
                after_date="2026/01/01",
                included_folders=[],
                excluded_folders=[],
            ),
        )
        with pytest.raises(OutlookBoundaryError, match="requires a received timestamp"):
            await source.message(MESSAGE["id"], files=files())
