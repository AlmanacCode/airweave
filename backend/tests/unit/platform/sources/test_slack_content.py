"""Whole-page Slack originals: opt-in, immutable provenance and no failed-page progress."""

import hashlib
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from airweave.domains.entities.canonical.requests import BlobReference, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.slack_files import SlackFileManifest
from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackPrincipal, SlackSource
from airweave.platform.sources.slack_content import capture_slack_files


def connector():
    source = SlackSource(
        auth=StaticTokenProvider("token"), logger=MagicMock(), http_client=MagicMock()
    )
    source.slack_config = SlackConfig(
        expected_team_id="T1", expected_user_id="U1", capture_files=True
    )
    source._verified_principal = SlackPrincipal(ok=True, team_id="T1", user_id="U1")
    return source


def storage():
    files = MagicMock(MAX_FILE_SIZE_BYTES=200 * 1024 * 1024)
    saved = []

    async def store(content, *, media_type=None):
        saved.append(content)
        digest = hashlib.sha256(content).hexdigest()
        return BlobReference(
            key=digest, sha256=digest, size_bytes=len(content), media_type=media_type
        )

    files.store_canonical_blob = AsyncMock(side_effect=store)
    files.capture_canonical_url = AsyncMock(
        return_value=BlobReference(
            key="original", sha256="a" * 64, size_bytes=3, media_type="text/plain"
        )
    )
    return files, saved


@pytest.mark.asyncio
async def test_capture_enriches_separately_and_retains_explicit_partial_outcomes():
    source = connector()
    message = {
        "ts": "1",
        "files": [
            {"id": "F1"},
            {"id": "F2", "is_external": True},
            {
                "id": "F3",
                "size": 300 * 1024 * 1024,
                "mimetype": "text/plain",
                "url_private": "https://files.slack.com/large",
            },
        ],
    }
    source._get = AsyncMock(
        return_value={
            "ok": True,
            "file": {
                "id": "F1",
                "name": "note.txt",
                "mimetype": "text/plain",
                "size": 3,
                "url_private_download": "https://files.slack.com/original",
            },
        }
    )
    files, saved = storage()
    record = await capture_slack_files(source, source._capture_message(message, "C1"), files)
    assert record.payload == message and "name" not in record.payload["files"][0]
    manifest = SlackFileManifest.model_validate_json(saved[-1])
    assert [(x.outcome, x.reason) for x in manifest.files] == [
        ("captured", None),
        ("unavailable", "unsupported_external"),
        ("unavailable", "oversized"),
    ]
    assert manifest.files[0].file["name"] == "note.txt"
    assert record.completeness == "partial"
    assert [blob.source_path for blob in record.blobs] == ["/files/0", None]
    assert record.blobs[-1].role == "representation_manifest"
    assert files.capture_canonical_url.call_args.kwargs["follow_redirects"] is False
    assert files.capture_canonical_url.call_args.kwargs["expected_media_type"] == "text/plain"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 401, 429, 503])
async def test_late_download_failure_returns_no_page_or_new_continuation(status):
    source = connector()
    files, saved = storage()
    source._get = AsyncMock(
        return_value={
            "messages": [
                {
                    "ts": "1",
                    "files": [
                        {
                            "id": "F1",
                            "size": 3,
                            "mimetype": "text/plain",
                            "url_private": "https://files.slack.com/one",
                        },
                        {
                            "id": "F2",
                            "size": 3,
                            "mimetype": "text/plain",
                            "url_private": "https://files.slack.com/two",
                        },
                    ],
                }
            ],
            "response_metadata": {"next_cursor": "next"},
        }
    )
    error = httpx.HTTPStatusError(
        "private",
        request=httpx.Request("GET", "https://files.slack.com/private"),
        response=httpx.Response(status),
    )
    files.capture_canonical_url.side_effect = [files.capture_canonical_url.return_value, error]
    continuation = ScanContinuation()
    with pytest.raises(SourceError):
        await source.capture_page(
            CompletedScope(record_type="message", container_id="C1"), continuation, files=files
        )
    assert continuation.value == {} and not saved


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "http://files.slack.com/x",
        "https://files.slack.com.evil/x",
        "https://token@slack.com/x",
        "https://slack.com:444/x",
    ],
)
async def test_untrusted_original_never_downloads(url):
    source = connector()
    files, _ = storage()
    record = source._capture_message({"ts": "1", "files": [{"id": "F1", "url_private": url}]}, "C1")
    with pytest.raises(ValueError, match="permitted origin"):
        await capture_slack_files(source, record, files)
    files.capture_canonical_url.assert_not_called()


def test_capture_files_is_opt_in_and_changes_only_enabled_fingerprint():
    source = connector()
    enabled = source.capture_cycle_configuration.fingerprint
    source.slack_config.capture_files = False
    legacy = source.capture_cycle_configuration.fingerprint
    import json

    expected = hashlib.sha256(
        json.dumps({"version": 2, "team_id": "T1", "user_id": "U1"}, sort_keys=True).encode()
    ).hexdigest()
    assert legacy == expected and enabled != legacy
    assert SlackConfig().capture_files is False


@pytest.mark.asyncio
async def test_size_race_and_metadata_identity_mismatch_do_not_publish_manifest():
    source = connector()
    files, saved = storage()
    record = source._capture_message(
        {
            "ts": "1",
            "files": [
                {
                    "id": "F1",
                    "size": 4,
                    "mimetype": "text/plain",
                    "url_private": "https://files.slack.com/original",
                }
            ],
        },
        "C1",
    )
    with pytest.raises(ValueError, match="size changed"):
        await capture_slack_files(source, record, files)
    assert not saved
    source._get = AsyncMock(return_value={"file": {"id": "F2"}})
    record = source._capture_message({"ts": "1", "files": [{"id": "F1"}]}, "C1")
    with pytest.raises(ValueError, match="identity changed"):
        await capture_slack_files(source, record, files)
    assert not saved


@pytest.mark.asyncio
async def test_explicit_file_denial_is_partial_but_missing_scope_fails_page():
    from airweave.platform.sources.slack import SlackApiError

    source = connector()
    files, saved = storage()
    record = source._capture_message({"ts": "1", "files": [{"id": "F1"}]}, "C1")
    source._get = AsyncMock(side_effect=SlackApiError("not_visible"))
    result = await capture_slack_files(source, record, files)
    assert result.completeness == "partial"
    assert SlackFileManifest.model_validate_json(saved[-1]).files[0].reason == "access_denied"
    source._get = AsyncMock(side_effect=SlackApiError("missing_scope"))
    files.store_canonical_blob.reset_mock()
    with pytest.raises(SlackApiError):
        await capture_slack_files(source, record, files)
    files.store_canonical_blob.assert_not_called()


@pytest.mark.asyncio
async def test_missing_metadata_remains_partial_and_never_uses_thumbnail():
    source = connector()
    files, saved = storage()
    record = source._capture_message(
        {"ts": "1", "files": [{"id": "F1", "url_private": "https://files.slack.com/original"}]},
        "C1",
    )
    source._get = AsyncMock(
        return_value={
            "file": {"id": "F1", "thumb_360": "https://files.slack.com/thumbnail", "size": 3}
        }
    )
    result = await capture_slack_files(source, record, files)
    assert result.completeness == "partial"
    assert SlackFileManifest.model_validate_json(saved[-1]).files[0].reason == "missing_metadata"
    files.capture_canonical_url.assert_not_called()
