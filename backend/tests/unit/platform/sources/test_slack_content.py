"""One-file capture interpretation and retained-inventory continuation boundaries."""

import hashlib
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import InvalidScanContinuation
from airweave.domains.entities.canonical.requests import (
    BlobReference,
    CaptureRecord,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.slack_files import SlackFileManifest
from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackApiError, SlackPrincipal, SlackSource
from airweave.platform.sources.slack_content import capture_slack_file


def connector():
    result = SlackSource(
        auth=StaticTokenProvider("fixture"), logger=MagicMock(), http_client=MagicMock()
    )
    result.slack_config = SlackConfig(
        expected_team_id="T1", expected_user_id="U1", capture_files=True
    )
    result._verified_principal = SlackPrincipal(ok=True, team_id="T1", user_id="U1")
    return result


def native(identity="F1", **changes):
    return {
        "id": identity,
        "size": 3,
        "mimetype": "text/plain",
        "url_private": f"https://files.slack.com/{identity}",
        **changes,
    }


def original(payload):
    return CaptureRecord(
        identity=RecordIdentity(
            record_type="file", native_id=payload["id"], container_id="parent:fixture"
        ),
        payload=payload,
        observed_at=datetime.now(timezone.utc),
    )


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
        return_value=BlobReference(key="original", sha256="a" * 64, size_bytes=3)
    )
    return files, saved


@pytest.mark.asyncio
async def test_file_enrichment_preserves_raw_child_and_root_blob():
    source = connector()
    files, saved = storage()
    source._get = AsyncMock(return_value={"file": native(name="note.txt")})
    item = original({"id": "F1"})
    result = await capture_slack_file(source, item, files)
    assert result.payload == {"id": "F1"} and result.completeness == "complete"
    assert [b.source_path for b in result.blobs] == ["", None]
    manifest = SlackFileManifest.model_validate_json(saved[-1])
    assert manifest.files[0].file["name"] == "note.txt"
    assert manifest.files[0].outcome == "captured"
    assert files.capture_canonical_url.call_args.kwargs["follow_redirects"] is False
    assert files.capture_canonical_url.call_args.kwargs["expected_media_type"] == "text/plain"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 401, 429, 503])
async def test_nonterminal_download_failure_cannot_create_manifest(status):
    source = connector()
    files, saved = storage()
    files.capture_canonical_url.side_effect = httpx.HTTPStatusError(
        "private",
        request=httpx.Request("GET", "https://files.slack.com/private"),
        response=httpx.Response(status),
    )
    with pytest.raises(SourceError):
        await capture_slack_file(source, original(native()), files)
    assert not saved


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
    with pytest.raises(ValueError, match="permitted origin"):
        await capture_slack_file(source, original(native(url_private=url)), files)
    files.capture_canonical_url.assert_not_called()


@pytest.mark.asyncio
async def test_explicit_missing_and_denied_metadata_vs_configuration_failure():
    source = connector()
    files, saved = storage()
    item = original({"id": "F1"})
    source._get = AsyncMock(side_effect=SlackApiError("not_visible"))
    result = await capture_slack_file(source, item, files)
    assert result.completeness == "partial"
    assert SlackFileManifest.model_validate_json(saved[-1]).files[0].reason == "access_denied"
    source._get = AsyncMock(
        return_value={"file": {"id": "F1", "thumb_360": "https://files.slack.com/thumb"}}
    )
    await capture_slack_file(source, item, files)
    assert SlackFileManifest.model_validate_json(saved[-1]).files[0].reason == "missing_metadata"
    files.capture_canonical_url.assert_not_called()
    source._get = AsyncMock(side_effect=SlackApiError("missing_scope"))
    with pytest.raises(SlackApiError):
        await capture_slack_file(source, item, files)


@pytest.mark.asyncio
async def test_file_size_and_enriched_identity_must_match():
    source = connector()
    files, saved = storage()
    with pytest.raises(ValueError, match="size changed"):
        await capture_slack_file(source, original(native(size=4)), files)
    source._get = AsyncMock(return_value={"file": native("F2")})
    with pytest.raises(ValueError, match="identity changed"):
        await capture_slack_file(source, original({"id": "F1"}), files)
    assert not saved


@pytest.mark.asyncio
async def test_child_continuation_uses_retained_inventory_and_exact_parent():
    source = connector()
    files, _ = storage()
    parent = SourceRecord.model_construct(
        id=uuid4(),
        identity=RecordIdentity(record_type="message", native_id="1", container_id="C1"),
        payload_schema_version=2,
        payload={"files": [native(), native("F2")]},
        content_access="available",
        deleted_at=None,
    )
    scope = source.child_scope(parent, "file")
    first = await source.capture_page(scope, ScanContinuation(), files=files, parent=parent)
    assert not first.final and first.records[0].parent == parent.identity
    second = await source.capture_page(scope, first.continuation, files=files, parent=parent)
    assert second.final and second.records[0].identity.native_id == "F2"
    changed = parent.model_copy(update={"payload": {"files": [native("F2")]}})
    with pytest.raises(InvalidScanContinuation):
        await source.capture_page(scope, first.continuation, files=files, parent=changed)
    other = parent.model_copy(
        update={"identity": parent.identity.model_copy(update={"container_id": "C2"})}
    )
    with pytest.raises(ValueError, match="current retained message"):
        await source.capture_page(scope, ScanContinuation(), files=files, parent=other)
    assert files.capture_canonical_url.await_count == 2


def test_disabled_topology_and_fingerprint_stay_legacy():
    import json

    source = connector()
    assert source.canonical_container_parents["file"] == "message"
    enabled = source.capture_cycle_configuration.fingerprint
    source.slack_config = source.slack_config.model_copy(update={"capture_files": False})
    expected = hashlib.sha256(
        json.dumps({"version": 2, "team_id": "T1", "user_id": "U1"}, sort_keys=True).encode()
    ).hexdigest()
    assert source.capture_cycle_configuration.fingerprint == expected != enabled
    assert source.canonical_record_types == ("channel", "message")
    assert SlackConfig().capture_files is False
    assert "file" in SlackSource.canonical_record_types


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata,reason",
    [
        (native(is_external=True), "unsupported_external"),
        (native(size=201 * 1024 * 1024), "oversized"),
    ],
)
async def test_terminal_file_omission_is_explicit_child_state(metadata, reason):
    source = connector()
    files, saved = storage()
    result = await capture_slack_file(source, original(metadata), files)
    assert result.completeness == "partial"
    assert len(result.blobs) == 1 and result.blobs[0].role == "representation_manifest"
    assert SlackFileManifest.model_validate_json(saved[-1]).files[0].reason == reason
    files.capture_canonical_url.assert_not_called()
