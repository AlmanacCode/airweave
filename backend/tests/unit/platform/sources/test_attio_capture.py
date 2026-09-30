"""Native Attio capture boundaries; credential-free HTTP fixtures."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceAuthError, SourceError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.platform.configs.config import AttioConfig
from airweave.platform.sources.attio import AttioSource

WORKSPACE, OBJECT, RECORD, LIST = [str(UUID(int=i)) for i in range(1, 5)]


def response(data, status=200):
    return httpx.Response(
        status, json=data, request=httpx.Request("GET", "https://api.attio.com/v2/notes")
    )


def native(kind, value, **fields):
    return {"id": {"workspace_id": WORKSPACE, kind + "_id": value}, **fields}


def parent(kind, value, container=None):
    return SourceRecord(
        id=UUID(int=90),
        sync_id=UUID(int=91),
        identity=RecordIdentity(record_type=kind, native_id=value, container_id=container),
        revision=1,
        payload={},
        payload_schema_version=1,
        capture_hash="0" * 64,
        content_hash=None,
        completeness="complete",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


async def source(*pages, principal=None):
    client = AsyncMock()
    client.get.side_effect = [
        response(
            principal
            if principal is not None
            else {
                "active": True,
                "workspace_id": WORKSPACE,
                "token_level": "user",
                "authorized_by_workspace_member_id": str(UUID(int=7)),
            }
        ),
        *pages,
    ]
    result = await AttioSource.create(
        auth=ManagedAuthProvider(
            api_key="test-only",
            connected_account_id="ca_test",
            allowed_hosts=frozenset({"api.attio.com"}),
        ),
        logger=MagicMock(),
        http_client=client,
        config=AttioConfig(workspace_id=WORKSPACE),
    )
    return result, client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "principal",
    [{"active": True}, {"active": True, "workspace_id": str(UUID(int=999))}],
)
async def test_identity_requires_active_matching_workspace(principal):
    with pytest.raises(ValueError):
        await source(principal=principal)


@pytest.mark.asyncio
async def test_native_entries_are_distinct_memberships():
    capture, client = await source()
    for number in (10, 11):
        list_id = str(UUID(int=number))
        owner = parent("list", list_id)
        entry = native(
            "entry",
            str(UUID(int=number + 100)),
            parent_record_id=RECORD,
            parent_object="people",
            entry_values={"stage": [{"status": "new"}]},
        )
        entry["id"]["list_id"] = list_id
        client.post.return_value = response({"data": [entry]})
        page = await capture.capture_page(
            capture.child_scope(owner, "entry"), ScanContinuation(), parent=owner, files=MagicMock()
        )
        assert page.records[0].payload == entry
        assert page.records[0].identity.native_id != RECORD
        assert page.records[0].identity.container_id == list_id
        assert page.records[0].parent == owner.identity
        assert client.post.call_args.kwargs["headers"] == {}
    assert all(
        capture.capture_cycle_configuration.policy(k) == "discovery_only"
        for k in capture.canonical_record_types
    )


@pytest.mark.asyncio
async def test_more_than_fifty_notes_resume_and_preserve_native_content():
    obj = response({"data": native("object", OBJECT, api_slug="people")})
    notes = [
        native(
            "note",
            str(UUID(int=i + 100)),
            parent_object="people",
            parent_record_id=RECORD,
            content_plaintext="body",
            content_markdown="**body**",
            future={"kept": True},
        )
        for i in range(51)
    ]
    capture, client = await source(
        obj, response({"data": notes[:50]}), obj, response({"data": notes[50:]})
    )
    owner = parent("record", RECORD, OBJECT)
    scope = capture.child_scope(owner, "note")
    page = await capture.capture_page(scope, ScanContinuation(), files=MagicMock(), parent=owner)
    assert not page.final
    assert len(page.records) == 50
    # A reconstructed source uses only persisted continuation, not in-memory iteration state.
    resumed, resumed_client = await source(obj, response({"data": notes[50:]}))
    last = await resumed.capture_page(scope, page.continuation, files=MagicMock(), parent=owner)
    assert last.final and last.records[0].payload == notes[-1]
    assert resumed_client.get.call_args.kwargs["params"]["offset"] == 50


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code,status", [("merge_in_progress", 404), ("not_found", 404), ("forbidden", 403)]
)
async def test_note_failure_does_not_finalize_or_hide_error(code, status):
    capture, client = await source(
        response({"data": native("object", OBJECT, api_slug="people")}),
        response({"code": code}, status),
    )
    owner = parent("record", RECORD, OBJECT)
    with pytest.raises(SourceError):
        await capture.capture_page(
            capture.child_scope(owner, "note"), ScanContinuation(), files=MagicMock(), parent=owner
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["workspace", "object", "duplicate"])
async def test_invalid_record_page_rejected_atomically(fault):
    capture, client = await source()
    raw = native("record", RECORD)
    raw["id"]["object_id"] = OBJECT
    if fault in {"workspace", "object"}:
        raw["id"][fault + "_id"] = str(UUID(int=999))
    client.post.return_value = response({"data": [raw, raw] if fault == "duplicate" else [raw]})
    owner = parent("object", OBJECT)
    with pytest.raises(ValueError):
        await capture.capture_page(
            capture.child_scope(owner, "record"),
            ScanContinuation(),
            files=MagicMock(),
            parent=owner,
        )


@pytest.mark.asyncio
async def test_previous_page_overlap_fails_and_absence_never_inferred():
    capture, client = await source()
    owner = parent("object", OBJECT)
    raw = native("record", RECORD)
    raw["id"]["object_id"] = OBJECT
    client.post.return_value = response({"data": [raw]})
    with pytest.raises(ValueError, match="repeated identities"):
        await capture.capture_page(
            capture.child_scope(owner, "record"),
            ScanContinuation(value={"offset": 500, "previous_ids": [RECORD]}),
            files=MagicMock(),
            parent=owner,
        )
    with pytest.raises(ValueError, match="cannot confirm absence"):
        await capture.confirm_absent(owner)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500])
async def test_transient_note_failure_propagates_after_retry_budget(status):
    from types import MethodType

    from tenacity import stop_after_attempt

    capture, client = await source(
        response({"data": native("object", OBJECT, api_slug="people")}),
        response({"code": "transient"}, status),
    )
    # Exercise production translation with one attempt, without waiting on fixture failures.
    capture._request = MethodType(
        AttioSource._request.retry_with(stop=stop_after_attempt(1)), capture
    )
    owner = parent("record", RECORD, OBJECT)
    with pytest.raises(SourceError):
        await capture.capture_page(
            capture.child_scope(owner, "note"), ScanContinuation(), files=MagicMock(), parent=owner
        )


@pytest.mark.asyncio
async def test_inactive_http200_is_reconnect_error():
    with pytest.raises(SourceAuthError) as error:
        await source(principal={"active": False})
    assert error.value.status_code == 200
    assert "reconnect" in str(error.value)
