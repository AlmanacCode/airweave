"""Native Notion acquisition fixtures; no provider calls or OAuth grants."""

import hashlib
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import ScopeAccessLost
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceEntityForbiddenError, SourceError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.platform.configs.config import NotionConfig
from airweave.platform.sources.notion import API_VERSION, NotionSource

PAGE, DATA, DATABASE, BLOCK, CHILD = (str(UUID(int=i)) for i in range(1, 6))
STAMP = "2026-09-30T00:00:00Z"


def native(kind="page", identity=PAGE, **fields):
    return {
        "object": kind,
        "id": identity,
        "created_time": STAMP,
        "last_edited_time": STAMP,
        "in_trash": False,
        "parent": {"type": "workspace", "workspace": True},
        "unknown": {"retained": True},
        **fields,
    }


def listing(rows=(), cursor=None, status=None):
    result = {
        "object": "list",
        "results": list(rows),
        "has_more": cursor is not None,
        "next_cursor": cursor,
    }
    if status:
        result["request_status"] = {"type": "incomplete", "incomplete_reason": status}
    return result


def response(payload, status=200):
    return httpx.Response(
        status, json=payload, request=httpx.Request("GET", "https://api.notion.com/v1/test")
    )


async def source(*, gets=(), posts=()):
    client = AsyncMock()
    client.get.side_effect = [response(item) if isinstance(item, dict) else item for item in gets]
    client.post.side_effect = [response(item) if isinstance(item, dict) else item for item in posts]
    result = await NotionSource.create(
        auth=ManagedAuthProvider(
            api_key="fixture",
            connected_account_id="fixture",
            allowed_hosts=frozenset({"api.notion.com"}),
        ),
        logger=MagicMock(),
        http_client=client,
        config=NotionConfig(),
    )
    return result, client


def parent(kind="page", identity=PAGE, **fields):
    return SourceRecord.model_construct(
        identity=RecordIdentity(record_type=kind, native_id=identity),
        payload=native(kind, identity, **fields),
        parent=None,
    )


@pytest.mark.asyncio
async def test_search_uses_exact_current_root_not_search_snapshot_and_keeps_unknown_fields():
    original = native(title=[{"plain_text": "Current"}])
    connector, client = await source(gets=[original], posts=[listing([native(title=[])])])
    page = await connector.capture_page(
        CompletedScope(record_type="page"), ScanContinuation(), files=MagicMock()
    )
    assert page.final and page.records[0].payload == original
    assert page.records[0].parent is None and page.records[0].completeness == "partial"
    assert page.records[0].source_created_at.isoformat() == "2026-09-30T00:00:00+00:00"
    assert client.get.call_args.kwargs["headers"] == {"Notion-Version": API_VERSION}
    assert client.post.call_args.kwargs["json"]["filter"]["value"] == "page"
    assert connector.capture_cycle_configuration.policy("page") == "discovery_with_validation"


@pytest.mark.asyncio
async def test_search_rejects_wrong_exact_id_and_capped_discovery():
    connector, _ = await source(gets=[native(identity=CHILD)], posts=[listing([native()])])
    with pytest.raises(ValueError, match="wrong exact"):
        await connector.capture_page(
            CompletedScope(record_type="page"), ScanContinuation(), files=MagicMock()
        )
    connector, _ = await source(posts=[listing(status="query_result_limit_reached")])
    with pytest.raises(SourceError, match="capped"):
        await connector.capture_page(
            CompletedScope(record_type="page"), ScanContinuation(), files=MagicMock()
        )


@pytest.mark.asyncio
async def test_omission_refresh_distinguishes_native_unavailability_from_capability_failure():
    connector, _ = await source(gets=[response({"code": "object_not_found"}, 404)])
    result = await connector.refresh_known(parent(), files=MagicMock())
    assert result.kind == "delete" and result.removal_reason == "scope_removed"
    connector, _ = await source(gets=[response({"code": "restricted_resource"}, 403)])
    with pytest.raises(SourceEntityForbiddenError):
        await connector.refresh_known(parent(), files=MagicMock())
    connector, _ = await source(gets=[native(in_trash=True)])
    result = await connector.refresh_known(parent(), files=MagicMock())
    assert result.removal_reason == "provider_deleted" and result.payload["in_trash"] is True


@pytest.mark.asyncio
async def test_query_rows_are_independently_retrieved_roots_and_cap_windows_resume():
    row = native(created_time="2026-09-30T01:00:00Z")
    connector, client = await source(
        gets=[row, row], posts=[listing([row], status="query_result_limit_reached"), listing([row])]
    )
    owner = parent("data_source", DATA)
    scope = connector.child_scope(owner, "page")
    first = await connector.capture_page(scope, ScanContinuation(), files=MagicMock(), parent=owner)
    assert not first.final and first.records == ()
    assert first.discovered_records[0].parent is None
    second = await connector.capture_page(
        scope, first.continuation, files=MagicMock(), parent=owner
    )
    assert second.final
    assert client.post.call_args.kwargs["json"]["filter"]["created_time"]["on_or_after"].startswith(
        "2026-09-30T01:"
    )


@pytest.mark.asyncio
async def test_query_limit_with_same_timestamp_fails_without_claiming_completion():
    connector, _ = await source(
        gets=[native()], posts=[listing([native()], status="query_result_limit_reached")]
    )
    owner = parent("data_source", DATA)
    with pytest.raises(SourceError, match="cannot be split"):
        await connector.capture_page(
            connector.child_scope(owner, "page"),
            ScanContinuation(value={"window_start": STAMP}),
            files=MagicMock(),
            parent=owner,
        )


@pytest.mark.asyncio
async def test_database_query_scope_does_not_parent_discovered_data_source():
    database = native("database", DATABASE, data_sources=[{"id": DATA, "name": "Rows"}])
    data = native("data_source", DATA, parent={"type": "database_id", "database_id": DATABASE})
    connector, _ = await source(gets=[database, data])
    owner = parent("database", DATABASE)
    result = await connector.capture_page(
        connector.child_scope(owner, "data_source"),
        ScanContinuation(),
        files=MagicMock(),
        parent=owner,
    )
    assert result.final and not result.records
    assert result.discovered_records[0].identity.record_type == "data_source"
    assert result.discovered_records[0].parent is None


@pytest.mark.asyncio
async def test_direct_shared_data_source_does_not_depend_on_reading_database_parent():
    connector, _ = await source(gets=[response({"code": "object_not_found"}, 404)])
    owner = parent("data_source", DATA, parent={"type": "database_id", "database_id": DATABASE})
    result = await connector.capture_page(
        connector.child_scope(owner, "database"),
        ScanContinuation(),
        files=MagicMock(),
        parent=owner,
    )
    assert result.final and result.discovered_records == ()


@pytest.mark.asyncio
async def test_child_page_reference_retains_block_and_independent_page_without_double_crawl():
    block = native(
        "block",
        CHILD,
        type="child_page",
        has_children=True,
        parent={"type": "page_id", "page_id": PAGE},
    )
    connector, _ = await source(gets=[listing([block]), native(identity=CHILD)])
    owner = parent()
    result = await connector.capture_page(
        connector.child_scope(owner, "block"), ScanContinuation(), files=MagicMock(), parent=owner
    )
    assert result.records[0].parent == owner.identity
    assert result.records[0].allow_reparent
    assert result.discovered_records[0].parent is None
    reference = parent(
        "block",
        CHILD,
        **{key: value for key, value in block.items() if key not in {"object", "id"}},
    )
    empty = await connector.capture_page(
        connector.child_scope(reference, "block"),
        ScanContinuation(),
        files=MagicMock(),
        parent=reference,
    )
    assert empty.final and not empty.records


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["wrong_parent", "oversized", "missing_cursor", "repeated_cursor"])
async def test_invalid_block_pages_fail_before_capture(case):
    block = native(
        "block",
        BLOCK,
        type="paragraph",
        has_children=False,
        parent={"type": "page_id", "page_id": CHILD if case == "wrong_parent" else PAGE},
    )
    data = listing([block] * (26 if case == "oversized" else 1))
    progress = ScanContinuation()
    if case in {"missing_cursor", "repeated_cursor"}:
        data["has_more"] = True
    if case == "repeated_cursor":
        data["next_cursor"] = "same"
        progress = ScanContinuation(value={"cursor": "same"})
    connector, _ = await source(gets=[data])
    owner = parent()
    with pytest.raises((ValueError, ValidationError)):
        await connector.capture_page(
            connector.child_scope(owner, "block"), progress, files=MagicMock(), parent=owner
        )


@pytest.mark.asyncio
async def test_exact_owner_unavailability_withdraws_scope_not_generic_errors():
    connector, _ = await source(gets=[response({"code": "object_not_found"}, 404)])
    owner = parent()
    with pytest.raises(ScopeAccessLost) as caught:
        await connector.capture_page(
            connector.child_scope(owner, "block"),
            ScanContinuation(),
            files=MagicMock(),
            parent=owner,
        )
    assert caught.value.removal_reason == "scope_removed"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["deleted", "moved", "trashed", "same_owner"])
async def test_omitted_block_requires_exact_loss_or_move_evidence(state):
    owner = RecordIdentity(record_type="page", native_id=PAGE)
    block = native(
        "block",
        BLOCK,
        type="paragraph",
        has_children=False,
        in_trash=state == "trashed",
        parent={"type": "page_id", "page_id": CHILD if state == "moved" else PAGE},
    )
    reply = response({"code": "object_not_found"}, 404) if state == "deleted" else block
    connector, _ = await source(gets=[reply])
    known = parent("block", BLOCK).model_copy(update={"parent": owner})
    if state == "same_owner":
        with pytest.raises(ValueError, match="still readable"):
            await connector.confirm_absent(known)
    else:
        await connector.confirm_absent(known)


@pytest.mark.asyncio
async def test_database_inventory_mutation_uses_existing_typed_restart():
    from airweave.domains.entities.canonical.page_source import InvalidScanContinuation

    database = native("database", DATABASE, data_sources=[{"id": DATA}])
    connector, _ = await source(gets=[database])
    owner = parent("database", DATABASE)
    with pytest.raises(InvalidScanContinuation):
        await connector.capture_page(
            connector.child_scope(owner, "data_source"),
            ScanContinuation(value={"cursor": "previous-list-digest", "offset": 25}),
            files=MagicMock(),
            parent=owner,
        )


@pytest.mark.asyncio
async def test_trashed_database_stops_enumeration_without_withdrawing_independent_children():
    connector, client = await source(
        gets=[native("database", DATABASE, in_trash=True, data_sources=[{"id": DATA}])]
    )
    owner = parent("database", DATABASE)
    with pytest.raises(ScopeAccessLost) as caught:
        await connector.capture_page(
            connector.child_scope(owner, "data_source"),
            ScanContinuation(),
            files=MagicMock(),
            parent=owner,
        )
    assert caught.value.removal_reason == "scope_removed"
    assert client.get.call_count == 1


@pytest.mark.asyncio
async def test_unknown_query_incompleteness_never_uses_limit_recovery():
    connector, _ = await source(posts=[listing(status="unrecognized_future_reason")])
    owner = parent("data_source", DATA)
    with pytest.raises(SourceError, match="unsupported reason"):
        await connector.capture_page(
            connector.child_scope(owner, "page"),
            ScanContinuation(),
            files=MagicMock(),
            parent=owner,
        )


@pytest.mark.asyncio
async def test_nonadjacent_cursor_cycle_is_rejected_from_durable_continuation():
    connector, _ = await source(
        posts=[listing(cursor="A"), listing(cursor="B"), listing(cursor="A")]
    )
    scope = CompletedScope(record_type="page")
    first = await connector.capture_page(scope, ScanContinuation(), files=MagicMock())
    second = await connector.capture_page(scope, first.continuation, files=MagicMock())
    with pytest.raises(ValueError, match="cursor cycle"):
        await connector.capture_page(scope, second.continuation, files=MagicMock())


@pytest.mark.asyncio
async def test_large_search_advances_with_bounded_recent_cursor_history():
    connector, _ = await source(posts=[listing(cursor=f"page-{i}") for i in range(520)])
    scope = CompletedScope(record_type="page")
    continuation = ScanContinuation()
    for _ in range(520):
        page = await connector.capture_page(scope, continuation, files=MagicMock())
        assert not page.final
        # Exercise the durable JSON boundary on every page, not only in-memory state.
        continuation = ScanContinuation.model_validate_json(page.continuation.model_dump_json())
        assert len(continuation.value["cursor_hashes"]) <= 512
    hashes = continuation.value["cursor_hashes"]
    assert continuation.value["cursor"] == "page-519"
    assert hashes[0] == hashlib.sha256(b"page-8").hexdigest()
    assert hashes[-1] == hashlib.sha256(b"page-519").hexdigest()


@pytest.mark.asyncio
async def test_query_window_resets_recent_cursor_history():
    row = native(created_time="2026-09-30T01:00:00Z")
    connector, _ = await source(
        gets=[row],
        posts=[
            listing(cursor="reusable"),
            listing([row], status="query_result_limit_reached"),
            listing(cursor="reusable"),
        ],
    )
    owner = parent("data_source", DATA)
    scope = connector.child_scope(owner, "page")
    first = await connector.capture_page(scope, ScanContinuation(), files=MagicMock(), parent=owner)
    window = await connector.capture_page(
        scope, first.continuation, files=MagicMock(), parent=owner
    )
    assert not window.final and window.continuation.value["cursor_hashes"] == []
    assert window.continuation.value["cursor"] is None
    following = await connector.capture_page(
        scope, window.continuation, files=MagicMock(), parent=owner
    )
    assert following.continuation.value["cursor"] == "reusable"
