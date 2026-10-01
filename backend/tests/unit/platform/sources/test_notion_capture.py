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
        config=NotionConfig(expected_workspace_id=UUID(int=100), expected_bot_id=UUID(int=101)),
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


@pytest.fixture
def property_files(tmp_path, monkeypatch):
    from uuid import uuid4

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.storage.file_service import FileService

    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir",
        lambda _: str(tmp_path / "temp"),
    )
    return FileService(uuid4(), FilesystemBackend(tmp_path / "blobs"), sync_id=uuid4())


def property_list(identity, kind, values, *, cursor=None, calculation=None):
    metadata = {"id": identity, "type": kind, "next_url": None}
    if calculation is not None:
        metadata[kind] = calculation
    return {**listing(values, cursor), "type": "property_item", "property_item": metadata}


@pytest.mark.asyncio
async def test_property_archive_preserves_all_native_pages_and_final_rollup(property_files):
    import json

    prop = {"id": "roll%2Fup", "type": "rollup", "rollup": {"type": "incomplete"}}
    page = native(properties={"Sum": prop})
    first = property_list(
        prop["id"],
        "rollup",
        [],
        cursor="next",
        calculation={"type": "incomplete", "function": "sum", "incomplete": {}},
    )
    last = property_list(
        prop["id"], "rollup", [], calculation={"type": "number", "function": "sum", "number": 13}
    )
    connector, client = await source(gets=[page, first, last, page])
    owner = parent()
    result = await connector.capture_page(
        connector.child_scope(owner, "page_property"),
        ScanContinuation(),
        files=property_files,
        parent=owner,
    )
    record = result.records[0]
    assert result.final and record.identity.native_id == prop["id"]
    assert record.identity.container_id == PAGE and record.parent == owner.identity
    assert record.payload["value_status"] == "available" and record.completeness == "partial"
    assert record.source_updated_at is None
    archive = json.loads(await property_files.storage.read_file(record.blobs[0].key))
    assert archive == {
        "format_version": 1,
        "page_id": PAGE,
        "property_id": prop["id"],
        "responses": [first, last],
        "notion_version": "2026-03-11",
        "page_last_edited_time": STAMP,
    }
    assert client.get.call_args_list[1].args[0].endswith("properties/roll%2Fup")
    assert client.get.call_args_list[2].kwargs["params"]["start_cursor"] == "next"
    assert record.payload["response_count"] == 2


@pytest.mark.asyncio
async def test_unsupported_formula_is_explicit_retained_native_result(property_files):
    prop = {"id": "calc", "type": "formula"}
    page = native(properties={"Formula": prop})
    value = {
        **prop,
        "object": "property_item",
        "formula": {"type": "unsupported", "unsupported": {}},
    }
    connector, _ = await source(gets=[page, value, page])
    owner = parent()
    result = await connector.capture_page(
        connector.child_scope(owner, "page_property"),
        ScanContinuation(),
        files=property_files,
        parent=owner,
    )
    assert result.records[0].payload["value_status"] == "unsupported"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["wrong_id", "unfinished_rollup", "shape_changed", "provider_error", "edited_page", "overflow"],
)
async def test_property_failure_never_publishes_partial_archive(case, property_files, monkeypatch):
    from airweave.domains.entities.canonical.page_source import InvalidScanContinuation

    prop = {"id": "roll", "type": "rollup"}
    page = native(properties={"Rollup": prop})
    value = property_list(
        "wrong" if case == "wrong_id" else "roll",
        "rollup",
        [],
        calculation={
            "type": "incomplete" if case == "unfinished_rollup" else "number",
            "number": 3,
        },
    )
    gets = [page, value, page]
    expected = (ValueError, SourceError)
    if case == "shape_changed":
        gets = [
            page,
            property_list("roll", "rollup", [], cursor="next", calculation={"type": "incomplete"}),
            {
                "object": "property_item",
                "id": "roll",
                "type": "rollup",
                "rollup": {"type": "number", "number": 3},
            },
        ]
        expected = InvalidScanContinuation
    elif case == "provider_error":
        gets = [page, response({"code": "restricted_resource"}, 403)]
        expected = SourceEntityForbiddenError
    elif case == "edited_page":
        gets[-1] = {**page, "last_edited_time": "2026-09-30T01:00:00Z"}
        expected = InvalidScanContinuation
    elif case == "overflow":
        monkeypatch.setattr("airweave.platform.sources.notion.MAX_FILE_SIZE_BYTES", 32)
        expected = SourceError
    connector, _ = await source(gets=gets)
    owner = parent()
    with pytest.raises(expected):
        await connector.capture_page(
            connector.child_scope(owner, "page_property"),
            ScanContinuation(),
            files=property_files,
            parent=owner,
        )
    assert await property_files.storage.list_files() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["raw/slash", "bad%escape", "id?query=oops", "id#fragment"])
async def test_unencoded_property_ids_never_enter_native_path(identity, property_files):
    page = native(properties={"Field": {"id": identity, "type": "number"}})
    connector, client = await source(gets=[page])
    owner = parent()
    with pytest.raises(ValidationError):
        await connector.capture_page(
            connector.child_scope(owner, "page_property"),
            ScanContinuation(),
            files=property_files,
            parent=owner,
        )
    assert client.get.call_count == 1


@pytest.mark.asyncio
async def test_property_archive_roundtrips_unicode_and_empty_value(property_files):
    import json

    prop = {"id": "p%25", "type": "rich_text", "rich_text": []}
    page = native(properties={'Notes "日本語"': prop})
    value = {**property_list(prop["id"], "rich_text", []), "unknown": '日本語\n"escaped"'}
    connector, _ = await source(gets=[page, value, page])
    owner = parent()
    result = await connector.capture_page(
        connector.child_scope(owner, "page_property"),
        ScanContinuation(),
        files=property_files,
        parent=owner,
    )
    record = result.records[0]
    archive = json.loads(await property_files.storage.read_file(record.blobs[0].key))
    assert archive["responses"] == [value]
    assert record.payload["name"] == 'Notes "日本語"'
    assert archive["property_id"] == "p%25"


def principal(**overrides):
    return {
        "object": "user",
        "type": "bot",
        "id": str(UUID(int=101)),
        "bot": {"workspace_id": str(UUID(int=100))},
        **overrides,
    }


@pytest.mark.asyncio
async def test_validate_exact_workspace_bot_pair_and_fingerprint():
    connector, client = await source(gets=[principal(name="Editable label")])
    await connector.validate()
    assert client.get.call_args.args[0] == "https://api.notion.com/v1/users/me"
    for field in ("expected_workspace_id", "expected_bot_id"):
        changed = await NotionSource.create(
            auth=connector.auth,
            logger=MagicMock(),
            http_client=AsyncMock(),
            config=connector.config.model_copy(update={field: UUID(int=102)}),
        )
        assert (
            changed.capture_cycle_configuration.fingerprint
            != connector.capture_cycle_configuration.fingerprint
        )
    assert set(connector.capture_cycle_configuration.completion_policies.values()) == {
        "discovery_with_validation"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [principal(id=str(UUID(int=102))), principal(bot={"workspace_id": str(UUID(int=102))})],
)
async def test_validate_rejects_other_workspace_or_bot(payload):
    from airweave.domains.sources.exceptions import SourceAuthError

    connector, client = await source(gets=[payload])
    with pytest.raises(SourceAuthError, match="does not match") as caught:
        await connector.validate()
    assert caught.value.status_code == 200
    assert client.get.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        principal(object="page"),
        principal(type="person"),
        principal(id="not-a-uuid"),
        principal(bot={}),
        principal(bot={"workspace_id": None}),
        principal(bot={"workspace_id": "name"}),
    ],
)
async def test_validate_rejects_missing_or_malformed_bot_identity(payload):
    connector, _ = await source(gets=[payload])
    with pytest.raises(ValidationError):
        await connector.validate()


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"expected_workspace_id": str(UUID(int=100))},
        {"expected_workspace_id": "workspace name", "expected_bot_id": str(UUID(int=101))},
        {"expected_workspace_id": str(UUID(int=100)), "expected_bot_id": "bot name"},
        {
            "expected_workspace_id": str(UUID(int=100)),
            "expected_bot_id": str(UUID(int=101)),
            "extra": True,
        },
    ],
)
def test_notion_config_requires_only_verified_uuid_pair(config):
    with pytest.raises(ValidationError):
        NotionConfig.model_validate(config)
