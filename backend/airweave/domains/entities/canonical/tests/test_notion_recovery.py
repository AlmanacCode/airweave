"""Real Notion pipeline and PostgreSQL recovery with synthetic native HTTP only."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run, saved
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.entity import Entity
from airweave.platform.configs.config import NotionConfig
from airweave.platform.sources.notion import NotionSource

PAGE, DATA, DATABASE, BLOCK1, BLOCK2 = (str(UUID(int=i)) for i in range(1, 6))
STAMP = "2026-09-30T00:00:00Z"


def native(kind, identity, **fields):
    return {
        "object": kind,
        "id": identity,
        "created_time": STAMP,
        "last_edited_time": STAMP,
        "in_trash": False,
        "parent": {"type": "workspace", "workspace": True},
        **fields,
    }


def listing(rows=(), cursor=None):
    return {
        "object": "list",
        "results": list(rows),
        "next_cursor": cursor,
        "has_more": cursor is not None,
    }


class NativeFixture:
    """Endpoint router only; production source, pipeline, driver and SQL stay real."""

    def __init__(self):
        self.interrupt = True
        self.calls = []
        self.page = native("page", PAGE, properties={})
        self.data = native(
            "data_source", DATA, parent={"type": "database_id", "database_id": DATABASE}
        )
        self.database = native("database", DATABASE, data_sources=[{"id": DATA}])
        self.blocks = [
            native(
                "block",
                item,
                type="paragraph",
                has_children=False,
                parent={"type": "page_id", "page_id": PAGE},
                paragraph={"rich_text": [{"plain_text": "retained"}]},
            )
            for item in (BLOCK1, BLOCK2)
        ]

    async def request(self, url, *, headers, json=None, params=None):
        assert headers["Notion-Version"] == "2026-03-11"
        path = url.removeprefix("https://api.notion.com/v1/")
        self.calls.append((path, json, params))
        if path == "search":
            payload = listing([self.data] if json["filter"]["value"] == "data_source" else [])
        elif path == f"data_sources/{DATA}":
            payload = self.data
        elif path == f"databases/{DATABASE}":
            payload = self.database
        elif path == f"data_sources/{DATA}/query":
            payload = listing([self.page])
        elif path == f"pages/{PAGE}":
            payload = self.page
        elif path == f"blocks/{PAGE}/children":
            if params.get("start_cursor"):
                if self.interrupt:
                    raise ConnectionError("synthetic interruption after committed block page")
                payload = listing([self.blocks[1]])
            else:
                payload = listing([self.blocks[0]], "block-page-2")
        else:
            raise AssertionError(f"Unexpected synthetic request: {path}")
        return httpx.Response(200, json=payload, request=httpx.Request("GET", url))


async def runner(database, source, fixture, attempt=1, files=None):
    service, fence = source
    client = AsyncMock()
    client.get.side_effect = fixture.request
    client.post.side_effect = fixture.request
    connector = await NotionSource.create(
        auth=StaticTokenProvider("fixture"),
        logger=MagicMock(),
        http_client=client,
        config=NotionConfig(),
    )
    ctx, _, runtime, bus = components(database, source)
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
        connector.canonical_container_parents,
        page_source=connector,
        files=files or MagicMock(),
    )
    runtime.source, runtime.canonical_capture = connector, pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    return instance, connector


async def test_discovered_roots_reach_frontier_and_interrupted_membership_refreshes(
    database, source
):
    fixture = NativeFixture()
    first, _ = await runner(database, source, fixture)
    with pytest.raises(ConnectionError, match="synthetic interruption"):
        await run(first)
    rows, checkpoint, scans = await saved(database)
    assert "canonical_checkpoint" not in checkpoint
    assert all(
        row.parent_record_type is None
        for row in rows
        if row.entity_definition_short_name != "block"
    )
    scan = next(
        row for row in scans if row.record_type == "block" and row.continuation.get("cursor")
    )
    assert scan.continuation["cursor"] == "block-page-2" and scan.phase == "collecting"
    before = len(fixture.calls)
    fixture.interrupt = False
    second, _ = await runner(database, source, fixture, attempt=2)
    await run(second)
    later = fixture.calls[before:]
    block_calls = [item for item in later if item[0] == f"blocks/{PAGE}/children"]
    # Block inventories are themselves membership authorities. A new attempt
    # refreshes the whole exact inventory; durable originals survive the failure.
    assert len(block_calls) == 2
    assert "start_cursor" not in block_calls[0][2]
    assert block_calls[1][2]["start_cursor"] == "block-page-2"
    rows, checkpoint, _ = await saved(database)
    assert {row.native_id for row in rows} == {PAGE, DATA, DATABASE, BLOCK1, BLOCK2}
    assert all(row.deleted_at is None for row in rows)
    assert all(row.completeness == "partial" for row in rows)
    assert checkpoint["canonical_cycle"]["phase"] == "complete"
    assert (
        checkpoint["canonical_cycle"]["configuration"]["completion_policies"]["page"]
        == "discovery_with_validation"
    )
    # No query-scope ancestry was assigned to independently visible pages/data sources.
    assert all(
        row.parent_record_type is None
        for row in rows
        if row.entity_definition_short_name in {"page", "data_source", "database"}
    )


async def test_root_omission_exact_refresh_preserves_visible_page_and_current_native_payload(
    database, source
):
    fixture = NativeFixture()
    fixture.interrupt = False
    instance, connector = await runner(database, source, fixture)
    older = {**fixture.page, "properties": {"title": "old"}}
    service, fence = source
    async with database() as db:
        await service.capture(
            db, CaptureBatch(fence=fence, records=(connector._record("page", older),))
        )
    await run(instance)
    async with database() as db:
        page = await db.scalar(select(Entity).where(Entity.native_id == PAGE))
        assert page.source_payload == fixture.page and page.deleted_at is None
        assert page.parent_record_type is None
    assert any(call[0] == f"pages/{PAGE}" for call in fixture.calls)


async def test_known_root_loss_redacts_descendants_even_when_search_returns_no_page(
    database, source
):
    fixture = NativeFixture()
    service, fence = source
    instance, connector = await runner(database, source, fixture)
    root = connector._record("page", fixture.page)
    child = connector._record("block", fixture.blocks[0], parent=root.identity)
    async with database() as db:
        captured = await service.capture(db, CaptureBatch(fence=fence, records=(root, child)))

    async def missing(url, **kwargs):
        path = url.removeprefix("https://api.notion.com/v1/")
        if path == "search":
            return httpx.Response(200, json=listing(), request=httpx.Request("POST", url))
        assert path == f"pages/{PAGE}"
        return httpx.Response(
            404, json={"code": "object_not_found"}, request=httpx.Request("GET", url)
        )

    connector.http_client.get.side_effect = missing
    connector.http_client.post.side_effect = missing
    await run(instance)
    async with database() as db:
        for change in captured.changes:
            record = await service.store.read(
                db, fence.organization_id, fence.sync_id, change.record.id
            )
            assert record.content_access == "unavailable" and record.payload == {}
        rows = (await db.scalars(select(Entity))).all()
        assert all(row.removal_reason == "scope_removed" for row in rows)


async def test_prior_block_deleted_between_sweeps_reconciles_after_exact_confirmation(
    database, source
):
    fixture = NativeFixture()
    fixture.interrupt = False
    instance, connector = await runner(database, source, fixture)
    service, fence = source
    root = connector._record("page", fixture.page)
    child = connector._record("block", fixture.blocks[0], parent=root.identity)
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(root, child)))
    calls = []

    async def request(url, **kwargs):
        path = url.removeprefix("https://api.notion.com/v1/")
        calls.append(path)
        if path == f"blocks/{PAGE}/children":
            return httpx.Response(200, json=listing(), request=httpx.Request("GET", url))
        if path == f"blocks/{BLOCK1}":
            return httpx.Response(
                404, json={"code": "object_not_found"}, request=httpx.Request("GET", url)
            )
        return await fixture.request(url, **kwargs)

    connector.http_client.get.side_effect = request
    await run(instance)
    rows, checkpoint, _ = await saved(database)
    missing = next(row for row in rows if row.native_id == BLOCK1)
    assert missing.deleted_at is not None
    assert f"blocks/{BLOCK1}" in calls
    assert checkpoint["canonical_cycle"]["phase"] == "complete"


class PropertyFixture(NativeFixture):
    def __init__(self):
        super().__init__()
        self.page["properties"] = {
            name: {"id": name, "type": "title", "title": []} for name in ("a", "b")
        }
        self.property_calls = []
        self.interrupt_property = True

    async def request(self, url, *, headers, json=None, params=None):
        path = url.removeprefix("https://api.notion.com/v1/")
        if path == f"blocks/{PAGE}/children":
            return httpx.Response(200, json=listing(), request=httpx.Request("GET", url))
        if "/properties/" in path:
            prop = path.rsplit("/", 1)[-1]
            cursor = (params or {}).get("start_cursor")
            self.property_calls.append((prop, cursor))
            if prop == "b" and cursor and self.interrupt_property:
                raise ConnectionError("synthetic interruption within one property")
            more = prop == "b" and cursor is None
            payload = {
                **listing(
                    [
                        {
                            "object": "property_item",
                            "id": prop,
                            "type": "title",
                            "title": {"plain_text": "retained"},
                        }
                    ],
                    "following" if more else None,
                ),
                "type": "property_item",
                "property_item": {"id": prop, "type": "title", "next_url": None},
            }
            return httpx.Response(200, json=payload, request=httpx.Request("GET", url))
        return await super().request(url, headers=headers, json=json, params=params)


async def test_property_retry_keeps_committed_sibling_and_restarts_only_interrupted_property(
    database, source, tmp_path, monkeypatch
):
    import json

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.storage.file_service import FileService

    service, fence = source
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir",
        lambda _: str(tmp_path / "temp"),
    )
    files = FileService(
        fence.job_id, FilesystemBackend(tmp_path / "storage"), sync_id=fence.sync_id
    )
    fixture = PropertyFixture()
    first, _ = await runner(database, source, fixture, files=files)
    with pytest.raises(ConnectionError, match="within one property"):
        await run(first)
    rows, checkpoint, scans = await saved(database)
    properties = [row for row in rows if row.entity_definition_short_name == "page_property"]
    assert [row.native_id for row in properties] == ["a"]
    assert "canonical_checkpoint" not in checkpoint
    scan = next(row for row in scans if row.record_type == "page_property")
    assert scan.continuation["index"] == 1
    assert len(await files.storage.list_files()) == 1
    before = len(fixture.property_calls)
    fixture.interrupt_property = False
    second, _ = await runner(database, source, fixture, attempt=2, files=files)
    await run(second)
    assert fixture.property_calls[before:] == [("b", None), ("b", "following")]
    rows, checkpoint, _ = await saved(database)
    properties = [row for row in rows if row.entity_definition_short_name == "page_property"]
    assert {row.native_id for row in properties} == {"a", "b"}
    assert all(row.parent_record_type == "page" and row.container_id == PAGE for row in properties)
    assert next(row for row in properties if row.native_id == "a").record_revision == 1
    second_property = next(row for row in properties if row.native_id == "b")
    archive = json.loads(await files.storage.read_file(second_property.blob_references[0]["key"]))
    assert len(archive["responses"]) == 2
    assert checkpoint["canonical_cycle"]["phase"] == "complete"

    # A genuinely later job gets a new cycle and reconciles a removed property.
    from airweave.models.sync_job import SyncJob

    previous_cycle = checkpoint["canonical_cycle"]["version"]["cycle_id"]
    next_job, next_attempt = uuid4(), uuid4()
    async with database() as db:
        old_job = await db.get(SyncJob, fence.job_id)
        old_job.status = "completed"
        db.add(
            SyncJob(
                id=next_job,
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                status="running",
            )
        )
        await db.commit()
    fixture.page["properties"].pop("a")
    fixture.page["last_edited_time"] = "2026-09-30T02:00:00Z"
    next_source = (
        service,
        fence.model_copy(update={"job_id": next_job, "attempt_id": next_attempt}),
    )
    third, _ = await runner(database, next_source, fixture, files=files)
    await run(third)
    rows, checkpoint, _ = await saved(database)
    removed = next(
        row
        for row in rows
        if row.entity_definition_short_name == "page_property" and row.native_id == "a"
    )
    assert removed.deleted_at is not None and removed.removal_reason == "absent"
    assert checkpoint["canonical_cycle"]["version"]["cycle_id"] != previous_cycle
    assert checkpoint["canonical_cycle"]["phase"] == "complete"


async def test_removed_native_property_reconciles_only_after_current_inventory(database, source):
    from datetime import datetime, timezone

    from airweave.domains.entities.canonical.requests import CaptureRecord, RecordIdentity

    fixture = NativeFixture()
    fixture.interrupt = False
    instance, connector = await runner(database, source, fixture)
    service, fence = source
    root = connector._record("page", fixture.page)
    old_property = CaptureRecord(
        identity=RecordIdentity(
            record_type="page_property", native_id="removed", container_id=PAGE
        ),
        parent=root.identity,
        payload={"native": "previously captured value"},
        observed_at=datetime.now(timezone.utc),
        completeness="partial",
    )
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(root, old_property)))
    await run(instance)
    rows, checkpoint, _ = await saved(database)
    removed = next(row for row in rows if row.native_id == "removed")
    assert removed.deleted_at is not None and removed.removal_reason == "absent"
    assert next(row for row in rows if row.native_id == PAGE).deleted_at is None
    assert checkpoint["canonical_cycle"]["phase"] == "complete"
