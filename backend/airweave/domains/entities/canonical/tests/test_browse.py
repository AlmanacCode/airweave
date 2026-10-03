"""Retained chronology crosses sources without requiring indexed publication."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError
from sqlalchemy import select, update

from airweave.domains.entities.canonical.query import CanonicalQueryService, InvalidRecordCursor
from airweave.domains.entities.canonical.query_models import RecordBrowseFilters, RecordBrowseQuery
from airweave.domains.entities.canonical.query_store import (
    CanonicalQueryStore,
    RecordMetadataUnavailable,
)
from airweave.domains.entities.canonical.requests import CaptureBatch, RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore, SourceNotFound
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.models import Sync, SyncJob
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection

START = datetime(2026, 10, 1, tzinfo=timezone.utc)


def query_service():
    return CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "browse-test-key")


def test_browse_contract_rejects_ambiguous_scope_and_normalizes_sets():
    one, two = uuid4(), uuid4()
    common = {"basis": "source_created", "sync_ids": (two, one)}
    assert RecordBrowseFilters(**common).sync_ids == tuple(sorted((one, two)))
    with pytest.raises(ValidationError):
        RecordBrowseFilters(**{**common, "sync_ids": (one, one)})
    with pytest.raises(ValidationError):
        RecordBrowseFilters(**common, created_after=START, created_before=START)
    with pytest.raises(ValidationError):
        RecordBrowseFilters(**{**common, "basis": "first_stored"})


async def second_source(database, capture, fence, provider):
    sync_id, job_id = uuid4(), uuid4()
    async with database() as db:
        db.add(
            Sync(id=sync_id, organization_id=fence.organization_id, name="Other retained source")
        )
        await db.flush()
        db.add(
            SyncJob(
                id=job_id, organization_id=fence.organization_id, sync_id=sync_id, status="running"
            )
        )
        await db.commit()
    async with database() as db:
        other = await capture.activate_writer(
            db, fence.organization_id, sync_id, job_id, attempt_id=uuid4(), attempt_number=1
        )
    await bind_projection(database, other, source_name=provider)
    return other


async def test_unprepared_cross_source_keyset_half_open_and_live_updates(database, source):
    capture, fence = source
    await bind_projection(database, fence, source_name="google_calendar")
    other = await second_source(database, capture, fence, "google_drive")
    for owner, records in (
        (
            fence,
            (
                observation("first", source_created_at=START, source_updated_at=START),
                observation("missing-clock"),
                observation("upper", source_created_at=START + timedelta(days=1)),
            ),
        ),
        (other, (observation("tie", source_created_at=START, source_updated_at=START),)),
    ):
        async with database() as db:
            await capture.capture(db, CaptureBatch(fence=owner, records=records))
    service = query_service()
    filters = RecordBrowseFilters(
        sync_ids=(fence.sync_id, other.sync_id),
        basis="source_created",
        created_after=START,
        created_before=START + timedelta(days=1),
    )
    orders = []
    for direction in ("asc", "desc"):
        scoped = filters.model_copy(update={"order": direction})
        async with database() as db:
            first = await service.browse(
                db, fence.organization_id, RecordBrowseQuery(filters=scoped, limit=1)
            )
            second = await service.browse(
                db,
                fence.organization_id,
                RecordBrowseQuery(filters=scoped, limit=1, cursor=first.next_cursor),
            )
            assert first.has_more and not second.has_more
            assert {x.identity.native_id for x in (*first.items, *second.items)} == {"first", "tie"}
            orders.append([first.items[0].record_id, second.items[0].record_id])
            assert first.coverage == "retained_traversal" and first.missing_clock == "excluded"
            assert "original" not in first.items[0].model_dump()
            assert (
                await db.scalar(
                    select(Entity.indexed_revision).where(Entity.id == first.items[0].record_id)
                )
                is None
            )
            for changed in (
                scoped.model_copy(update={"sync_ids": (fence.sync_id,)}),
                scoped.model_copy(update={"record_types": ("event",)}),
            ):
                with pytest.raises(InvalidRecordCursor):
                    await service.browse(
                        db,
                        fence.organization_id,
                        RecordBrowseQuery(filters=changed, cursor=first.next_cursor),
                    )
            with pytest.raises(InvalidRecordCursor):
                await service.browse(
                    db, uuid4(), RecordBrowseQuery(filters=scoped, cursor=first.next_cursor)
                )
    assert orders[0] == list(reversed(orders[1]))
    from airweave.api import deps
    from airweave.api.v1.endpoints.records import record_error_response, router
    from airweave.domains.entities.canonical.store import CanonicalStoreError

    app = FastAPI()
    app.include_router(router, prefix="/records")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_tenant_db] = session
    app.dependency_overrides[deps.get_owned_context] = lambda: SimpleNamespace(
        organization=SimpleNamespace(id=fence.organization_id)
    )
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: service
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        result = await client.post(
            "/records/browse", json=RecordBrowseQuery(filters=filters).model_dump(mode="json")
        )
        assert result.status_code == 200 and len(result.json()["items"]) == 2
        invalid = await client.post(
            "/records/browse",
            json={"filters": {"basis": "first_stored", "sync_ids": [str(fence.sync_id)]}},
        )
        assert invalid.status_code == 422
    # Live traversal may observe a moved row again; this is not a frozen snapshot.
    updated = filters.model_copy(update={"basis": "source_updated", "order": "asc"})
    async with database() as db:
        first = await service.browse(
            db, fence.organization_id, RecordBrowseQuery(filters=updated, limit=1)
        )
    async with database() as db:
        await db.execute(
            update(Entity)
            .where(Entity.id == first.items[0].record_id)
            .values(source_updated_at=START + timedelta(hours=1))
        )
        await db.commit()
    async with database() as db:
        later = await service.browse(
            db,
            fence.organization_id,
            RecordBrowseQuery(filters=updated, cursor=first.next_cursor, limit=1),
        )
        last = await service.browse(
            db,
            fence.organization_id,
            RecordBrowseQuery(filters=updated, cursor=later.next_cursor, limit=1),
        )
        assert last.items[0].record_id == first.items[0].record_id
        assert later.consistency == "live"
        with pytest.raises(SourceNotFound):
            await service.browse(db, uuid4(), RecordBrowseQuery(filters=filters))


async def test_authority_before_limit_and_native_version_without_body(database, source):
    capture, fence = source
    binding = await bind_projection(database, fence, source_name="almanac")
    parent = RecordIdentity(record_type="session", native_id="session")
    version = {
        "kind": "session",
        "revision": 2,
        "content_revision": 4,
        "created_at": START.isoformat(),
    }
    metadata = {
        "authority": "almanac",
        "representation": "snapshot",
        "schema_version": 1,
        "owner_id": "synthetic-owner",
        "version": version,
        "operation": "upsert",
    }
    child = RecordIdentity(record_type="message", native_id="message", container_id="session")
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    observation(
                        identity=parent,
                        payload={
                            **metadata,
                            "identity": parent.model_dump(),
                            "parent": None,
                            "original": {"title": "Session", "body": "MUST NOT RETURN"},
                        },
                        source_created_at=START,
                    ),
                ),
            ),
        )
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    observation(
                        identity=child,
                        parent=parent,
                        payload={
                            **metadata,
                            "identity": child.model_dump(),
                            "parent": parent.model_dump(),
                            "original": {"body": "PRIVATE"},
                        },
                        source_created_at=START + timedelta(minutes=1),
                    ),
                ),
            ),
        )
    safe = RecordIdentity(record_type="knowledge", native_id="safe")
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    observation(
                        identity=safe,
                        payload={
                            **metadata,
                            "identity": safe.model_dump(),
                            "version": {"kind": "record", "revision": 1},
                            "parent": None,
                            "original": {"type": "page", "title": "Safe older page"},
                        },
                        source_created_at=START - timedelta(days=1),
                    ),
                ),
            ),
        )
    service = query_service()
    filters = RecordBrowseFilters(sync_ids=(fence.sync_id,), basis="source_created")
    async with database() as db:
        page = await service.browse(db, fence.organization_id, RecordBrowseQuery(filters=filters))
        assert len(page.items) == 3 and page.items[0].native_version.content_revision == 4
        assert page.items[0].parent == parent
        assert (
            "PRIVATE" not in page.model_dump_json()
            and "MUST NOT RETURN" not in page.model_dump_json()
        )

    class WithdrawingQueries(CanonicalQueryStore):
        async def browse(self, db, *args):
            items = await super().browse(db, *args)
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.id == binding.source_connection_id)
                .values(is_authenticated=False)
            )
            await db.commit()
            return items

    withdrawing = CanonicalQueryService(CanonicalRecordStore(), WithdrawingQueries(), "test")
    async with database() as db:
        with pytest.raises(SourceNotFound):
            await withdrawing.browse(db, fence.organization_id, RecordBrowseQuery(filters=filters))
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.id == binding.source_connection_id)
            .values(is_authenticated=True)
        )
        await db.commit()
    # A withdrawn ancestor hides its otherwise newest child before LIMIT.
    async with database() as db:
        await db.execute(
            update(Entity)
            .where(Entity.native_id == "session")
            .values(removal_reason="access_revoked")
        )
        await db.commit()
    async with database() as db:
        visible = await service.browse(
            db, fence.organization_id, RecordBrowseQuery(filters=filters, limit=1)
        )
        assert [item.identity for item in visible.items] == [safe]
        await db.execute(
            update(Entity)
            .where(Entity.native_id == "session")
            .values(removal_reason=None, source_payload={"broken": True})
        )
        await db.commit()
    async with database() as db:
        with pytest.raises(RecordMetadataUnavailable):
            await service.browse(db, fence.organization_id, RecordBrowseQuery(filters=filters))
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.id == binding.source_connection_id)
            .values(is_authenticated=False)
        )
        await db.commit()
    async with database() as db:
        with pytest.raises(SourceNotFound):
            await service.browse(db, fence.organization_id, RecordBrowseQuery(filters=filters))
