"""Exact query behavior against committed records, plus signed cursor isolation."""

from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.query import CanonicalQueryService, InvalidRecordCursor
from airweave.domains.entities.canonical.query_models import RecordFilters, RecordListQuery
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalRecordStore, SourceNotFound
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation


def query_service():
    return CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-test-key"
    )


async def test_list_filters_and_cursor_scope(database, source):
    capture, fence = source
    await bind_projection(database, fence)
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    observation("one", "inbox"),
                    observation("two", "inbox"),
                    observation("three", "elsewhere"),
                ),
            ),
        )
    service = query_service()
    filters = RecordFilters(container_id="inbox")
    async with database() as db:
        first = await service.list_records(
            db, fence.organization_id, fence.sync_id, RecordListQuery(filters=filters, limit=1)
        )
        second = await service.list_records(
            db,
            fence.organization_id,
            fence.sync_id,
            RecordListQuery(filters=filters, limit=1, cursor=first.next_cursor),
        )
        assert first.has_more and not second.has_more
        assert {row.identity.native_id for row in (*first.records, *second.records)} == {
            "one",
            "two",
        }
        assert first.records[0].id < second.records[0].id
        assert first.consistency == "live"
        with pytest.raises(InvalidRecordCursor):
            await service.list_records(
                db, fence.organization_id, fence.sync_id, RecordListQuery(cursor=first.next_cursor)
            )
        with pytest.raises(InvalidRecordCursor):
            await service.list_records(
                db,
                uuid4(),
                fence.sync_id,
                RecordListQuery(filters=filters, cursor=first.next_cursor),
            )
        with pytest.raises(SourceNotFound):
            await service.list_records(db, uuid4(), fence.sync_id, RecordListQuery())


async def test_change_window_continues_then_polls_new_commits(database, source):
    capture, fence = source
    await bind_projection(database, fence)
    service = query_service()
    async with database() as db:
        await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation("one"), observation("two")))
        )
    async with database() as db:
        first = await service.changes(db, fence.organization_id, fence.sync_id, limit=1)
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(observation("three"),)))
    async with database() as db:
        second = await service.changes(
            db, fence.organization_id, fence.sync_id, cursor=first.next_cursor, limit=1
        )
        third = await service.changes(
            db, fence.organization_id, fence.sync_id, cursor=second.next_cursor, limit=1
        )
    assert first.high_watermark == second.high_watermark == 2
    assert [first.changes[0].sequence, second.changes[0].sequence, third.changes[0].sequence] == [
        1,
        2,
        3,
    ]
    assert first.has_more and not second.has_more and not third.has_more
    assert third.high_watermark == 3


async def test_deleted_record_is_readable_but_not_in_active_list(database, source):
    capture, fence = source
    await bind_projection(database, fence)
    service = query_service()
    async with database() as db:
        result = await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(observation("deleted", kind="delete", removal_reason="provider_deleted"),),
            ),
        )
    record_id = result.changes[0].record.id
    async with database() as db:
        record = await service.read(db, fence.organization_id, fence.sync_id, record_id)
        active = await service.list_records(
            db, fence.organization_id, fence.sync_id, RecordListQuery()
        )
        deleted = await service.list_records(
            db,
            fence.organization_id,
            fence.sync_id,
            RecordListQuery(filters=RecordFilters(state="deleted")),
        )
    assert record.deleted_at is not None
    assert not active.records
    assert [row.id for row in deleted.records] == [record_id]


async def test_parent_filter_is_exact_visible_and_bound_to_continuation(database, source):
    from airweave.domains.entities.canonical.query import RecordNotFound
    from airweave.domains.entities.canonical.requests import RecordIdentity
    from airweave.domains.entities.canonical.tests.helpers import capture as store

    capture, fence = source
    await bind_projection(database, fence)
    parent = observation(
        identity=RecordIdentity(record_type="message", native_id="parent", container_id="C1")
    ).model_copy(
        update={"payload": {"files": ["F1", "F2"]}, "descendant_visibility_fields": ("files",)}
    )
    other = observation(
        identity=RecordIdentity(record_type="message", native_id="parent", container_id="C2")
    )
    children = tuple(
        observation(
            identity=RecordIdentity(record_type="file", native_id=name, container_id="owned"),
            parent=parent.identity,
        )
        for name in ("F1", "F2")
    )
    unrelated = observation(
        identity=RecordIdentity(record_type="file", native_id="F3", container_id="other"),
        parent=other.identity,
    )
    saved = await store(database, capture, fence, parent, other, *children, unrelated)
    ids = {change.record.identity: change.record.id for change in saved.changes}
    service = query_service()
    filters = RecordFilters(parent_record_id=ids[parent.identity])
    # Exercise the actual query-string boundary with explicit fixture authentication.
    from types import SimpleNamespace

    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from airweave.api import deps
    from airweave.api.v1.endpoints.records import router
    from airweave.db.session import get_db

    app = FastAPI()
    app.include_router(router, prefix="/sync")

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_context] = lambda: SimpleNamespace(
        organization=SimpleNamespace(id=fence.organization_id)
    )
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: service
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        response = await client.get(
            f"/sync/{fence.sync_id}/records", params={"parent_record_id": str(ids[parent.identity])}
        )
    assert response.status_code == 200
    assert {item["id"] for item in response.json()["records"]} == {
        str(ids[child.identity]) for child in children
    }
    async with database() as db:
        first = await service.list_records(
            db, fence.organization_id, fence.sync_id, RecordListQuery(filters=filters, limit=1)
        )
        assert first.has_more and first.records[0].parent == parent.identity
        with pytest.raises(InvalidRecordCursor):
            await service.list_records(
                db,
                fence.organization_id,
                fence.sync_id,
                RecordListQuery(
                    filters=RecordFilters(parent_record_id=ids[other.identity]),
                    cursor=first.next_cursor,
                ),
            )
        for organization, sync in ((uuid4(), fence.sync_id), (fence.organization_id, uuid4())):
            with pytest.raises(RecordNotFound):
                await service.list_records(db, organization, sync, RecordListQuery(filters=filters))
    # Attachment inventory changes hide previously attested children on the next page.
    await store(database, capture, fence, parent.model_copy(update={"payload": {"files": []}}))
    async with database() as db:
        page = await service.list_records(
            db,
            fence.organization_id,
            fence.sync_id,
            RecordListQuery(filters=filters, cursor=first.next_cursor),
        )
        assert not page.records
    await store(
        database,
        capture,
        fence,
        parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await service.list_records(
                db,
                fence.organization_id,
                fence.sync_id,
                RecordListQuery(filters=filters, cursor=first.next_cursor),
            )
