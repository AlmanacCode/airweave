"""Exact query behavior against committed records, plus signed cursor isolation."""

from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.query import CanonicalQueryService, InvalidRecordCursor
from airweave.domains.entities.canonical.query_models import RecordFilters, RecordListQuery
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalRecordStore, SourceNotFound
from airweave.domains.entities.canonical.tests.helpers import observation


def query_service():
    return CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-test-key"
    )


async def test_list_filters_and_cursor_scope(database, source):
    capture, fence = source
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
