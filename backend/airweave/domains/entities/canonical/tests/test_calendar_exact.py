"""Real SQL checks for native identity, version conflicts and withdrawn scope."""

from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.calendar_exact import CalendarReadIncomplete, read_event
from airweave.domains.entities.canonical.query import RecordNotFound
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.tests.helpers import bind_projection
from airweave.platform.sources.records.google_calendar import record


async def seed(
    database,
    source,
    raw_status="confirmed",
    expanded_status="cancelled",
    raw_updated="2026-09-01T00:00:00Z",
    expanded_updated="2026-09-02T00:00:00Z",
):
    capture, fence = source
    await bind_projection(database, fence)
    parent = record("calendar", {"id": "cal"})

    def item(kind, status, updated):
        payload = {
            "id": "exact",
            "status": status,
            "updated": updated,
            "summary": "private body",
            "start": {"date": "2026-09-01"},
            "end": {"date": "2026-09-02"},
        }
        return record(kind, payload, "cal").model_copy(update={"parent": parent.identity})

    observations = (
        parent,
        item("event", raw_status, raw_updated),
        item("event_occurrence", expanded_status, expanded_updated),
    )
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=observations))
    return parent


async def test_newer_provider_deletion_redacts_body_and_wrong_scope_is_absent(database, source):
    await seed(database, source)
    _, fence = source
    async with database() as db:
        result = await read_event(db, fence.organization_id, fence.sync_id, "cal", "exact")
        assert result.deleted_at and result.payload == {} and result.blobs == ()
        assert result.identity.record_type == "event_occurrence"
        with pytest.raises(RecordNotFound):
            await read_event(db, fence.organization_id, fence.sync_id, "other", "exact")
        with pytest.raises(RecordNotFound):
            await read_event(db, uuid4(), fence.sync_id, "cal", "exact")


async def test_reinstatement_uses_provider_version_not_capture_order(database, source):
    await seed(database, source, "cancelled", "confirmed")
    _, fence = source
    async with database() as db:
        result = await read_event(db, fence.organization_id, fence.sync_id, "cal", "exact")
        assert result.deleted_at is None and result.payload["status"] == "confirmed"


async def test_equal_versions_conflicting_status_fail_closed(database, source):
    await seed(database, source, expanded_updated="2026-09-01T00:00:00Z")
    _, fence = source
    async with database() as db:
        with pytest.raises(CalendarReadIncomplete):
            await read_event(db, fence.organization_id, fence.sync_id, "cal", "exact")


async def test_parent_withdrawal_blocks_both_candidates(database, source):
    parent = await seed(database, source)
    capture, fence = source
    withdrawn = parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"})
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(withdrawn,)))
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await read_event(db, fence.organization_id, fence.sync_id, "cal", "exact")


async def test_horizon_eviction_does_not_delete_raw_event(database, source):
    await seed(database, source, expanded_status="confirmed")
    capture, fence = source
    eviction = record("event_occurrence", {"id": "exact"}, "cal").model_copy(
        update={"kind": "delete", "removal_reason": "scope_removed"}
    )
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(eviction,)))
    async with database() as db:
        result = await read_event(db, fence.organization_id, fence.sync_id, "cal", "exact")
        assert result.deleted_at is None and result.identity.record_type == "event"


async def test_missing_native_versions_fail_closed(database, source):
    await seed(database, source, raw_updated=None, expanded_updated=None)
    _, fence = source
    async with database() as db:
        with pytest.raises(CalendarReadIncomplete):
            await read_event(db, fence.organization_id, fence.sync_id, "cal", "exact")


async def test_cancelled_recurring_tombstone_keeps_only_exclusion_identity(database, source):
    capture, fence = source
    await bind_projection(database, fence)
    parent = record("calendar", {"id": "cal"})
    exclusion = record(
        "event_occurrence",
        {
            "id": "exception",
            "status": "cancelled",
            "recurringEventId": "series",
            "originalStartTime": {"date": "2026-09-01"},
            "summary": "must not leak",
            "attendees": [{"email": "private@example.com"}],
        },
        "cal",
    ).model_copy(update={"parent": parent.identity})
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(parent, exclusion)))
    async with database() as db:
        result = await read_event(db, fence.organization_id, fence.sync_id, "cal", "exception")
        assert result.deleted_at is not None
        assert result.payload == {
            "id": "exception",
            "status": "cancelled",
            "recurringEventId": "series",
            "originalStartTime": {"date": "2026-09-01"},
        }


async def test_exact_http_is_source_authenticated_and_tenant_scoped(database, source):
    from types import SimpleNamespace

    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy import update

    from airweave.api import deps
    from airweave.api.v1.endpoints.calendar_records import router
    from airweave.api.v1.endpoints.records import record_error_response
    from airweave.db.session import get_db
    from airweave.domains.entities.canonical.store import CanonicalStoreError
    from airweave.models.source_connection import SourceConnection

    await seed(database, source)
    _, fence = source
    async with database() as db:
        db.add(
            SourceConnection(
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                name="Synthetic",
                short_name="google_calendar",
                is_authenticated=True,
            )
        )
        await db.commit()
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)
    owner = fence.organization_id

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_owned_context] = context
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_tenant_db] = session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        path = f"/sync/{fence.sync_id}/calendar/events/exact"
        result = await client.get(path, params={"calendar_id": "cal"})
        assert result.status_code == 200 and result.json()["payload"] == {}
        owner = uuid4()
        assert (await client.get(path, params={"calendar_id": "cal"})).status_code == 404
        owner = fence.organization_id
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        assert (await client.get(path, params={"calendar_id": "cal"})).status_code == 404


async def test_same_cancelled_exception_raw_state_beats_duplicate_tombstone(database, source):
    from airweave.domains.entities.canonical.calendar_exact import choose

    capture, fence = source
    await bind_projection(database, fence)
    parent = record("calendar", {"id": "cal"})
    payload = {
        "id": "exception",
        "status": "cancelled",
        "etag": "same",
        "recurringEventId": "series",
        "originalStartTime": {"date": "2026-09-01"},
    }
    observations = tuple(
        record(kind, payload, "cal").model_copy(update={"parent": parent.identity})
        for kind in ("event", "event_occurrence")
    )
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(parent, *observations)))
    async with database() as db:
        result = await read_event(db, fence.organization_id, fence.sync_id, "cal", "exception")
        assert result.deleted_at is None and result.identity.record_type == "event"
        assert result.payload["originalStartTime"] == {"date": "2026-09-01"}
        with pytest.raises(RecordNotFound):
            choose(
                (
                    result.model_copy(
                        update={"content_access": "unavailable", "removal_reason": None}
                    ),
                )
            )
