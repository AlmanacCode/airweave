"""Stored provider instances support bounded ranges without recurrence guessing."""

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from airweave.api import deps
from airweave.api.v1.endpoints.calendar_records import router
from airweave.api.v1.endpoints.records import record_error_response
from airweave.db.session import get_db
from airweave.domains.entities.canonical.calendar_query import (
    CalendarChanged,
    CalendarRange,
    CalendarRangeNotCaptured,
    CalendarRangeService,
)
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.models import SyncCursor
from airweave.models.source_connection import SourceConnection
from airweave.platform.cursors.google_calendar import CalendarWindowCoverage
from airweave.platform.sources.records.google_calendar import record


def window():
    return CalendarRange(
        start="2026-03-08T00:00:00-08:00",
        end="2026-03-10T00:00:00-07:00",
        timezone="America/Los_Angeles",
        limit=1,
    )


async def seed(database, source):
    capture, fence = source
    parent = record("calendar", {"id": "cal", "timeZone": "America/Los_Angeles"})
    events = [
        {"id": "all-day", "start": {"date": "2026-03-08"}, "end": {"date": "2026-03-09"}},
        {
            "id": "real-instance-id",
            "recurringEventId": "master",
            "originalStartTime": {"dateTime": "2026-03-01T09:00:00-08:00"},
            "start": {"dateTime": "2026-03-09T09:00:00-07:00"},
            "end": {"dateTime": "2026-03-09T10:00:00-07:00"},
        },
        {"id": "ends-at-boundary", "start": {"date": "2026-03-07"}, "end": {"date": "2026-03-08"}},
    ]
    observations = (
        parent,
        *(
            record("event_occurrence", item, "cal").model_copy(update={"parent": parent.identity})
            for item in events
        ),
    )
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=observations))
    coverage = CalendarWindowCoverage(
        start="2026-03-01T00:00:00Z",
        end="2026-04-01T00:00:00Z",
        timezone="America/Los_Angeles",
        completed_at=datetime.now(timezone.utc),
        scan_id=uuid4(),
    )
    async with database() as db:
        await capture.save_checkpoint(
            db,
            fence,
            {
                "occurrence_coverage": {"cal": coverage.model_dump(mode="json")},
                "canonical_checkpoint": {
                    "writer_attempt_id": str(uuid4()),
                    "observed_change_sequence": 999999,
                },
            },
        )
    return parent, coverage


async def test_paged_range_all_day_moved_instance_identity_and_changes(database, source):
    parent, coverage = await seed(database, source)
    capture, fence = source
    service = CalendarRangeService("secret")
    async with database() as db:
        first = await service.read(db, fence.organization_id, fence.sync_id, "cal", window())
        assert first.records[0].identity.native_id == "all-day"
        assert first.has_more and first.refresh_state == "last_completed"
        next_query = window().model_copy(update={"cursor": first.next_cursor})
        second = await service.read(db, fence.organization_id, fence.sync_id, "cal", next_query)
        assert second.records[0].identity.native_id == "real-instance-id" and not second.has_more
        saved = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == fence.sync_id))
        assert saved.cursor_data["canonical_checkpoint"]["writer_attempt_id"] == str(
            fence.attempt_id
        )
        assert saved.cursor_data["canonical_checkpoint"]["observed_change_sequence"] == 4
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    record(
                        "event_occurrence",
                        {
                            "id": "late",
                            "start": {"date": "2026-03-09"},
                            "end": {"date": "2026-03-10"},
                        },
                        "cal",
                    ),
                ),
            ),
        )
    async with database() as db:
        with pytest.raises(CalendarChanged):
            await service.read(db, fence.organization_id, fence.sync_id, "cal", next_query)
        current = await service.read(db, fence.organization_id, fence.sync_id, "cal", window())
        assert current.coverage == coverage and current.refresh_state != "last_completed"
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    parent.model_copy(
                        update={"kind": "delete", "removal_reason": "access_revoked"}
                    ),
                ),
            ),
        )
    async with database() as db:
        with pytest.raises(CanonicalStoreError):
            await service.read(db, fence.organization_id, fence.sync_id, "cal", window())


async def test_http_missing_coverage_action_and_cross_org(database, source, monkeypatch):
    capture, fence = source
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence, records=(record("calendar", {"id": "cal", "timeZone": "UTC"}),)
            ),
        )
    async with database() as db:
        db.add(
            SourceConnection(
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                name="Synthetic Calendar",
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

    app.dependency_overrides[deps.get_context] = context
    app.dependency_overrides[get_db] = session
    params = {"calendar_id": "cal", "start": "2026-03-01T00:00:00Z", "end": "2026-03-02T00:00:00Z"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        path = f"/sync/{fence.sync_id}/calendar/events"
        missing = await client.get(path, params=params)
        assert missing.status_code == 409
        assert missing.json()["error"]["action"]["operation"] == "sync_calendar_window"
        owner = uuid4()
        assert (await client.get(path, params=params)).status_code == 404
        owner = fence.organization_id
        assert (
            await client.get(path, params={**params, "timezone": "not/a/zone"})
        ).status_code == 422
        await seed(database, source)
        original_read = CalendarRangeService.read

        async def revoke_during_read(self, *args, **kwargs):
            from sqlalchemy import update

            result = await original_read(self, *args, **kwargs)
            async with database() as other:
                await other.execute(
                    update(SourceConnection)
                    .where(SourceConnection.sync_id == fence.sync_id)
                    .values(is_authenticated=False)
                )
                await other.commit()
            return result

        monkeypatch.setattr(CalendarRangeService, "read", revoke_during_read)
        assert (await client.get(path, params=params)).status_code == 404


async def test_request_outside_successful_coverage_never_returns_empty_success(database, source):
    await seed(database, source)
    _, fence = source
    async with database() as db:
        with pytest.raises(CalendarRangeNotCaptured):
            await CalendarRangeService("secret").read(
                db,
                fence.organization_id,
                fence.sync_id,
                "cal",
                CalendarRange(
                    start="2001-01-01T00:00:00Z", end="2001-01-02T00:00:00Z", timezone="UTC"
                ),
            )


async def test_empty_observed_range_is_distinct_from_missing_coverage(database, source):
    await seed(database, source)
    _, fence = source
    async with database() as db:
        page = await CalendarRangeService("secret").read(
            db,
            fence.organization_id,
            fence.sync_id,
            "cal",
            CalendarRange(start="2026-03-20T00:00:00Z", end="2026-03-21T00:00:00Z", timezone="UTC"),
        )
        assert page.records == () and not page.has_more
        assert page.coverage and page.refresh_state == "last_completed"


async def test_budget_rejects_oversized_row_before_orm_materialization(database, source):
    from sqlalchemy import update

    from airweave.domains.entities.canonical.calendar_query import CalendarRangeError
    from airweave.models import Entity

    await seed(database, source)
    _, fence = source
    async with database() as db:
        await db.execute(
            update(Entity)
            .where(Entity.sync_id == fence.sync_id, Entity.native_id == "all-day")
            .values(source_payload={"oversized": "x" * (21 * 1024 * 1024)})
        )
        await db.commit()
    async with database() as db:
        with pytest.raises(CalendarRangeError):
            await CalendarRangeService._bounded_rows(
                db, fence.organization_id, fence.sync_id, "cal"
            )
        assert not any(isinstance(item, Entity) for item in db.identity_map.values())


async def test_empty_selection_reconciles_partial_parent_without_saved_cursor(database, source):
    from airweave.domains.entities.canonical.requests import (
        CompletedScope,
        ReconcileScope,
        StartedScope,
    )
    from airweave.platform.sources.records.google_calendar import generate_calendar_observations

    capture, fence = source
    parent = record("calendar", {"id": "partial", "timeZone": "UTC"})
    child = record("event", {"id": "old", "status": "confirmed"}, "partial").model_copy(
        update={"parent": parent.identity}
    )
    async with database() as db:
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(parent, child)))
    child_id = result.changes[-1].record.id

    async def never_get(*args, **kwargs):
        raise AssertionError("Empty selection must not fetch provider")

    # Same-attempt retry is intentionally harder: StartedScope must clear old sightings.
    async for item in generate_calendar_observations(never_get, None, calendar_ids=()):
        if isinstance(item, StartedScope):
            async with database() as db:
                await capture.start_scope(db, fence, item)
        elif type(item) is CompletedScope:
            async with database() as db:
                await capture.reconcile_scope(
                    db,
                    ReconcileScope(fence=fence, scope=item, observed_at=datetime.now(timezone.utc)),
                )
    async with database() as db:
        current = await capture.store.read(db, fence.organization_id, fence.sync_id, child_id)
        assert current.content_access == "unavailable" and current.payload == {}
        await db.rollback()
        removed = await capture.reconcile_parents(db, fence)
        assert removed.capture.changes[0].record.removal_reason != "provider_deleted"


async def test_missing_selected_calendar_revokes_partial_parent_without_cursor(database, source):
    from airweave.domains.entities.canonical.requests import (
        CaptureRecord,
        RemovedScope,
        StartedScope,
    )
    from airweave.platform.sources.records.google_calendar import generate_calendar_observations

    capture, fence = source
    parent = record("calendar", {"id": "missing", "timeZone": "UTC"})
    child = record("event", {"id": "retained", "status": "confirmed"}, "missing").model_copy(
        update={"parent": parent.identity}
    )
    async with database() as db:
        initial = await capture.capture(db, CaptureBatch(fence=fence, records=(parent, child)))
    child_id = initial.changes[-1].record.id

    async def empty_membership(*args, **kwargs):
        return {"items": []}

    with pytest.raises(ValueError, match="not accessible"):
        async for item in generate_calendar_observations(
            empty_membership, None, calendar_ids=("missing",)
        ):
            async with database() as db:
                if isinstance(item, CaptureRecord):
                    await capture.capture(db, CaptureBatch(fence=fence, records=(item,)))
                elif isinstance(item, StartedScope):
                    await capture.start_scope(db, fence, item)
                elif isinstance(item, RemovedScope):
                    await capture.remove_scope(db, fence, item)
                else:
                    raise AssertionError("Missing requested scope cannot complete")
    async with database() as db:
        current = await capture.store.read(db, fence.organization_id, fence.sync_id, child_id)
        assert current.content_access == "unavailable" and current.payload == {}
        assert current.removal_reason == "access_revoked"
        assert (
            await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == fence.sync_id)) is None
        )
