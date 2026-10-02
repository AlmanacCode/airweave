"""Actual composed calendar source and SQL with synthetic Graph HTTP only."""

from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.store import source_record
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.entities.canonical.tests.test_mixed_scopes import next_job
from airweave.domains.sources.exceptions import SourceEntityNotFoundError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.platform.configs.config import CalendarOccurrenceWindow, OutlookCalendarConfig
from airweave.platform.sources.outlook_calendar import OutlookCalendarSource


class CalendarHTTP:
    def __init__(self):
        self.round = 1
        self.interrupt = False
        self.missing = False
        self.calls = []

    async def handle(self, request):
        path = request.url.path
        if path == "/v1.0/me":
            return httpx.Response(200, json={"id": "mailbox"})
        assert request.headers["Prefer"] == 'IdType="ImmutableId"'
        self.calls.append((path, dict(request.url.params)))
        if path == "/v1.0/me/calendars":
            return httpx.Response(200, json={"value": [{"id": "cal"}]})
        if path == "/v1.0/me/calendars/cal":
            return httpx.Response(200, json={"id": "cal", "canViewPrivateItems": True})
        if path.endswith("/events"):
            return httpx.Response(200, json={"value": [{"id": "single"}, {"id": "master"}]})
        if path.endswith("/calendarView"):
            if self.interrupt:
                raise ConnectionError("Synthetic phase interruption")
            ids = ["single", "history" if self.round == 1 else "current"]
            return httpx.Response(200, json={"value": [{"id": item} for item in ids]})
        identity = path.rsplit("/", 1)[-1]
        assert identity in {"single", "master", "history", "current"}
        if identity == "history" and self.missing:
            return httpx.Response(404, json={"error": {"code": "ErrorItemNotFound"}})
        kind = {"single": "singleInstance", "master": "seriesMaster"}.get(identity, "occurrence")
        day = "2026-03-03" if identity == "history" else "2026-03-08"
        return httpx.Response(
            200,
            json={
                "id": identity,
                "changeKey": "stable",
                "type": kind,
                "start": {"dateTime": day + "T09:00:00", "timeZone": "UTC"},
                "end": {"dateTime": day + "T10:00:00", "timeZone": "UTC"},
                "isAllDay": False,
                "isCancelled": False,
                "hasAttachments": False,
                "subject": identity,
                "body": {"contentType": "text", "content": "Exact native body — नमस्ते"},
                "unknownNativeField": {"retained": True},
            },
        )


async def capture(database, source, native, attempt=1):
    service, fence = source
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handle)) as client:
        connector = await OutlookCalendarSource.create(
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            http_client=client,
            config=OutlookCalendarConfig(
                capture_originals=True,
                expected_principal_id="mailbox",
            ),
        )
        adapter = connector.capture_page_source
        assert adapter is not None
        ctx, _, runtime, bus = components(database, source)
        pipeline = CanonicalCapturePipeline(
            service,
            database,
            bus,
            adapter.canonical_record_types,
            CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
            adapter.canonical_container_parents,
            page_source=adapter,
            files=MagicMock(),
        )
        await pipeline.start(ctx)

        async def no_limits():
            pass

        await pipeline.run_scans(ctx, runtime, no_limits)
        await pipeline.cleanup_orphaned_entities(ctx, runtime)
        await pipeline.save_checkpoint(ctx, runtime)


async def state(database, source):
    async with database() as db:
        rows = (
            await db.scalars(select(Entity).where(Entity.entity_definition_short_name == "event"))
        ).all()
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        coverage = await capture_coverage(db, source[1].organization_id, (source[1].sync_id,))
        return {row.native_id: row for row in rows}, scan, coverage[source[1].sync_id]


@pytest.mark.parametrize("missing", [False, True])
async def test_calendar_phase_resume_and_exact_historical_validation(
    database, source, missing, monkeypatch
):
    native = CalendarHTTP()
    clock = {"start": "2026-03-01T00:00:00Z"}
    monkeypatch.setattr(
        OutlookCalendarConfig,
        "resolved_window",
        lambda self: CalendarOccurrenceWindow(start=clock["start"], end="2026-04-01T00:00:00Z"),
    )
    await capture(database, source, native)
    initial, _, coverage = await state(database, source)
    assert set(initial) == {"single", "master", "history"}
    assert coverage.phase == "complete" and coverage.discovery == "incomplete"
    source = await next_job(database, source)
    native.round, native.interrupt = 2, True
    clock["start"] = "2026-03-06T00:00:00Z"
    with pytest.raises(ConnectionError, match="phase interruption"):
        await capture(database, source, native)
    _, interrupted, coverage = await state(database, source)
    assert interrupted.phase == "collecting"
    assert interrupted.continuation["phase"] == "calendarView"
    assert coverage.phase == "active"
    sweep, window = interrupted.sweep_id, interrupted.continuation["context"]["window"]
    call_boundary = len(native.calls)
    native.interrupt, native.missing = False, missing
    clock["start"] = "2026-03-07T00:00:00Z"
    if missing:
        with pytest.raises(SourceEntityNotFoundError):
            await capture(database, source, native, attempt=2)
    else:
        await capture(database, source, native, attempt=2)
    rows, scan, coverage = await state(database, source)
    assert scan.sweep_id == sweep
    assert scan.continuation["context"]["window"] == window
    assert set(rows) == {"single", "master", "history", "current"}
    assert all(row.deleted_at is None for row in rows.values())
    assert rows["single"].id == initial["single"].id
    assert rows["single"].record_revision == initial["single"].record_revision
    assert all(row.parent_native_id == "cal" and row.container_id == "cal" for row in rows.values())
    assert rows["history"].id == initial["history"].id
    assert rows["single"].source_payload["unknownNativeField"] == {"retained": True}
    resumed = native.calls[call_boundary:]
    assert resumed[0][0] == "/v1.0/me/calendars"
    assert not any(path.endswith("/events") for path, _ in resumed)
    assert any(path.endswith("/events/history") for path, _ in resumed)
    view = next(params for path, params in resumed if path.endswith("/calendarView"))
    assert view["startDateTime"].startswith("2026-03-06")
    assert coverage.phase == ("active" if missing else "complete")
    assert scan.phase == ("reconciling" if missing else "complete")
    if not missing:
        async with map_record(
            source_record(rows["single"]), "outlook_calendar", MagicMock()
        ) as mapped:
            assert any(
                part.native_body and part.native_body.text == "Exact native body — नमस्ते"
                for part in mapped.parts
            )
            assert any(part.entity is None for part in mapped.parts)
        assert scan.execution_state["published"]["request_context"]["window"] == window
        assert scan.execution_state["published"]["checkpoint"] is None
        assert all(row.last_seen_run_id == sweep for row in rows.values())


@pytest.mark.parametrize("mode,checkpoint", [("changes", None), ("full", {"token": "unsafe"})])
async def test_full_scope_plan_rejects_incremental_authority(database, source, mode, checkpoint):
    from airweave.domains.entities.canonical.cycle_models import ProviderCheckpoint
    from airweave.domains.entities.canonical.scan_store import ScanConflict
    from airweave.domains.entities.canonical.scope_execution import ScopePlan
    from airweave.domains.entities.canonical.tests.test_scans import begin

    with pytest.raises(ScanConflict, match="cannot request incremental"):
        await begin(
            database,
            *source,
            plan=ScopePlan(
                mode=mode,
                starting_checkpoint=ProviderCheckpoint(value=checkpoint) if checkpoint else None,
            ),
        )
    async with database() as db:
        assert await db.scalar(select(CaptureScan)) is None


async def test_full_scope_plan_rejects_parent_changed_during_planning(
    database, source, monkeypatch
):
    from datetime import datetime, timezone

    from airweave.domains.entities.canonical.requests import CaptureRecord
    from airweave.domains.entities.canonical.scan_store import ScanConflict
    from airweave.domains.entities.canonical.tests.helpers import capture as capture_records
    from airweave.platform.sources.outlook_calendar_capture import OutlookCalendarCapture

    original = OutlookCalendarCapture.prepare_scope

    async def race(self, scope, cycle, previous, *, parent, force_full):
        plan = await original(self, scope, cycle, previous, parent=parent, force_full=force_full)
        if parent is not None:
            await capture_records(
                database,
                *source,
                CaptureRecord(
                    identity=parent.identity,
                    payload={**parent.payload, "name": "Renamed while selecting plan"},
                    observed_at=datetime.now(timezone.utc),
                ),
            )
        return plan

    monkeypatch.setattr(OutlookCalendarCapture, "prepare_scope", race)
    native = CalendarHTTP()
    with pytest.raises(ScanConflict, match="owner changed during plan selection"):
        await capture(database, source, native)
    async with database() as db:
        assert (
            await db.scalar(select(Entity).where(Entity.entity_definition_short_name == "event"))
            is None
        )
        assert (
            await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event")) is None
        )
    assert not any(path.endswith("/events") for path, _ in native.calls)
