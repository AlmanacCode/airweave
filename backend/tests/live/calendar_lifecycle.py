"""Private SQL assertions for Calendar lifecycle; no token output."""

from datetime import datetime
from urllib.parse import unquote
from uuid import UUID

from sqlalchemy import select

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.scope_execution import ScopeExecution
from airweave.domains.entities.canonical.store import content_is_available
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity


async def event_checkpoints(db, organization_id, sync_id):
    """Load each exact calendar's previously published native boundary."""
    rows = await db.scalars(
        select(CaptureScan).where(
            CaptureScan.organization_id == organization_id,
            CaptureScan.sync_id == sync_id,
            CaptureScan.record_type == "event",
        )
    )
    checkpoints = {}
    for row in rows:
        if row.execution_state:
            execution = ScopeExecution.model_validate(row.execution_state)
            if execution.published and execution.published.checkpoint:
                checkpoints[row.container_id] = execution.published.checkpoint
    return checkpoints


def count_calendar_request(request, previous, counters):
    """A native delta must use its own calendar's exact published checkpoint."""
    token = request.url.params.get("syncToken")
    if not token:
        return
    path = unquote(request.url.path)
    assert "/calendars/" in path and path.endswith("/events")
    calendar_id = path.split("/calendars/", 1)[1].removesuffix("/events")
    checkpoint = previous.get(calendar_id)
    assert checkpoint is not None and token == checkpoint.value["sync_token"]
    counters["sync_token_requests"] += 1


async def verify_calendar_scopes(sessions, organization_id, sync_id, manifest, saved, loaded):
    """Require published scopes for all currently available retained calendars."""
    if manifest["provider"] != "google_calendar":
        return ()
    cycle = saved["canonical_cycle"]
    async with sessions() as db:
        calendar_ids = tuple(
            await db.scalars(
                select(Entity.native_id).where(
                    Entity.organization_id == organization_id,
                    Entity.sync_id == sync_id,
                    Entity.entity_definition_short_name == "calendar",
                    content_is_available(),
                )
            )
        )
        rows = list(
            await db.scalars(
                select(CaptureScan).where(
                    CaptureScan.organization_id == organization_id,
                    CaptureScan.sync_id == sync_id,
                    CaptureScan.cycle_id == UUID(cycle["version"]["cycle_id"]),
                )
            )
        )
        coverage = (await capture_coverage(db, organization_id, (sync_id,)))[sync_id]
    selected = manifest["calendar_config"].get("calendar_ids")
    if selected is not None:
        assert set(calendar_ids) == set(selected)
    expected = {("calendar", None)} | {
        (kind, calendar_id)
        for calendar_id in calendar_ids
        for kind in ("event", "event_occurrence")
    }
    # Historical withdrawn scopes can remain; only current eligible scopes qualify.
    rows = [row for row in rows if (row.record_type, row.container_id) in expected]
    assert {(row.record_type, row.container_id) for row in rows} == expected
    assert len(rows) == len(expected)
    full, changes = 0, 0
    for row in rows:
        assert row.phase == "complete" and str(row.cycle_id) == cycle["version"]["cycle_id"]
        execution = ScopeExecution.model_validate(row.execution_state)
        assert execution.last_full is not None and execution.published is not None
        assert execution.published.sweep_id == row.sweep_id
        if execution.plan.mode == "changes":
            assert loaded and row.record_type == "event"
            changes += 1
        else:
            assert execution.plan.mode == "full"
            full += 1
        if row.record_type == "event":
            assert execution.published.checkpoint.value["sync_token"]
        if row.record_type == "event_occurrence":
            params = execution.published.request_context["parameters"]
            window = cycle["source_plan"]["window"]
            assert datetime.fromisoformat(params["timeMin"]) == datetime.fromisoformat(
                window["start"]
            )
            assert datetime.fromisoformat(params["timeMax"]) == datetime.fromisoformat(
                window["end"]
            )
    assert coverage.phase == "complete" and coverage.mode == "mixed"
    assert coverage.last_full_capture is None and coverage.provider_checkpoint_promoted_at is None
    assert coverage.scope_summary.model_dump() == {
        "eligible": len(expected),
        "completed_full": full,
        "completed_changes": changes,
        "unfinished": 0,
    }
    return calendar_ids
