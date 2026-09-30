"""Private SQL assertions for the selected-calendar lifecycle; no token output."""

from datetime import datetime
from urllib.parse import unquote

from sqlalchemy import select

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.scope_execution import ScopeExecution
from airweave.models.capture_scan import CaptureScan


async def event_checkpoint(db, organization_id, sync_id, calendar_id):
    row = await db.scalar(
        select(CaptureScan).where(
            CaptureScan.organization_id == organization_id,
            CaptureScan.sync_id == sync_id,
            CaptureScan.record_type == "event",
            CaptureScan.container_id == calendar_id,
        )
    )
    if row is None or not row.execution_state:
        return None
    execution = ScopeExecution.model_validate(row.execution_state)
    return execution.published.checkpoint if execution.published else None


def count_calendar_request(request, calendar_id, previous, counters):
    if not request.url.params.get("syncToken"):
        return
    assert previous is not None
    assert unquote(request.url.path).endswith(f"/calendars/{calendar_id}/events")
    assert request.url.params["syncToken"] == previous.value["sync_token"]
    counters["sync_token_requests"] += 1


async def verify_calendar_scopes(sessions, organization_id, sync_id, manifest, saved, loaded):
    """Require real completed scope publication and current SQL public coverage."""
    if manifest["provider"] != "google_calendar":
        return
    calendar_id = manifest["calendar_config"]["calendar_ids"][0]
    async with sessions() as db:
        rows = list(
            await db.scalars(
                select(CaptureScan).where(
                    CaptureScan.organization_id == organization_id, CaptureScan.sync_id == sync_id
                )
            )
        )
        coverage = (await capture_coverage(db, organization_id, (sync_id,)))[sync_id]
    assert len(rows) == 3
    assert {row.record_type for row in rows} == {"calendar", "event", "event_occurrence"}
    cycle = saved["canonical_cycle"]
    for row in rows:
        assert row.phase == "complete" and str(row.cycle_id) == cycle["version"]["cycle_id"]
        assert row.container_id == (None if row.record_type == "calendar" else calendar_id)
        execution = ScopeExecution.model_validate(row.execution_state)
        assert execution.last_full is not None and execution.published is not None
        assert execution.published.sweep_id == row.sweep_id
        if row.record_type == "event":
            assert execution.plan.mode == ("changes" if loaded else "full")
            assert execution.published.checkpoint.value["sync_token"]
        if row.record_type == "event_occurrence":
            params = execution.published.request_context["parameters"]
            window = manifest["calendar_config"]["occurrence_window"]

            assert datetime.fromisoformat(params["timeMin"]) == datetime.fromisoformat(
                window["start"]
            )
            assert datetime.fromisoformat(params["timeMax"]) == datetime.fromisoformat(
                window["end"]
            )
    assert coverage.phase == "complete" and coverage.mode == "mixed"
    assert coverage.last_full_capture is None and coverage.provider_checkpoint_promoted_at is None
    assert coverage.scope_summary.model_dump() == {
        "eligible": 3,
        "completed_full": 2 if loaded else 3,
        "completed_changes": 1 if loaded else 0,
        "unfinished": 0,
    }
