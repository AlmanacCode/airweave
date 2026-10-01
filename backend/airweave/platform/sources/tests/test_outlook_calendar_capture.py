"""Synthetic Graph interpretation, not live provider or SQL recovery qualification."""

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.cycle_models import CaptureCycle, CycleVersion
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceEntityNotFoundError, SourceServerError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import CalendarOccurrenceWindow, OutlookCalendarConfig
from airweave.platform.sources.outlook_calendar_capture import BASE, OutlookCalendarCapture
from airweave.platform.sources.outlook_graph import OutlookBoundaryError, OutlookGraphClient

WINDOW = CalendarOccurrenceWindow(start="2026-10-01T00:00:00Z", end="2026-11-01T00:00:00Z")
ROOT = CompletedScope(record_type="calendar")
CALENDAR = {"id": "calendar", "name": "Original", "canViewPrivateItems": True}
EVENT = {
    "id": "single",
    "changeKey": "v1",
    "type": "singleInstance",
    "start": {"dateTime": "2026-10-02T10:00:00.0000000", "timeZone": "UTC"},
    "end": {"dateTime": "2026-10-02T11:00:00.0000000", "timeZone": "UTC"},
    "isAllDay": False,
    "isCancelled": False,
    "hasAttachments": False,
    "originalStartTimeZone": "India Standard Time",
    "nativeExtension": {"keep": [1, True, None]},
}


def retained(identity, payload, parent=None):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=identity,
        parent=parent,
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="a" * 64,
        content_hash=None,
        completeness="partial",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


PARENT = retained(RecordIdentity(record_type="calendar", native_id="calendar"), CALENDAR)


async def source_for(client):
    return await OutlookCalendarCapture.create(
        graph=OutlookGraphClient(
            StaticTokenProvider("secret"), client, "outlook_calendar", "principal"
        ),
        config=OutlookCalendarConfig(
            capture_originals=True, expected_principal_id="principal", occurrence_window=WINDOW
        ),
    )


async def initial(source, parent=PARENT):
    prepared = await source.prepare_cycle(None)
    assert prepared.mode == "full"
    assert not source.capture_cycle_configuration.scope_changes
    cycle = CaptureCycle(
        version=CycleVersion(cycle_id=uuid4(), revision=1),
        configuration=source.capture_cycle_configuration,
        mode=prepared.mode,
        source_plan=prepared.source_plan,
    )
    scope = source.child_scope(parent, "event") if parent else ROOT
    plan = await source.prepare_scope(scope, cycle, None, parent=parent, force_full=False)
    return scope, source.initial_scope_continuation(scope, cycle, plan)


@pytest.mark.asyncio
async def test_one_event_owner_across_full_and_expanded_pages_preserves_native_cancelled_json():
    reads = []

    def graph(request):
        path = request.url.path
        if path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        reads.append(path)
        assert request.headers["Prefer"] == 'IdType="ImmutableId"'
        if path.endswith("/events"):
            assert "startDateTime" not in request.url.params
            return httpx.Response(200, json={"value": [{"id": "single"}, {"id": "master"}]})
        if path.endswith("/calendarView"):
            assert request.url.params["startDateTime"] == WINDOW.start.isoformat()
            return httpx.Response(200, json={"value": [{"id": "single"}, {"id": "exception"}]})
        native_id = path.rsplit("/", 1)[1]
        event = {**EVENT, "id": native_id}
        if native_id == "master":
            event.update(type="seriesMaster", recurrence={"pattern": {"type": "weekly"}})
        if native_id == "exception":
            event.update(
                type="exception",
                seriesMasterId="master",
                isCancelled=True,
                originalStart="2026-10-01T10:00:00Z",
            )
        return httpx.Response(200, json=event)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await source_for(client)
        scope, cursor = await initial(source)
        first = await source.capture_page(scope, cursor, files=MagicMock(), parent=PARENT)
        second = await source.capture_page(
            scope, first.continuation, files=MagicMock(), parent=PARENT
        )
    assert not first.final and second.final
    assert first.records[0].identity == second.records[0].identity
    assert first.records[0].payload == EVENT
    assert second.records[1].kind == "upsert" and second.records[1].payload["isCancelled"] is True
    assert {r.parent for r in (*first.records, *second.records)} == {PARENT.identity}
    assert all(r.completeness == "partial" for r in (*first.records, *second.records))
    assert reads.count("/v1.0/me/calendars/calendar/events/single") == 2


@pytest.mark.asyncio
async def test_late_failure_retries_same_page_and_resumed_process_keeps_frozen_window():
    fail = True
    requests = []
    next_link = f"{BASE}/calendars/calendar/events?$skiptoken=second"

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        requests.append(str(request.url))
        if request.url.path.endswith("/events"):
            if "$skiptoken" not in request.url.params:
                return httpx.Response(
                    200, json={"value": [{"id": "first"}], "@odata.nextLink": next_link}
                )
            return httpx.Response(200, json={"value": [{"id": "second"}, {"id": "third"}]})
        if request.url.path.endswith("/third") and fail:
            return httpx.Response(503)
        return httpx.Response(200, json={**EVENT, "id": request.url.path.rsplit("/", 1)[1]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await source_for(client)
        scope, cursor = await initial(source)
        first = await source.capture_page(scope, cursor, files=MagicMock(), parent=PARENT)
        serialized = first.continuation.model_dump_json()
        with pytest.raises(SourceServerError):
            await source.capture_page(scope, first.continuation, files=MagicMock(), parent=PARENT)
        assert first.continuation.model_dump_json() == serialized
        fail = False
        resumed = await source_for(client)
        second = await resumed.capture_page(
            scope,
            ScanContinuation.model_validate_json(serialized),
            files=MagicMock(),
            parent=PARENT,
        )
    assert [r.identity.native_id for r in second.records] == ["second", "third"]
    assert second.continuation.value["phase"] == "calendarView"
    assert requests.count(next_link) == 2
    assert second.continuation.value["context"]["window"] == WINDOW.model_dump(mode="json")


@pytest.mark.asyncio
async def test_exact_omissions_keep_historical_occurrences_and_ambiguous404_fails():
    missing = False
    historical = {
        **EVENT,
        "id": "old",
        "type": "occurrence",
        "seriesMasterId": "master",
        "start": {"dateTime": "2020-01-01T00:00:00", "timeZone": "UTC"},
    }

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        return httpx.Response(404) if missing else httpx.Response(200, json=historical)

    record = retained(
        RecordIdentity(record_type="event", native_id="old", container_id="calendar"),
        historical,
        PARENT.identity,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await source_for(client)
        observation = await source.refresh_known(record, files=MagicMock())
        assert observation.kind == "upsert" and observation.payload == historical
        missing = True
        with pytest.raises(SourceEntityNotFoundError):
            await source.refresh_known(record, files=MagicMock())


@pytest.mark.asyncio
async def test_inventory_permission_metadata_and_foreign_link_rejected_before_hydration():
    foreign = False
    calls = []

    def graph(request):
        calls.append(request.url.path)
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if request.url.path.endswith("/calendars"):
            page = {"value": [{"id": "calendar"}]}
            if foreign:
                page["@odata.nextLink"] = f"{BASE}/calendars/another/events?$skiptoken=private"
            return httpx.Response(200, json=page)
        return httpx.Response(200, json=CALENDAR)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await source_for(client)
        scope, cursor = await initial(source, parent=None)
        page = await source.capture_page(scope, cursor, files=MagicMock())
        assert page.final and page.records[0].payload == CALENDAR
        assert "canViewPrivateItems" in page.records[0].descendant_visibility_fields
        foreign = True
        before = calls.count("/v1.0/me/calendars/calendar")
        with pytest.raises(OutlookBoundaryError, match="changed its native collection"):
            await source.capture_page(scope, cursor, files=MagicMock())
        assert calls.count("/v1.0/me/calendars/calendar") == before


@pytest.mark.asyncio
async def test_repeated_token_and_removed_full_listing_never_complete():
    removed = False
    next_link = f"{BASE}/calendars/calendar/events?$skiptoken=loop"

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if removed:
            return httpx.Response(
                200, json={"value": [{"id": "x", "@removed": {"reason": "deleted"}}]}
            )
        return httpx.Response(200, json={"value": [], "@odata.nextLink": next_link})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await source_for(client)
        scope, cursor = await initial(source)
        page = await source.capture_page(scope, cursor, files=MagicMock(), parent=PARENT)
        with pytest.raises(OutlookBoundaryError, match="repeated"):
            await source.capture_page(scope, page.continuation, files=MagicMock(), parent=PARENT)
        removed = True
        with pytest.raises(OutlookBoundaryError, match="Invalid Outlook calendar collection"):
            await source.capture_page(scope, cursor, files=MagicMock(), parent=PARENT)


@pytest.mark.asyncio
async def test_principal_and_scope_binding_reject_before_any_provider_read():
    calls = []

    def graph(request):
        calls.append(str(request.url))
        return httpx.Response(200, json={"id": "principal"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await source_for(client)
        scope, cursor = await initial(source)
        before = len(calls)
        wrong = cursor.model_dump(mode="json")
        wrong["value"]["context"]["calendar_id"] = "another"
        with pytest.raises(OutlookBoundaryError, match="another scope"):
            await source.capture_page(
                scope, ScanContinuation.model_validate(wrong), files=MagicMock(), parent=PARENT
            )
        source.graph.verified_principal_id = None
        with pytest.raises(OutlookBoundaryError, match="not attested"):
            await source.capture_page(scope, cursor, files=MagicMock(), parent=PARENT)
        assert len(calls) == before
