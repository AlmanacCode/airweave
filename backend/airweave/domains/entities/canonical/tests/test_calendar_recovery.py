"""Real Calendar source and SQL page lifecycle; only provider HTTP is synthetic."""

from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.calendar_query import CalendarRange, CalendarRangeService
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.entities.canonical.tests.test_mixed_scopes import next_job
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.platform.configs.config import GoogleCalendarConfig
from airweave.platform.sources.google_calendar import GoogleCalendarSource
from airweave.platform.sources.records.google_calendar import BASE

CONFIG = GoogleCalendarConfig(
    occurrence_window={"start": "2026-03-01T00:00:00Z", "end": "2026-04-01T00:00:00Z"}
)
MEMBERS = {"items": [{"id": "cal", "timeZone": "UTC"}]}


def event(native_id):
    return {
        "id": native_id,
        "start": {"dateTime": "2026-03-08T09:00:00Z"},
        "end": {"dateTime": "2026-03-08T10:00:00Z"},
    }


class NativeHTTP:
    def __init__(self, replies, principal="fixture-primary"):
        self.principal = principal
        self.replies, self.calls = list(replies), []

    async def handle(self, request):
        if request.url.path.endswith("/calendars/primary"):
            return httpx.Response(200, json={"id": self.principal})
        self.calls.append((request.url.path, dict(request.url.params)))
        path, value = self.replies.pop(0)
        assert str(request.url).split("?")[0] == BASE + path
        if isinstance(value, Exception):
            raise value
        status, payload = value if isinstance(value, tuple) else (200, value)
        return httpx.Response(status, json=payload)


async def setup(database, source, native, attempt=1, config=CONFIG):
    service, fence = source
    config = config.model_copy(update={"expected_primary_calendar_id": "fixture-primary"})
    client = httpx.AsyncClient(transport=httpx.MockTransport(native.handle))
    connector = await GoogleCalendarSource.create(
        auth=StaticTokenProvider("synthetic"), logger=MagicMock(), http_client=client, config=config
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
        files=MagicMock(),
    )
    await pipeline.start(ctx)
    return pipeline, ctx, runtime, client


async def no_limits():
    pass


async def run(pipeline, ctx, runtime):
    await pipeline.run_scans(ctx, runtime, no_limits)
    await pipeline.cleanup_orphaned_entities(ctx, runtime)
    await pipeline.save_checkpoint(ctx, runtime)


async def test_native_calendar_resume_delta_and_published_range(database, source):
    # Pre-page-engine tokens cannot authorize a delta without exact scope evidence.
    async with database() as db:
        await source[0].save_checkpoint(db, source[1], {"calendar_tokens": {"cal": "legacy"}})
    native = NativeHTTP(
        [
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("a")], "nextPageToken": "p2"}),
            ("/calendars/cal/events", ConnectionError("interrupted")),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("b")], "nextSyncToken": "full-token"}),
            ("/calendars/cal/events", {"items": [event("occurrence")]}),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [], "nextSyncToken": "delta-token"}),
            ("/calendars/cal/events", {"items": [event("occurrence")]}),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    with pytest.raises(ConnectionError):
        await run(pipeline, ctx, runtime)
    await client.aclose()
    pipeline, ctx, runtime, client = await setup(database, source, native, 2)
    await run(pipeline, ctx, runtime)
    assert native.calls[4][1]["pageToken"] == "p2"
    async with database() as db:
        result = await CalendarRangeService("secret").read(
            db,
            source[1].organization_id,
            source[1].sync_id,
            "cal",
            CalendarRange(start="2026-03-08T00:00:00Z", end="2026-03-09T00:00:00Z", timezone="UTC"),
        )
        assert result.records[0].identity.native_id == "occurrence"
        assert result.refresh_state == "last_completed"
    await client.aclose()
    source = await next_job(database, source)
    pipeline, ctx, runtime, client = await setup(database, source, native)
    await run(pipeline, ctx, runtime)
    assert native.calls[7][1]["syncToken"] == "full-token"
    async with database() as db:
        masters = (
            await db.scalars(
                select(Entity).where(
                    Entity.entity_definition_short_name == "event", Entity.deleted_at.is_(None)
                )
            )
        ).all()
        assert {row.native_id for row in masters} == {"a", "b"}
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        assert scan.execution_state["last_full"]["checkpoint"]["value"] == {
            "sync_token": "full-token"
        }
        assert scan.execution_state["published"]["checkpoint"]["value"] == {
            "sync_token": "delta-token"
        }
    assert not native.replies
    await client.aclose()


async def test_native_410_restarts_only_one_master_scope(database, source):
    native = NativeHTTP(
        [
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("kept")], "nextSyncToken": "old"}),
            ("/calendars/cal/events", {"items": []}),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("delta-only")], "nextPageToken": "delta2"}),
            ("/calendars/cal/events", (410, {"error": {"message": "Expired"}})),
            ("/calendars/cal/events", {"items": [event("kept")], "nextSyncToken": "fresh"}),
            ("/calendars/cal/events", {"items": []}),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    await run(pipeline, ctx, runtime)
    await client.aclose()
    source = await next_job(database, source)
    pipeline, ctx, runtime, client = await setup(database, source, native)
    await run(pipeline, ctx, runtime)
    assert native.calls[5][1]["syncToken"] == "old"
    assert "syncToken" not in native.calls[6][1] and "pageToken" not in native.calls[6][1]
    async with database() as db:
        stale = await db.scalar(select(Entity).where(Entity.native_id == "delta-only"))
        assert stale.deleted_at is not None and stale.removal_reason == "absent"
    await client.aclose()


async def test_config_change_selects_full_instead_of_rejecting_forever(database, source):
    native = NativeHTTP(
        [
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [], "nextSyncToken": "old"}),
            ("/calendars/cal/events", {"items": []}),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [], "nextSyncToken": "new"}),
            ("/calendars/cal/events", {"items": []}),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    await run(pipeline, ctx, runtime)
    await client.aclose()
    source = await next_job(database, source)
    changed = CONFIG.model_copy(update={"calendar_ids": ("cal",)})
    pipeline, ctx, runtime, client = await setup(database, source, native, config=changed)
    await run(pipeline, ctx, runtime)
    assert "syncToken" not in native.calls[4][1]
    await client.aclose()


@pytest.mark.parametrize("required", [False, True])
async def test_occurrence_loss_withdraws_then_restoration_reacquires_unchanged_master(
    database, source, required
):
    native = NativeHTTP(
        [
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("unchanged")], "nextSyncToken": "discard"}),
            ("/calendars/cal/events", (404, {})),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("unchanged")], "nextSyncToken": "restored"}),
            ("/calendars/cal/events", {"items": []}),
        ]
    )
    from airweave.domains.entities.canonical.page_source import RequiredScopeAccessLost

    config = CONFIG.model_copy(update={"calendar_ids": ("cal",)}) if required else CONFIG
    pipeline, ctx, runtime, client = await setup(database, source, native, config=config)
    if required:
        with pytest.raises(RequiredScopeAccessLost):
            await run(pipeline, ctx, runtime)
    else:
        await run(pipeline, ctx, runtime)
    async with database() as db:
        entity = await db.scalar(select(Entity).where(Entity.native_id == "unchanged"))
        record_id = entity.id
        result = await source[0].store.read(
            db, source[1].organization_id, source[1].sync_id, record_id
        )
        assert result.content_access == "unavailable" and result.payload == {}
    await client.aclose()
    if not required:
        source = await next_job(database, source)
    pipeline, ctx, runtime, client = await setup(
        database, source, native, 2 if required else 1, config
    )
    await run(pipeline, ctx, runtime)
    assert "syncToken" not in native.calls[4][1]
    async with database() as db:
        result = await source[0].store.read(
            db, source[1].organization_id, source[1].sync_id, record_id
        )
        assert result.content_access == "available" and result.payload == event("unchanged")
    await client.aclose()


async def test_missing_selection_withdrawal_survives_interruption_then_restores(database, source):
    native = NativeHTTP(
        [
            ("/users/me/calendarList", {"items": []}),
            ("/users/me/calendarList/cal", (404, {})),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("fresh")], "nextSyncToken": "new"}),
            ("/calendars/cal/events", {"items": []}),
        ]
    )
    config = CONFIG.model_copy(update={"calendar_ids": ("cal",)})
    pipeline, ctx, runtime, client = await setup(database, source, native, config=config)

    async def stop_after_committed_page():
        async with database() as db:
            scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "calendar"))
            if scan and scan.continuation.get("selection_unavailable"):
                raise InterruptedError("Synthetic postcommit interruption")

    with pytest.raises(InterruptedError):
        await pipeline.run_scans(ctx, runtime, stop_after_committed_page)
    async with database() as db:
        root = await db.scalar(select(Entity).where(Entity.native_id == "cal"))
        assert root.deleted_at is not None and root.removal_reason == "scope_removed"
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "calendar"))
        assert scan.phase == "collecting" and scan.continuation["selection_unavailable"]
    await client.aclose()
    pipeline, ctx, runtime, client = await setup(database, source, native, 2, config)
    await run(pipeline, ctx, runtime)
    assert "syncToken" not in native.calls[3][1]
    assert not native.replies
    await client.aclose()


async def test_failed_refresh_keeps_publication_and_new_writer_is_not_fresh(database, source):
    native = NativeHTTP(
        [
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [], "nextSyncToken": "old"}),
            ("/calendars/cal/events", {"items": [event("old-occurrence")]}),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [], "nextSyncToken": "new"}),
            ("/calendars/cal/events", {"items": [event("new-occurrence")], "nextPageToken": "p2"}),
            ("/calendars/cal/events", ConnectionError("interrupted")),
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": []}),
        ]
    )
    query = CalendarRange(start="2026-03-08T00:00:00Z", end="2026-03-09T00:00:00Z", timezone="UTC")
    pipeline, ctx, runtime, client = await setup(database, source, native)
    await run(pipeline, ctx, runtime)
    async with database() as db:
        scan = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "event_occurrence")
        )
        published = scan.execution_state["published"]
    await client.aclose()
    source = await next_job(database, source)
    async with database() as db:
        result = await CalendarRangeService("secret").read(
            db, source[1].organization_id, source[1].sync_id, "cal", query
        )
        assert result.refresh_state == "refresh_in_progress"
    pipeline, ctx, runtime, client = await setup(database, source, native)
    with pytest.raises(ConnectionError):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        scan = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "event_occurrence")
        )
        assert scan.execution_state["published"] == published
        result = await CalendarRangeService("secret").read(
            db, source[1].organization_id, source[1].sync_id, "cal", query
        )
        assert result.refresh_state == "refresh_in_progress"
        assert {item.identity.native_id for item in result.records} == {
            "old-occurrence",
            "new-occurrence",
        }
    await client.aclose()
    pipeline, ctx, runtime, client = await setup(database, source, native, 2)
    await run(pipeline, ctx, runtime)
    async with database() as db:
        result = await CalendarRangeService("secret").read(
            db, source[1].organization_id, source[1].sync_id, "cal", query
        )
        assert result.refresh_state == "last_completed"
        assert {item.identity.native_id for item in result.records} == {"new-occurrence"}
        scan = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "event_occurrence")
        )
        assert (
            scan.execution_state["published"]["observed_change_sequence"]
            > published["observed_change_sequence"]
        )
    await client.aclose()


async def test_membership_role_downgrade_hides_prior_event_details(database, source):
    config = CONFIG.model_copy(update={"calendar_ids": ("cal",)})
    native = NativeHTTP(
        [
            ("/users/me/calendarList", MEMBERS),
            ("/calendars/cal/events", {"items": [event("private-detail")], "nextSyncToken": "old"}),
            ("/calendars/cal/events", {"items": []}),
            (
                "/users/me/calendarList",
                {"items": [{"id": "cal", "timeZone": "UTC", "accessRole": "freeBusyReader"}]},
            ),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native, config=config)
    await run(pipeline, ctx, runtime)
    await client.aclose()
    source = await next_job(database, source)
    pipeline, ctx, runtime, client = await setup(database, source, native, config=config)
    with pytest.raises(ValueError, match="not accessible"):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        original = await db.scalar(select(Entity).where(Entity.native_id == "private-detail"))
        result = await source[0].store.read(
            db, source[1].organization_id, source[1].sync_id, original.id
        )
        assert result.content_access == "unavailable" and result.payload == {}
        parent = await db.scalar(select(Entity).where(Entity.native_id == "cal"))
        assert parent.removal_reason == "access_revoked"
    assert not native.replies and len(native.calls) == 4
    await client.aclose()


async def test_reader_role_change_hides_children_until_same_identity_recaptured(database, source):
    owner = {"items": [{"id": "cal", "timeZone": "UTC", "accessRole": "owner"}]}
    reader = {"items": [{"id": "cal", "timeZone": "UTC", "accessRole": "reader"}]}
    secret = {**event("private"), "visibility": "private", "description": "Formerly visible"}
    redacted = {**event("private"), "visibility": "private"}
    native = NativeHTTP(
        [
            ("/users/me/calendarList", owner),
            ("/calendars/cal/events", {"items": [secret], "nextSyncToken": "owner-token"}),
            ("/calendars/cal/events", {"items": []}),
            ("/users/me/calendarList", reader),
            ("/users/me/calendarList", reader),
            ("/calendars/cal/events", {"items": [redacted], "nextSyncToken": "reader-token"}),
            ("/calendars/cal/events", {"items": []}),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    await run(pipeline, ctx, runtime)
    async with database() as db:
        child = await db.scalar(select(Entity).where(Entity.native_id == "private"))
        child_id = child.id
        parent = await db.scalar(select(Entity).where(Entity.native_id == "cal"))
        old_epoch = parent.visibility_epoch
    await client.aclose()
    source = await next_job(database, source)
    pipeline, ctx, runtime, client = await setup(database, source, native)

    async def stop_after_role_commit():
        async with database() as db:
            parent = await db.scalar(select(Entity).where(Entity.native_id == "cal"))
            if parent.visibility_epoch != old_epoch:
                raise InterruptedError("Synthetic role-change commit boundary")

    with pytest.raises(InterruptedError):
        await pipeline.run_scans(ctx, runtime, stop_after_role_commit)
    async with database() as db:
        hidden = await source[0].store.read(
            db, source[1].organization_id, source[1].sync_id, child_id
        )
        assert hidden.content_access == "unavailable" and hidden.payload == {}
    await client.aclose()
    pipeline, ctx, runtime, client = await setup(database, source, native, 2)
    await run(pipeline, ctx, runtime)
    assert "syncToken" not in native.calls[5][1]
    async with database() as db:
        current = await source[0].store.read(
            db, source[1].organization_id, source[1].sync_id, child_id
        )
        assert current.content_access == "available" and current.payload == redacted
        parent = await db.scalar(select(Entity).where(Entity.native_id == "cal"))
        assert parent.visibility_epoch == old_epoch + 1
    await client.aclose()


async def test_wrong_primary_identity_never_writes_capture(database, source):
    from airweave.models.sync_cursor import SyncCursor

    native = NativeHTTP([], principal="wrong-primary")
    async with database() as db:
        before = [(row.id, row.cursor_data) for row in (await db.scalars(select(SyncCursor))).all()]
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handle)) as client:
        with pytest.raises(ValueError, match="does not match"):
            await GoogleCalendarSource.create(
                auth=StaticTokenProvider("same-broker-account"),
                logger=MagicMock(),
                http_client=client,
                config=GoogleCalendarConfig(expected_primary_calendar_id="trusted-primary"),
            )
    assert native.calls == []
    async with database() as db:
        assert (await db.scalars(select(Entity))).all() == []
        after = [(row.id, row.cursor_data) for row in (await db.scalars(select(SyncCursor))).all()]
        assert after == before
