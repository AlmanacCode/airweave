"""Actual Gmail transport/page source, lifecycle and PostgreSQL; native HTTP is synthetic."""

from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.entities.canonical.tests.test_changes_cycles import record
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob
from airweave.platform.configs.config import GmailConfig
from airweave.platform.sources.gmail import GmailSource
from airweave.platform.sources.tests.test_gmail_capture import BASE, message


class NativeHTTP:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def handle(self, request):
        self.calls.append((str(request.url).split("?")[0], dict(request.url.params)))
        expected, response = self.responses.pop(0)
        assert str(request.url).split("?")[0] == BASE + expected
        if isinstance(response, Exception):
            raise response
        status, payload = response if isinstance(response, tuple) else (200, response)
        return httpx.Response(status, json=payload)


async def setup(database, source, native, *, attempt=1, query=None):
    service, fence = source
    client = httpx.AsyncClient(transport=httpx.MockTransport(native.handle))
    connector = await GmailSource.create(
        auth=StaticTokenProvider("synthetic"),
        logger=MagicMock(),
        http_client=client,
        config=GmailConfig(
            gmail_query=query, included_labels=[], excluded_labels=[], excluded_categories=[]
        ),
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


async def test_full_resume_interleaved_history_known_omission_and_next_delta(database, source):
    service, fence = source
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(record("known"),)))
    native = NativeHTTP(
        [
            ("/profile", {"historyId": "100"}),
            ("/messages", {"messages": [{"id": "a"}], "nextPageToken": "second"}),
            ("/messages/a", message("a")),
            (
                "/history",
                {"historyId": "110", "history": [{"messagesAdded": [{"message": {"id": "c"}}]}]},
            ),
            ("/messages/c", message("c")),
            ("/messages", ConnectionError("synthetic interruption")),
            ("/messages", {"messages": [{"id": "b"}]}),
            ("/messages/b", message("b")),
            (
                "/history",
                {"historyId": "120", "history": [{"messagesDeleted": [{"message": {"id": "a"}}]}]},
            ),
            ("/messages/a", (404, {"error": {"message": "gone"}})),
            ("/messages/known", message("known")),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with client:
        with pytest.raises(ConnectionError, match="interruption"):
            await run(pipeline, ctx, runtime)
    async with database() as db:
        cursor = await db.scalar(select(SyncCursor))
        assert "canonical_checkpoint" not in cursor.cursor_data
        assert cursor.cursor_data["canonical_cycle"]["promoted_checkpoint"] is None
    pipeline, ctx, runtime, client = await setup(database, source, native, attempt=2)
    async with client:
        await run(pipeline, ctx, runtime)
    assert len([url for url, _ in native.calls if url.endswith("/profile")]) == 1
    assert [
        params["startHistoryId"] for url, params in native.calls if url.endswith("/history")
    ] == ["100", "110"]
    assert native.calls[6][1]["pageToken"] == "second"
    assert not native.responses
    async with database() as db:
        full = await service.read_cycle(db, pipeline._writer())
        assert full.promoted_checkpoint.checkpoint.value == {"history_id": "120"}
        assert {
            r.native_id for r in (await db.scalars(select(Entity))).all() if r.deleted_at is None
        } == {"b", "c", "known"}
        old = await db.get(SyncJob, fence.job_id)
        old.status = "completed"
        job_id = uuid4()
        db.add(
            SyncJob(
                id=job_id,
                sync_id=fence.sync_id,
                organization_id=fence.organization_id,
                status="running",
            )
        )
        await db.commit()
        next_fence = await service.activate_writer(
            db, fence.organization_id, fence.sync_id, job_id, attempt_id=uuid4(), attempt_number=1
        )
    delta = NativeHTTP(
        [
            (
                "/history",
                {"historyId": "130", "history": [{"labelsAdded": [{"message": {"id": "b"}}]}]},
            ),
            ("/messages/b", message("b", labelIds=["TRASH"])),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, (service, next_fence), delta)
    async with client:
        await run(pipeline, ctx, runtime)
    assert delta.calls[0][1]["startHistoryId"] == "120"
    assert not delta.responses
    async with database() as db:
        changed = await service.read_cycle(db, pipeline._writer())
        assert changed.mode == "changes" and changed.last_full_capture == full.last_full_capture
        assert changed.promoted_checkpoint.checkpoint.value == {"history_id": "130"}
        assert (
            await db.scalar(select(Entity).where(Entity.native_id == "known"))
        ).deleted_at is None
        assert (await db.scalar(select(Entity).where(Entity.native_id == "b"))).source_payload[
            "labelIds"
        ] == ["TRASH"]


async def test_filtered_mutating_view_omission_is_not_provider_deletion(database, source):
    service, fence = source
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(record("no-longer-listed"),)))
    native = NativeHTTP(
        [("/messages", {"messages": [{"id": "a"}]}), ("/messages/a", message("a", labelIds=[]))]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native, query="in:inbox")
    async with client:
        await run(pipeline, ctx, runtime)
    assert native.calls[0][1]["q"] == "in:inbox"
    async with database() as db:
        cycle = await service.read_cycle(db, pipeline._writer())
        removed = await db.scalar(select(Entity).where(Entity.native_id == "no-longer-listed"))
    assert cycle.promoted_checkpoint is None
    assert removed.removal_reason == "absent" and removed.deleted_at is not None


async def test_history_expiry_restarts_full_without_using_partial_sightings(database, source):
    native = NativeHTTP(
        [
            ("/profile", {"historyId": "old"}),
            ("/messages", {"messages": [{"id": "partial"}]}),
            ("/messages/partial", message("partial")),
            ("/history", (404, {"error": {"message": "expired"}})),
            ("/profile", {"historyId": "fresh"}),
            ("/messages", {}),
            ("/history", {"historyId": "terminal"}),
            ("/messages/partial", (404, {"error": {"message": "gone"}})),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with client:
        await run(pipeline, ctx, runtime)
    async with database() as db:
        cycle = await source[0].read_cycle(db, pipeline._writer())
        partial = await db.scalar(select(Entity).where(Entity.native_id == "partial"))
    assert cycle.starting_checkpoint.value == {"history_id": "fresh"}
    assert cycle.promoted_checkpoint.checkpoint.value == {"history_id": "terminal"}
    assert partial.deleted_at is not None and partial.removal_reason == "provider_deleted"


async def test_history_offset_resume_and_lost_final_ack_do_not_rehydrate_committed_messages(
    database, source
):
    raw = {
        "historyId": "20",
        "history": [{"id": "entry", "messages": [{"id": str(i)} for i in range(51)]}],
    }
    responses = [("/profile", {"historyId": "10"}), ("/messages", {}), ("/history", raw)]
    responses.extend((f"/messages/{i}", message(str(i))) for i in range(50))
    responses.extend(
        [
            ("/history", ConnectionError("synthetic partial history stop")),
            ("/history", raw),
            ("/messages/50", message("50")),
        ]
    )
    native = NativeHTTP(responses)
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with client:
        with pytest.raises(ConnectionError, match="partial history"):
            await run(pipeline, ctx, runtime)
    pipeline, ctx, runtime, client = await setup(database, source, native, attempt=2)
    async with client:
        await pipeline.run_scans(ctx, runtime, no_limits)
        # The process disappears after its durable final page, before final checkpoint publication.
    assert not native.responses
    calls = len(native.calls)
    pipeline, ctx, runtime, client = await setup(database, source, native, attempt=3)
    async with client:
        await run(pipeline, ctx, runtime)
    assert len(native.calls) == calls
    assert len([url for url, _ in native.calls if "/messages/" in url]) == 51
    async with database() as db:
        state = await source[0].read_cycle(db, pipeline._writer())
    assert state.promoted_checkpoint.checkpoint.value == {"history_id": "20"}
    assert state.phase == "complete"
