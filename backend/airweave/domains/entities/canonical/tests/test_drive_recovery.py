"""Real canonical SQL/pipeline and Drive source; native HTTP is synthetic."""

from contextlib import aclosing
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob
from airweave.platform.configs.config import GoogleDriveConfig
from airweave.platform.sources.google_drive import GoogleDriveSource
from airweave.platform.sources.records.google_drive import BASE


class NativeHTTP:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    async def handle(self, request):
        if request.url.path.endswith("/about"):
            assert request.url.params["fields"] == "user(permissionId,me)"
            return httpx.Response(200, json={"user": {"permissionId": "principal-a", "me": True}})
        self.calls.append((request.url.path, dict(request.url.params)))
        path, value = self.replies.pop(0)
        assert str(request.url).split("?")[0] == BASE + path
        if isinstance(value, Exception):
            raise value
        status, payload = value if isinstance(value, tuple) else (200, value)
        return httpx.Response(status, json=payload)


def folder(native_id):
    return {"id": native_id, "mimeType": "application/vnd.google-apps.folder"}


async def setup(database, source, native, attempt=1):
    service, fence = source
    client = httpx.AsyncClient(transport=httpx.MockTransport(native.handle))
    connector = await GoogleDriveSource.create(
        auth=StaticTokenProvider("synthetic"),
        logger=MagicMock(),
        http_client=client,
        config=GoogleDriveConfig(expected_permission_id="principal-a"),
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


async def test_resume_pending_file_and_changes_then_lost_final_ack(database, source):
    native = NativeHTTP(
        [
            ("/changes/startPageToken", {"startPageToken": "before"}),
            ("/files", {"files": [{"id": "a"}, {"id": "b"}]}),
            ("/files/a", folder("a")),
            ("/files/b", ConnectionError("interrupted")),
            ("/files/b", folder("b")),
            (
                "/changes",
                {
                    "changes": [{"changeType": "file", "fileId": "a", "removed": True}],
                    "nextPageToken": "next",
                },
            ),
            ("/files/a", (404, {"error": {"message": "gone"}})),
            ("/changes", {"changes": [], "newStartPageToken": "after"}),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with aclosing(client):
        with pytest.raises(ConnectionError):
            await run(pipeline, ctx, runtime)
    async with database() as db:
        cycle = await source[0].read_cycle(db, pipeline._writer())
        assert cycle.promoted_checkpoint is None
        assert len((await db.scalars(select(Entity))).all()) == 1
    pipeline, ctx, runtime, client = await setup(database, source, native, 2)
    async with aclosing(client):
        await pipeline.run_scans(ctx, runtime, no_limits)
    # Crash after final durable page, before publication. No provider call on retry.
    pipeline, ctx, runtime, client = await setup(database, source, native, 3)
    async with aclosing(client):
        await run(pipeline, ctx, runtime)
    assert not native.replies
    assert sum(path.endswith("/files") for path, _ in native.calls) == 1
    assert sum(path.endswith("/startPageToken") for path, _ in native.calls) == 1
    async with database() as db:
        cycle = await source[0].read_cycle(db, pipeline._writer())
        assert cycle.promoted_checkpoint.checkpoint.value == {"page_token": "after"}
        records = {r.native_id: r for r in (await db.scalars(select(Entity))).all()}
        assert records["a"].removal_reason == "scope_removed"
        assert records["b"].deleted_at is None

    full_evidence = cycle.last_full_capture
    service, fence = source
    async with database() as db:
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
                "/changes",
                {
                    "changes": [{"changeType": "file", "fileId": "a", "removed": True}],
                    "newStartPageToken": "later",
                },
            ),
            ("/files/a", folder("a")),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, (service, next_fence), delta)
    async with aclosing(client):
        await run(pipeline, ctx, runtime)
    assert delta.calls[0][1]["pageToken"] == "after"
    assert not delta.replies
    async with database() as db:
        cycle = await service.read_cycle(db, pipeline._writer())
        assert cycle.mode == "changes" and cycle.last_full_capture == full_evidence
        assert cycle.promoted_checkpoint.checkpoint.value == {"page_token": "later"}
        assert all(row.deleted_at is None for row in (await db.scalars(select(Entity))).all())


async def test_drive_membership_restart_preserves_records_until_exact_validation(database, source):
    native = NativeHTTP(
        [
            ("/changes/startPageToken", {"startPageToken": "old"}),
            ("/files", {"files": [{"id": "a"}]}),
            ("/files/a", folder("a")),
            (
                "/changes",
                {"changes": [{"changeType": "drive", "driveId": "lost", "removed": True}]},
            ),
            ("/changes/startPageToken", {"startPageToken": "fresh"}),
            ("/files", {"files": []}),
            ("/changes", {"changes": [], "newStartPageToken": "after"}),
            ("/files/a", folder("a")),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with aclosing(client):
        await run(pipeline, ctx, runtime)
    assert not native.replies
    async with database() as db:
        row = await db.scalar(select(Entity))
        assert row.deleted_at is None
        cycle = await source[0].read_cycle(db, pipeline._writer())
        assert cycle.starting_checkpoint.value == {"page_token": "fresh"}


async def test_rejected_inventory_token_restarts_sweep_not_boundary(database, source):
    native = NativeHTTP(
        [
            ("/changes/startPageToken", {"startPageToken": "before"}),
            ("/files", {"files": [{"id": "a"}], "nextPageToken": "invalid"}),
            ("/files/a", folder("a")),
            (
                "/files",
                (
                    400,
                    {
                        "error": {
                            "errors": [
                                {
                                    "location": "pageToken",
                                    "locationType": "parameter",
                                    "reason": "invalid",
                                }
                            ]
                        }
                    },
                ),
            ),
            ("/files", {"files": [{"id": "b"}]}),
            ("/files/b", folder("b")),
            ("/changes", {"changes": [], "newStartPageToken": "after"}),
            ("/files/a", (404, {"error": {"message": "gone"}})),
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with aclosing(client):
        await run(pipeline, ctx, runtime)
    assert not native.replies
    assert sum(path.endswith("/startPageToken") for path, _ in native.calls) == 1
    async with database() as db:
        assert (
            await db.scalar(select(Entity).where(Entity.native_id == "a"))
        ).removal_reason == "scope_removed"


@pytest.mark.parametrize("failure", ["incomplete", "forbidden"])
async def test_provider_failure_cannot_promote_or_remove_prior_capture(database, source, failure):
    from airweave.domains.sources.exceptions import SourceEntityForbiddenError

    ending = (
        ("/files", {"incompleteSearch": True})
        if failure == "incomplete"
        else ("/files/b", (403, {"error": {"message": "forbidden"}}))
    )
    listing = (
        {"files": [{"id": "a"}], "nextPageToken": "second"}
        if failure == "incomplete"
        else {"files": [{"id": "a"}, {"id": "b"}]}
    )
    native = NativeHTTP(
        [
            ("/changes/startPageToken", {"startPageToken": "before"}),
            ("/files", listing),
            ("/files/a", folder("a")),
            ending,
        ]
    )
    pipeline, ctx, runtime, client = await setup(database, source, native)
    async with aclosing(client):
        with pytest.raises(ValueError if failure == "incomplete" else SourceEntityForbiddenError):
            await run(pipeline, ctx, runtime)
    async with database() as db:
        cycle = await source[0].read_cycle(db, pipeline._writer())
        assert cycle.promoted_checkpoint is None and cycle.phase == "active"
        assert (await db.scalar(select(Entity))).deleted_at is None
