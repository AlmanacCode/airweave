"""Actual child frontier commits/retry with synthetic Slack HTTP and original bytes."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.page_source import (
    InvalidScanContinuation,
    RequiredScopeAccessLost,
)
from airweave.domains.entities.canonical.requests import CaptureBatch, RecordIdentity
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackPrincipal, SlackSource

ROOT = {"channels": [{"id": "C1", "name": "Synthetic"}]}
MESSAGE = {
    "ts": "1",
    "text": "Original message",
    "files": [
        {
            "id": f"F{i}",
            "size": 3,
            "mimetype": "text/plain",
            "url_private": f"https://files.slack.com/F{i}",
        }
        for i in range(1, 4)
    ],
}


async def attempt(
    database, source, storage, calls, *, number, fail_last=False, responses=None, connector_out=None
):
    service, fence = source
    ctx, _, runtime, bus = components(database, source)
    connector = SlackSource(
        auth=StaticTokenProvider("fixture"), logger=MagicMock(), http_client=MagicMock()
    )
    connector.slack_config = SlackConfig(
        expected_team_id="T1", expected_user_id="U1", capture_files=True
    )
    connector._verified_principal = SlackPrincipal(ok=True, team_id="T1", user_id="U1")
    # A fresh message page attests its complete inventory in the same SQL commit.
    # A retry with no fresh owner page must still perform exact owner refresh.
    defaults = [ROOT, {"messages": [MESSAGE]}]
    connector._get = AsyncMock(side_effect=responses if responses is not None else defaults)
    if connector_out is not None:
        connector_out.append(connector)
    files = FileService(uuid4(), storage, sync_id=fence.sync_id)

    async def download(url, *args, **kwargs):
        identity = url.rsplit("/", 1)[-1]
        calls.append(identity)
        if fail_last and identity == "F3":
            raise ConnectionError("Synthetic last-file failure")
        return await files.store_canonical_blob(b"abc", media_type="text/plain")

    files.capture_canonical_url = download
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if number == 1 else uuid4(), number=number),
        connector.canonical_container_parents,
        page_source=connector,
        files=files,
    )
    runtime.source = connector
    runtime.canonical_capture = pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    try:
        await run(instance)
    finally:
        await files.cleanup_sync_directory(MagicMock())
    return pipeline, connector


async def test_last_file_retry_preserves_committed_children_and_inventory_hides_old_bytes(
    database, source, tmp_path
):
    service, fence = source
    storage = FilesystemBackend(tmp_path)
    calls = []
    with pytest.raises(ConnectionError, match="last-file"):
        await attempt(database, source, storage, calls, number=1, fail_last=True)
    assert calls == ["F1", "F2", "F3"]
    async with database() as db:
        rows = (await db.scalars(select(Entity).where(Entity.sync_id == fence.sync_id))).all()
        assert {row.native_id for row in rows} == {"C1", "1", "F1", "F2"}
        message = next(row for row in rows if row.native_id == "1")
        assert message.source_payload == MESSAGE and message.payload_schema_version == 2
    pipeline, connector = await attempt(database, source, storage, calls, number=2)
    assert calls == ["F1", "F2", "F3", "F3"]
    assert connector._get.await_count == 2
    async with database() as db:
        rows = (await db.scalars(select(Entity).where(Entity.sync_id == fence.sync_id))).all()
        children = [row for row in rows if row.entity_definition_short_name == "file"]
        assert len(children) == 3 and all(row.record_revision == 1 for row in children)
        current = connector._capture_message(
            {**MESSAGE, "files": [MESSAGE["files"][2]]}, "C1"
        ).model_copy(
            update={
                "payload_schema_version": 2,
                "descendant_visibility_fields": ("files",),
                "parent": RecordIdentity(record_type="channel", native_id="C1"),
                "completeness": "complete",
            }
        )
        await service.capture(db, CaptureBatch(fence=pipeline._writer(), records=(current,)))
    async with database() as db:
        for row in children:
            stored = await service.store.read(db, fence.organization_id, fence.sync_id, row.id)
            assert stored.content_access == "unavailable"


async def test_omitted_accessible_message_does_not_remove_retained_file_children(
    database, source, tmp_path
):
    from airweave.models.sync_job import SyncJob

    service, fence = source
    storage = FilesystemBackend(tmp_path)
    await attempt(database, source, storage, [], number=1)
    new_job = uuid4()
    async with database() as db:
        (await db.get(SyncJob, fence.job_id)).status = "completed"
        db.add(
            SyncJob(
                id=new_job,
                sync_id=fence.sync_id,
                organization_id=fence.organization_id,
                status="running",
            )
        )
        await db.commit()
    following = (service, fence.model_copy(update={"job_id": new_job, "attempt_id": uuid4()}))
    connectors = []
    with pytest.raises(ValueError, match="omitted an accessible prior message"):
        await attempt(
            database,
            following,
            storage,
            [],
            number=1,
            responses=[ROOT, {"messages": []}, {"messages": [MESSAGE]}],
            connector_out=connectors,
        )
    call = connectors[0]._get.call_args_list[-1]
    assert call.args[0].endswith("conversations.history")
    assert call.args[1] == {
        "channel": "C1",
        "oldest": "1",
        "latest": "1",
        "inclusive": "true",
        "limit": 1,
    }
    async with database() as db:
        rows = (await db.scalars(select(Entity).where(Entity.sync_id == fence.sync_id))).all()
        assert len(rows) == 5
        for row in rows:
            stored = await service.store.read(db, fence.organization_id, fence.sync_id, row.id)
            assert stored.deleted_at is None and stored.content_access == "available"
            if row.entity_definition_short_name == "file":
                assert stored.blobs


@pytest.mark.parametrize("repeat_owner", [False, True])
async def test_files_progress_before_history_finishes_and_resume_both_cursors(
    database, source, tmp_path, repeat_owner
):
    storage = FilesystemBackend(tmp_path)
    downloads = []
    histories = []
    owner_reads = []
    interrupt = True

    async def respond(url, params):
        if url.endswith("conversations.list"):
            return ROOT
        if "oldest" in params:
            owner_reads.append(params["oldest"])
            return {"messages": [MESSAGE]}
        cursor = params.get("cursor")
        histories.append(cursor)
        if cursor is None:
            return {"messages": [MESSAGE], "response_metadata": {"next_cursor": "H2"}}
        if cursor == "H2":
            if interrupt:
                raise asyncio.CancelledError()
            return {
                "messages": [MESSAGE] if repeat_owner else [],
                "response_metadata": {"next_cursor": "H3"},
            }
        assert cursor == "H3"
        return {"messages": []}

    with pytest.raises(asyncio.CancelledError):
        await attempt(database, source, storage, downloads, number=1, responses=respond)
    assert downloads == ["F1"]
    assert owner_reads == []
    async with database() as db:
        scans = (await db.scalars(select(CaptureScan))).all()
        history = next(row for row in scans if row.record_type == "message")
        files = next(row for row in scans if row.record_type == "file")
        assert history.phase == files.phase == "collecting"
        assert history.continuation["history_cursor"] == "H2"
        sweeps = (history.sweep_id, files.sweep_id)
        assert "canonical_checkpoint" not in (await db.scalar(select(SyncCursor))).cursor_data

    interrupt = False
    await attempt(database, source, storage, downloads, number=2, responses=respond)
    assert downloads == ["F1", "F2", "F3"]
    assert histories == [None, "H2", "H2", "H3"]
    assert owner_reads == ([] if repeat_owner else ["1"])
    async with database() as db:
        scans = (await db.scalars(select(CaptureScan))).all()
        history = next(row for row in scans if row.record_type == "message")
        files = next(row for row in scans if row.record_type == "file")
        assert (history.sweep_id, files.sweep_id) == sweeps
        assert history.phase == files.phase == "complete"
        rows = (await db.scalars(select(Entity))).all()
        assert next(row for row in rows if row.native_id == "1").source_payload == MESSAGE
        assert all(row.record_revision == 1 for row in rows if row.native_id.startswith("F"))
        cursor = (await db.scalar(select(SyncCursor))).cursor_data
        assert cursor["canonical_cycle"]["phase"] == "complete"


async def test_child_cursor_restart_bound_survives_interleaved_turns(
    database, source, tmp_path, monkeypatch
):
    original = SlackSource.capture_page
    file_calls = 0

    async def capture(connector, scope, continuation, **kwargs):
        nonlocal file_calls
        if scope.record_type == "file":
            file_calls += 1
            if file_calls != 2:
                raise InvalidScanContinuation("Synthetic child cursor expiry")
        return await original(connector, scope, continuation, **kwargs)

    monkeypatch.setattr(SlackSource, "capture_page", capture)

    async def respond(url, params):
        if url.endswith("conversations.list"):
            return ROOT
        if "oldest" in params:
            return {"messages": [MESSAGE]}
        return {
            "messages": [MESSAGE] if "cursor" not in params else [],
            "response_metadata": {"next_cursor": "H3" if "cursor" in params else "H2"},
        }

    downloads = []
    with pytest.raises(InvalidScanContinuation, match="child cursor"):
        await attempt(
            database, source, FilesystemBackend(tmp_path), downloads, number=1, responses=respond
        )
    assert file_calls == 3 and downloads == ["F1"]
    async with database() as db:
        cursor = (await db.scalar(select(SyncCursor))).cursor_data
        assert "canonical_checkpoint" not in cursor
        assert cursor["canonical_cycle"]["phase"] == "active"


async def test_required_child_access_loss_does_not_withdraw_enclosing_channel(
    database, source, tmp_path, monkeypatch
):
    original = SlackSource.capture_page

    async def capture(connector, scope, continuation, **kwargs):
        if scope.record_type == "file":
            # Exercise the shared driver contract; Slack does not currently emit this subtype.
            raise RequiredScopeAccessLost("Synthetic required child access loss")
        return await original(connector, scope, continuation, **kwargs)

    monkeypatch.setattr(SlackSource, "capture_page", capture)
    with pytest.raises(RequiredScopeAccessLost, match="required child"):
        await attempt(database, source, FilesystemBackend(tmp_path), [], number=1)
    service, fence = source
    async with database() as db:
        rows = (await db.scalars(select(Entity))).all()
        assert {row.native_id for row in rows} == {"C1", "1"}
        for row in rows:
            stored = await service.store.read(db, fence.organization_id, fence.sync_id, row.id)
            expected = "available" if row.native_id == "C1" else "unavailable"
            assert stored.content_access == expected
        assert "canonical_checkpoint" not in (await db.scalar(select(SyncCursor))).cursor_data


async def test_fresh_empty_inventory_reconciles_old_files_without_exact_owner_call(
    database, source, tmp_path
):
    from airweave.models.sync_job import SyncJob

    service, fence = source
    storage = FilesystemBackend(tmp_path)
    downloads = []
    await attempt(database, source, storage, downloads, number=1)
    assert downloads == ["F1", "F2", "F3"]
    job = uuid4()
    async with database() as db:
        (await db.get(SyncJob, fence.job_id)).status = "completed"
        db.add(
            SyncJob(
                id=job,
                sync_id=fence.sync_id,
                organization_id=fence.organization_id,
                status="running",
            )
        )
        await db.commit()
    following = (service, fence.model_copy(update={"job_id": job, "attempt_id": uuid4()}))
    pipeline, connector = await attempt(
        database,
        following,
        storage,
        downloads,
        number=1,
        responses=[ROOT, {"messages": [{**MESSAGE, "files": []}]}],
    )
    assert connector._get.await_count == 2
    assert downloads == ["F1", "F2", "F3"]
    async with database() as db:
        children = (
            await db.scalars(select(Entity).where(Entity.entity_definition_short_name == "file"))
        ).all()
        assert len(children) == 3 and all(child.deleted_at is not None for child in children)
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "file"))
        assert scan.phase == "complete"
        assert scan.parent_verified_attempt_id == pipeline._writer().attempt_id
