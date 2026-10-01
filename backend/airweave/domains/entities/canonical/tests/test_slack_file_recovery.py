"""Actual child frontier commits/retry with synthetic Slack HTTP and original bytes."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.requests import CaptureBatch, RecordIdentity
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.entity import Entity
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


async def attempt(database, source, storage, calls, *, number, fail_last=False):
    service, fence = source
    ctx, _, runtime, bus = components(database, source)
    connector = SlackSource(
        auth=StaticTokenProvider("fixture"), logger=MagicMock(), http_client=MagicMock()
    )
    connector.slack_config = SlackConfig(
        expected_team_id="T1", expected_user_id="U1", capture_files=True
    )
    connector._verified_principal = SlackPrincipal(ok=True, team_id="T1", user_id="U1")
    connector._get = AsyncMock(side_effect=[ROOT, {"messages": [MESSAGE]}])
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
