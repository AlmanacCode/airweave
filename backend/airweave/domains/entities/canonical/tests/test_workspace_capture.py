"""Actual Drive pipeline publishes Docs references atomically; HTTP is synthetic."""

import hashlib
from contextlib import aclosing
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.query import CanonicalQueryService, RecordNotFound
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.entities.canonical.tests.test_drive_recovery import run, setup
from airweave.domains.storage.file_service import FileService
from airweave.models.entity import Entity
from airweave.platform.sources.records.google_drive import file_record
from airweave.platform.sources.records.google_drive_pages import DrivePages


class NativeDocs:
    def __init__(self):
        self.fail = True

    async def handle(self, request):
        path = request.url.path
        if path.endswith("/about"):
            assert request.url.params["fields"] == "user(permissionId,me)"
            return httpx.Response(
                200, json={"user": {"permissionId": "principal-a", "me": True}}
            )
        if request.url.host == "docs.googleapis.com":
            if self.fail:
                raise ConnectionError("interrupted native acquisition")
            return httpx.Response(
                200,
                json={
                    "documentId": "doc",
                    "tabs": [
                        {
                            "tabProperties": {"tabId": "one"},
                            "documentTab": {"body": {"content": []}},
                        }
                    ],
                },
            )
        if path.endswith("/export"):
            return httpx.Response(200, content=b"retained-export")
        if path.endswith("/startPageToken"):
            return httpx.Response(200, json={"startPageToken": "start"})
        if path.endswith("/files"):
            return httpx.Response(200, json={"files": [{"id": "doc"}]})
        if path.endswith("/files/doc"):
            return httpx.Response(
                200,
                json={
                    "id": "doc",
                    "version": "7",
                    "mimeType": "application/vnd.google-apps.document",
                },
            )
        if path.endswith("/changes"):
            return httpx.Response(200, json={"changes": [], "newStartPageToken": "end"})
        raise AssertionError("Unexpected synthetic provider endpoint")


async def test_recapture_original_uuid_atomically_adds_manifest_and_withdraws_all_parts(
    database, source, tmp_path, monkeypatch
):
    service, fence = source
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir",
        lambda _: str(tmp_path / "temp"),
    )
    files = FileService(
        fence.job_id, FilesystemBackend(tmp_path / "storage"), sync_id=fence.sync_id
    )
    old_blob = await files.store_canonical_blob(
        b"retained-export",
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    old = file_record(
        {"id": "doc", "version": "7", "mimeType": "application/vnd.google-apps.document"}
    ).model_copy(
        update={"blobs": (old_blob,), "content_hash": old_blob.sha256, "completeness": "complete"}
    )
    previous = (await capture(database, service, fence, old)).changes[0].record
    native = NativeDocs()
    pipeline, ctx, runtime, client = await setup(database, source, native)
    pipeline.files = files
    async with aclosing(client):
        with pytest.raises(ConnectionError, match="interrupted"):
            await run(pipeline, ctx, runtime)
    async with database() as db:
        unchanged = await db.get(Entity, previous.id)
        assert unchanged.record_revision == previous.revision
        assert len(unchanged.blob_references) == 1
        cycle = await service.read_cycle(db, pipeline._writer())
        assert cycle.promoted_checkpoint is None
    native.fail = False
    pipeline, ctx, runtime, client = await setup(database, source, native, attempt=2)
    pipeline.files = files
    async with aclosing(client):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        completed = await service.read_cycle(db, pipeline._writer())
    # A pre-native completed Drive boundary cannot authorize a quiet delta that
    # would leave export-only Docs permanently without their native representation.
    previous_cycle = completed.model_copy(
        update={
            "configuration": completed.configuration.model_copy(
                update={"fingerprint": hashlib.sha256(b"drive-all-accessible-v1").hexdigest()}
            )
        }
    )
    boundary = AsyncMock(return_value={"startPageToken": "new-native-baseline"})
    plan = await DrivePages(boundary).prepare(previous_cycle, completed.configuration)
    assert plan.mode == "full"
    assert plan.starting_checkpoint.value == {"page_token": "new-native-baseline"}
    boundary.assert_awaited_once()
    query = CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-key")
    async with database() as db:
        current = await query.read(db, fence.organization_id, fence.sync_id, previous.id)
        assert current.id == previous.id and current.revision == previous.revision + 1
        assert len(current.blobs) == 3
        assert sum(blob.role == "representation_manifest" for blob in current.blobs) == 1
        for blob in current.blobs:
            assert await query.blob(
                db,
                fence.organization_id,
                fence.sync_id,
                current.id,
                current.revision,
                blob.sha256,
                files.storage,
            )
    await capture(
        database,
        service,
        pipeline._writer(),
        old.model_copy(
            update={
                "kind": "delete",
                "removal_reason": "scope_removed",
            }
        ),
    )
    async with database() as db:
        for blob in current.blobs:
            with pytest.raises(RecordNotFound):
                await query.blob(
                    db,
                    fence.organization_id,
                    fence.sync_id,
                    current.id,
                    current.revision,
                    blob.sha256,
                    files.storage,
                )
        assert len((await db.scalars(select(Entity))).all()) == 1
