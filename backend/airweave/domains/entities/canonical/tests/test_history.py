"""Historical originals use current authority, independently of current revision downloads."""

import hashlib
from uuid import uuid4

import pytest
from sqlalchemy import update

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.query import (
    BlobNotFound,
    BlobUnavailable,
    RecordNotFound,
    StaleRecordRevision,
)
from airweave.domains.entities.canonical.requests import BlobReference, CaptureBatch
from airweave.domains.entities.canonical.tests.conftest import migrate
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.domains.entities.canonical.tests.test_query import query_service
from airweave.models.entity_change import EntityChange
from airweave.models.source_connection import SourceConnection


async def test_historical_original_update_authority_and_integrity(database, source, tmp_path):
    capture, fence = source
    await bind_projection(database, fence)
    async with database() as db:
        connection = await db.connection()
        await connection.run_sync(migrate, "0017_entity_change_revision_lookup.py")
        await db.commit()
    service = query_service()
    storage = FilesystemBackend(tmp_path)
    body = b"historical synthetic original"
    digest = hashlib.sha256(body).hexdigest()
    ref = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(body),
        source_path="/attachment",
    )
    await storage.write_file(ref.key, body)
    async with database() as db:
        first = await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation(blobs=(ref,)),))
        )
    record_id = first.changes[0].record.id
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(observation(payload={"summary": "edited"}, content_hash="edited"),),
            ),
        )
    async with database() as db:
        selected = await service.read_revision(
            db, fence.organization_id, fence.sync_id, record_id, 1
        )
        assert selected.current_revision == 2
        assert selected.authority == "current_source_record_access"
        assert selected.record.blobs == (ref,)
        assert (
            await service.historical_blob(
                db, fence.organization_id, fence.sync_id, record_id, 1, digest, storage
            )
            == body
        )
        with pytest.raises(StaleRecordRevision):
            await service.blob(
                db, fence.organization_id, fence.sync_id, record_id, 1, digest, storage
            )
        with pytest.raises(BlobNotFound):
            await service.historical_blob(
                db, fence.organization_id, fence.sync_id, record_id, 2, digest, storage
            )
        for wrong_id, revision in ((uuid4(), 1), (record_id, 999)):
            with pytest.raises(RecordNotFound):
                await service.read_revision(
                    db, fence.organization_id, fence.sync_id, wrong_id, revision
                )
        with pytest.raises(RecordNotFound):
            await service.read_revision(db, uuid4(), fence.sync_id, record_id, 1)
    async with database() as db:
        from sqlalchemy import select

        change = await db.scalar(
            select(EntityChange).where(
                EntityChange.entity_record_id == record_id, EntityChange.record_revision == 1
            )
        )
        original = change.snapshot
        change.snapshot = {**original, "sync_id": str(uuid4())}
        await db.flush()
        with pytest.raises(RecordNotFound):
            await service.read_revision(db, fence.organization_id, fence.sync_id, record_id, 1)
        change.snapshot = original
        await db.flush()
    await storage.write_file(ref.key, b"corrupt")
    async with database() as db:
        with pytest.raises(BlobUnavailable):
            await service.historical_blob(
                db, fence.organization_id, fence.sync_id, record_id, 1, digest, storage
            )
    await storage.write_file(ref.key, body)
    original_read = storage.read_file

    async def revoke_after_read(path, *, max_bytes=None):
        result = await original_read(path, max_bytes=max_bytes)
        async with database() as other:
            await other.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await other.commit()
        return result

    storage.read_file = revoke_after_read
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await service.historical_blob(
                db, fence.organization_id, fence.sync_id, record_id, 1, digest, storage
            )
    storage.read_file = original_read
    async with database() as db:
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.sync_id == fence.sync_id)
            .values(is_authenticated=True)
        )
        await db.commit()
    async with database() as db:
        assert (
            await service.historical_blob(
                db, fence.organization_id, fence.sync_id, record_id, 1, digest, storage
            )
            == body
        )
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence, records=(observation(kind="delete", removal_reason="access_revoked"),)
            ),
        )
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await service.read_revision(db, fence.organization_id, fence.sync_id, record_id, 1)
