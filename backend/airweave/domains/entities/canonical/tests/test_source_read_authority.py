"""Real SQL/HTTP authority with retained bytes and deterministic transport races."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.query import RecordNotFound
from airweave.domains.entities.canonical.query_models import RecordListQuery
from airweave.domains.entities.canonical.requests import BlobReference
from airweave.domains.entities.canonical.store import SourceNotFound
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.domains.entities.canonical.tests.test_owned_search import indexed as indexed_fixture
from airweave.domains.entities.canonical.tests.test_query import query_service
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.search.canonical_visibility import visible_results
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.search.types import SearchResults
from airweave.models.entity import Entity
from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync

indexed = indexed_fixture


async def managed(database, fence, state="paused"):
    """A lifecycle row, not another permission ledger or provider credential."""
    async with database() as db:
        source_id = await db.scalar(
            select(SourceConnection.id).where(SourceConnection.sync_id == fence.sync_id)
        )
        db.add(
            OwnedProvisioning(
                organization_id=fence.organization_id,
                client_namespace="almanac",
                account_id=uuid4(),
                generation=2,
                observed_generation=2,
                request_hash="a" * 64,
                request_payload={},
                desired_state=state,
                source_connection_id=source_id,
                sync_id=fence.sync_id,
                cancellation_job_ids=[],
            )
        )
        await db.execute(update(Sync).where(Sync.id == fence.sync_id).values(status="paused"))
        await db.commit()


async def disconnect(database, fence, state="disconnected"):
    """Keep the stale auth flag true to prove desired disconnect wins independently."""
    async with database() as db:
        await db.execute(
            update(OwnedProvisioning)
            .where(OwnedProvisioning.sync_id == fence.sync_id)
            .values(desired_state=state)
        )
        await db.commit()


@pytest.mark.parametrize("withdrawal", ["unavailable", "disconnected"])
async def test_paused_reads_and_http_disconnect_redaction(database, source, tmp_path, withdrawal):
    capture_service, fence = source
    await bind_projection(database, fence)
    await managed(database, fence)
    raw = b"Retained private source canary"
    digest = hashlib.sha256(raw).hexdigest()
    blob = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(raw),
    )
    storage = FilesystemBackend(tmp_path)
    await storage.write_file(blob.key, raw)
    saved = await capture(database, capture_service, fence, observation(blobs=(blob,)))
    record = saved.changes[0].record
    service = query_service()
    async with database() as db:
        assert (await service.read(db, fence.organization_id, fence.sync_id, record.id)).payload
        assert (
            await service.blob(
                db, fence.organization_id, fence.sync_id, record.id, 1, digest, storage
            )
            == raw
        )
        assert (
            await service.list_records(db, fence.organization_id, fence.sync_id, RecordListQuery())
        ).records
    app = query_app(
        database, lambda: SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))
    )
    await disconnect(database, fence, withdrawal)
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        base = f"/sync/{fence.sync_id}/records"
        response = await client.get(f"{base}/{record.id}")
        assert response.status_code == 200
        assert response.json()["content_access"] == "unavailable"
        assert response.json()["payload"] == {} and response.json()["blobs"] == []
        assert (await client.get(base)).status_code == 404
        changes = (await client.get(base + "/changes")).json()["changes"]
        assert changes[0]["record"]["payload"] == {}
        assert changes[0]["record"]["blobs"] == []
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await service.blob(
                db, fence.organization_id, fence.sync_id, record.id, 1, digest, storage
            )
        # Denial is authority, not erasure or stale-index deletion.
        assert (await db.get(Entity, record.id)).source_payload == record.payload
        assert (
            await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id) == ()
        )
    assert await storage.read_file(blob.key) == raw


async def test_missing_source_and_native_withdrawal_deny_reads(database, source):
    capture_service, fence = source
    saved = await capture(database, capture_service, fence, observation())
    service = query_service()
    record_id = saved.changes[0].record.id
    async with database() as db:
        assert (
            await service.read(db, fence.organization_id, fence.sync_id, record_id)
        ).payload == {}
    await bind_projection(database, fence, "almanac")
    async with database() as db:
        assert (await service.read(db, fence.organization_id, fence.sync_id, record_id)).payload
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.sync_id == fence.sync_id)
            .values(is_authenticated=False)
        )
        await db.commit()
    async with database() as db:
        assert (
            await service.read(db, fence.organization_id, fence.sync_id, record_id)
        ).payload == {}
        with pytest.raises(SourceNotFound):
            await service.list_records(db, fence.organization_id, fence.sync_id, RecordListQuery())
        assert (await service.changes(db, fence.organization_id, fence.sync_id)).changes[
            0
        ].record.payload == {}


async def test_native_withdrawal_during_blob_io_denies_bytes(database, source, tmp_path):
    capture_service, fence = source
    await bind_projection(database, fence, "almanac")
    raw = b"Native bytes"
    digest = hashlib.sha256(raw).hexdigest()
    blob = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}", sha256=digest, size_bytes=len(raw)
    )
    saved = await capture(database, capture_service, fence, observation(blobs=(blob,)))
    storage = FilesystemBackend(tmp_path)
    await storage.write_file(blob.key, raw)
    original_read = storage.read_file

    async def revoke_after_read(*args, **kwargs):
        content = await original_read(*args, **kwargs)
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        return content

    storage.read_file = revoke_after_read
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await query_service().blob(
                db,
                fence.organization_id,
                fence.sync_id,
                saved.changes[0].record.id,
                1,
                digest,
                storage,
            )


async def test_managed_disconnect_before_reranking_and_after_reranking(database, indexed):
    fence, locator, connection = indexed
    await managed(database, fence)
    candidate = hit(fence, locator.encode())
    registry = Mock()
    registry.get.return_value = SimpleNamespace(
        source_class_ref=SimpleNamespace(canonical_record_types=("event", "message"))
    )
    executor = Mock()
    executor.prepare_query = AsyncMock(return_value=None)
    executor.execute = AsyncMock(return_value=SearchResults(results=[candidate]))
    service = OwnedSearchService(executor, registry)
    service._rank = AsyncMock(side_effect=service._rank)
    ctx = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))
    request = OwnedSearchRequest(query="canary", sync_ids=[fence.sync_id], mode="keyword")
    assert (await service.search(database, ctx, request)).items
    service._rank.reset_mock()

    async def revoke_during_retrieval(**kwargs):
        await disconnect(database, fence)
        return SearchResults(results=[candidate])

    executor.execute.side_effect = revoke_during_retrieval
    with pytest.raises(HTTPException):
        await service.search(database, ctx, request)
    service._rank.assert_not_awaited()
    async with database() as db:
        assert not await visible_results(
            db, fence.organization_id, connection.readable_collection_id, [candidate], registry
        )
        await db.execute(update(OwnedProvisioning).values(desired_state="paused"))
        await db.commit()
    executor.execute.side_effect = None

    async def revoke_during_ranking(query, candidates, hits, text):
        await disconnect(database, fence)
        return candidates, None

    service._rank.side_effect = revoke_during_ranking
    with pytest.raises(HTTPException):
        await service.search(database, ctx, request)
