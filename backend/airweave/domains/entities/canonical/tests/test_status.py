"""Real SQL status distinguishes retained progress from connection and extraction."""

from types import SimpleNamespace
from uuid import uuid4

from httpx import ASGITransport, AsyncClient
from sqlalchemy import update

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import (
    ProjectionDocument,
    ProjectionLocator,
    scope_projection_document_id,
)
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


async def test_status_current_exclusions_gaps_and_pipeline_invalidation(database, source):
    service, fence = source
    binding = await bind_projection(database, fence, "wispr")
    await capture(
        database,
        service,
        fence,
        observation("partial"),
        observation("unavailable"),
        observation("failed"),
        observation("metadata-only"),
        observation(identity=RecordIdentity(record_type="meeting_listing", native_id="excluded")),
    )
    store = CanonicalProjectionStore()
    async with database() as db:
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
    for work in pending:
        name = work.record.identity.native_id
        if name == "failed":
            continue
        parts = (
            []
            if name == "excluded"
            else [
                {
                    "part_index": 0,
                    "key": "body",
                    "kind": "body",
                    "outcome": "indexed" if name == "partial" else "unsupported",
                    "reason": None if name == "partial" else "ocr_unavailable",
                    "gaps": ["ocr_unavailable"] if name == "partial" else [],
                },
                {"part_index": 1, "key": "metadata", "kind": "metadata", "outcome": "indexed"},
            ]
        )
        if name == "metadata-only":
            parts = [{"part_index": 0, "key": "metadata", "kind": "metadata", "outcome": "indexed"}]
        coverage = ExtractionCoverage.model_validate({"parts": parts})
        generation = uuid4()
        docs = []
        for part in coverage.parts:
            if part.outcome != "indexed":
                continue
            locator = ProjectionLocator(
                record_id=work.record.id,
                revision=1,
                pipeline_version=1,
                generation=generation,
                part_index=part.part_index,
            ).encode()
            docs.append(
                ProjectionDocument(
                    schema_name="base_entity",
                    document_id=scope_projection_document_id(
                        fence.sync_id, binding.collection_id, f"Entity_{locator}__chunk_0"
                    ),
                )
            )
        async with database() as db:
            assert await store.prepare(
                db, work, generation, binding.collection_id, tuple(docs), coverage=coverage
            )
            assert await store.publish(db, work, generation, len(docs))
    async with database() as db:
        await db.execute(
            update(Entity)
            .where(Entity.native_id == "failed")
            .values(projection_error="safe_failure")
        )
        await db.commit()
    async with database() as db:
        await service.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=CycleConfiguration(
                    fingerprint="a" * 64,
                    root_record_type="event",
                    child_record_types=(),
                ),
            ),
        )
    owner = fence.organization_id

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    async with AsyncClient(
        transport=ASGITransport(app=query_app(database, context)), base_url="http://test"
    ) as client:
        path = f"/sync/{fence.sync_id}/status"
        result = (await client.get(path)).json()
        assert result["retained_records"] == 5
        assert result["preparation"] == {
            "candidate_records": 4,
            "current_records": 3,
            "pending_records": 1,
            "failed_records": 1,
            "excluded_records": 1,
        }
        assert result["extraction"] == {
            "partial_records": 1,
            "unavailable_records": 1,
            "unknown_records": 1,
        }
        assert result["capture"]["phase"] == "active"
        assert result["capture"]["discovery"] == "pending"
        async with database() as db:
            await db.execute(
                update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=2)
            )
            await db.commit()
        stale = (await client.get(path)).json()
        assert stale["preparation"] == {
            "candidate_records": 5,
            "current_records": 0,
            "pending_records": 5,
            "failed_records": 1,
            "excluded_records": 0,
        }
        owner = uuid4()
        assert (await client.get(path)).status_code == 404
        owner = fence.organization_id
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.id == binding.source_connection_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        assert (await client.get(path)).status_code == 404


async def test_status_ancestor_withdrawal_hides_child_and_empty_source_is_known(database, source):
    service, fence = source
    await bind_projection(database, fence)
    parent_id = RecordIdentity(record_type="calendar", native_id="parent")
    parent = observation(identity=parent_id)
    await capture(
        database, service, fence, parent, observation("child", "parent", parent=parent_id)
    )
    await capture(
        database,
        service,
        fence,
        parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))

    async with AsyncClient(
        transport=ASGITransport(app=query_app(database, context)), base_url="http://test"
    ) as client:
        response = await client.get(f"/sync/{fence.sync_id}/status")
        assert response.status_code == 200
        assert response.json()["retained_records"] == 0
        assert response.json()["preparation"]["candidate_records"] == 0
