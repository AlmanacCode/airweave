"""Real SQL authority and actual HTTP/executor with fake search/embedding transport."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, select, update

from airweave.api import deps
from airweave.api.v1.endpoints.records import router
from airweave.db.session import get_db
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.entities.canonical.content_models import ContentProvenance, MatchedPart
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.tests.helpers import capture, observation, publish_prepared
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.search.adapters.vector_db.fakes.vector_db import FakeVectorDB
from airweave.domains.search.executor import SearchPlanExecutor
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.types import SearchResults
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


@pytest.fixture
async def indexed(database, source):
    service, fence = source
    await capture(database, service, fence, observation())
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="fake", embedding_dimensions=3, sparse_embedder="fake"
        )
        db.add(deployment)
        await db.flush()
        collection = Collection(
            name="Test",
            readable_id="owned-search",
            organization_id=fence.organization_id,
            vector_db_deployment_metadata_id=deployment.id,
        )
        db.add(collection)
        await db.flush()
        connection = SourceConnection(
            name="Test",
            short_name="gmail",
            organization_id=fence.organization_id,
            readable_collection_id=collection.readable_id,
            sync_id=fence.sync_id,
            is_authenticated=True,
        )
        db.add(connection)
        await db.commit()
    async with database() as db:
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=2)
        )
        await db.commit()
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation = uuid4()
    async with database() as db:
        assert await publish_prepared(store, db, work, generation, 1, collection.id)
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=1,
        pipeline_version=2,
        generation=generation,
        part_index=0,
    )
    return fence, locator, connection


@pytest.fixture
async def http_search(database, indexed):
    fence, locator, connection = indexed
    registry = SimpleNamespace(
        get=lambda _: SimpleNamespace(
            source_class_ref=SimpleNamespace(canonical_record_types=("event", "message"))
        )
    )
    vector = FakeVectorDB()
    dense = AsyncMock()
    dense.embed_many.side_effect = AssertionError("Keyword search must not call dense embedding")
    # Any accidental federation discovery fails instead of quietly returning empty.
    providers = MagicMock()
    providers.get_all_by_collection = AsyncMock(side_effect=AssertionError("No provider discovery"))
    executor = SearchPlanExecutor(
        dense_embedder=dense,
        sparse_embedder=FakeSparseEmbedder(),
        vector_db=vector,
        sc_repo=providers,
        source_registry=registry,
        source_lifecycle=MagicMock(),
        access_broker=MagicMock(),
    )
    executor._discover_federated_sources = AsyncMock(side_effect=AssertionError("No federation"))
    service = OwnedSearchService(executor, registry)
    ctx = SimpleNamespace(
        organization=SimpleNamespace(id=fence.organization_id), is_api_key_auth=True
    )
    app = FastAPI()
    app.include_router(router, prefix="/sync")

    async def session():
        async with database() as db:
            yield db

    from airweave.domains.entities.canonical.query import CanonicalQueryService
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.store import CanonicalRecordStore

    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "test-extraction-key"
    )
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_context] = lambda: ctx
    app.dependency_overrides[deps.get_owned_search_context] = lambda: ctx
    app.dependency_overrides[deps.get_search_session_factory] = lambda: database
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(owned_search=service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client, vector, ctx, executor, dense


async def test_owned_http_deduplicates_and_preserves_partial_engine(database, indexed, http_search):
    fence, locator, connection = indexed
    client, vector, _, executor, dense = http_search
    first = hit(fence, locator.encode())
    second = first.model_copy(
        update={"entity_id": "second-chunk", "textual_representation": "Other evidence"}
    )
    vector.seed_results(
        SearchResults(results=[first, second], engine_partial=True, engine_coverage_percent=75)
    )
    transferred_columns = set()

    def columns_received(connection, cursor, statement, parameters, context, executemany):
        if cursor.description:
            transferred_columns.update(column[0] for column in cursor.description)

    async with database() as db:
        engine = db.bind.sync_engine
    event.listen(engine, "after_cursor_execute", columns_received)
    try:
        response = await client.post(
            "/sync/search",
            json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
        )
    finally:
        event.remove(engine, "after_cursor_execute", columns_received)
    assert not transferred_columns.intersection({"source_payload", "blob_references"})
    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["identity"] == {"record_type": "event", "native_id": "one", "container_id": None}
    assert item["source_connection_id"] == str(connection.id)
    assert item["record_id"] == str(locator.record_id)
    assert item["revision"] == locator.revision and item["sync_id"] == str(fence.sync_id)
    assert item["provider"] == "gmail" and item["title"] == first.name
    assert item["completeness"] == "complete" and item["observed_at"]
    assert item["source_created_at"] is None and item["source_updated_at"] is None
    assert item["email_thread_id"] is None
    assert item["excerpts"] == []  # Old generations lack content provenance.
    assert "payload" not in item
    assert body["engine_partial"] and body["retrieval_incomplete"]
    assert body["coverage"] == "bounded_candidates" and "next_cursor" not in body
    assert body["sources"][0]["pending_records"] == 0
    executor._discover_federated_sources.assert_not_called()
    dense.embed_many.assert_not_called()
    compiled = vector._calls[0][1]
    assert compiled.filter_groups[0].conditions[0].value == [str(fence.sync_id)]


async def test_owned_http_rejects_foreign_unavailable_and_unknown_controls(
    database, indexed, http_search
):
    fence, _, _ = indexed
    client, vector, ctx, _, _ = http_search
    base = {"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"}
    for invalid in (
        {"sort": "date"},
        {"offset": 10},
        {"limit": 201},
        {"filter": []},
        {"cursor": "next"},
    ):
        assert (await client.post("/sync/search", json={**base, **invalid})).status_code == 422
    assert (
        await client.post("/sync/search", json={**base, "record_types": ["unknown"]})
    ).status_code == 422
    assert (
        await client.post("/sync/search", json={**base, "sync_ids": [str(uuid4())]})
    ).status_code == 404
    ctx.organization.id = uuid4()
    assert (await client.post("/sync/search", json=base)).status_code == 404
    ctx.organization.id = fence.organization_id
    async with database() as db:
        await db.execute(update(SourceConnection).values(is_authenticated=False))
        await db.commit()
    assert (await client.post("/sync/search", json=base)).status_code == 404
    assert vector._calls == []


async def test_owned_http_stale_text_removed_and_lag_explicit(
    database, source, indexed, http_search
):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    service, _ = source
    await capture(database, service, fence, observation(payload={"new": "private"}))
    response = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
    )
    body = response.json()
    assert response.status_code == 200 and body["items"] == []
    assert body["excluded_candidates"] == 1 and body["sources"][0]["pending_records"] == 1
    assert body["retrieval_incomplete"]


async def test_owned_filters_are_explicit_postfilters(indexed, http_search):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    response = await client.post(
        "/sync/search",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "mode": "keyword",
            "record_types": ["message"],
        },
    )
    assert response.status_code == 200
    assert response.json()["items"] == [] and response.json()["postfilter_excluded"] == 1


async def test_owned_search_rejects_valid_other_account_hit(database, source, indexed, http_search):
    from airweave.models import Sync, SyncJob

    fence, _, _ = indexed
    client, vector, _, _, _ = http_search
    capture_service, _ = source
    other_sync, other_job = uuid4(), uuid4()
    async with database() as db:
        db.add(Sync(id=other_sync, organization_id=fence.organization_id, name="Other"))
        await db.flush()
        db.add(
            SyncJob(
                id=other_job,
                organization_id=fence.organization_id,
                sync_id=other_sync,
                status="running",
            )
        )
        db.add(
            SourceConnection(
                name="Other",
                short_name="gmail",
                organization_id=fence.organization_id,
                readable_collection_id="owned-search",
                sync_id=other_sync,
                is_authenticated=True,
            )
        )
        await db.commit()
        other_fence = await capture_service.activate_writer(
            db, fence.organization_id, other_sync, other_job, attempt_id=uuid4(), attempt_number=1
        )
    await capture(database, capture_service, other_fence, observation())
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, other_sync))[0]
    generation = uuid4()
    async with database() as db:
        assert await publish_prepared(store, db, work, generation, 1)
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=1,
        pipeline_version=1,
        generation=generation,
        part_index=0,
    )
    # Fake engine deliberately violates its compiled sync filter. SQL enrichment
    # must still prevent this otherwise-valid same-collection account leak.
    vector.seed_results(SearchResults(results=[hit(other_fence, locator.encode())]))
    response = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
    )
    assert response.status_code == 200
    assert response.json()["items"] == []
    assert response.json()["excluded_candidates"] == 1


async def test_disconnect_during_retrieval_rejects_whole_response(database, indexed, http_search):
    fence, locator, _ = indexed
    client, vector, _, executor, _ = http_search
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    execute = executor.execute

    async def disconnected(**kwargs):
        result = await execute(**kwargs)
        async with database() as db:
            await db.execute(update(SourceConnection).values(is_authenticated=False))
            await db.commit()
        return result

    executor.execute = disconnected
    response = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
    )
    assert response.status_code == 404
    assert "Private original text" not in response.text


async def test_final_gate_drops_early_hit_changed_during_later_collection(
    database, source, indexed, http_search
):
    from airweave.models import Sync, SyncJob

    fence, locator, _ = indexed
    client, vector, _, executor, _ = http_search
    capture_service, _ = source
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    other_sync, other_job = uuid4(), uuid4()
    async with database() as db:
        deployment = await db.scalar(select(VectorDbDeploymentMetadata))
        db.add(
            Collection(
                name="Other",
                readable_id="later-search",
                organization_id=fence.organization_id,
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        db.add(Sync(id=other_sync, organization_id=fence.organization_id, name="Other"))
        await db.flush()
        db.add(
            SyncJob(
                id=other_job,
                organization_id=fence.organization_id,
                sync_id=other_sync,
                status="running",
            )
        )
        db.add(
            SourceConnection(
                name="Other",
                short_name="gmail",
                organization_id=fence.organization_id,
                readable_collection_id="later-search",
                sync_id=other_sync,
                is_authenticated=True,
            )
        )
        await db.commit()
    execute = executor.execute
    calls = 0
    prepared_queries = []

    async def later_edit(**kwargs):
        nonlocal calls
        calls += 1
        prepared_queries.append(kwargs["prepared_query"])
        if calls == 2:
            await capture(database, capture_service, fence, observation(payload={"updated": True}))
            return SearchResults(results=[])
        return await execute(**kwargs)

    executor.execute = later_edit
    # Pin collection order only for this race proof, independently of SQL planner order.
    service = next(
        value
        for key, value in client._transport.app.dependency_overrides.items()
        if key is deps.get_container
    )().owned_search
    resolve = service._resolve_scopes

    async def ordered(*args):
        scopes, groups = await resolve(*args)
        return scopes, dict(sorted(groups.items(), key=lambda item: item[0][1] != "owned-search"))

    service._resolve_scopes = ordered
    response = await client.post(
        "/sync/search",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id), str(other_sync)],
            "mode": "keyword",
        },
    )
    assert prepared_queries[0] is not None and prepared_queries[0] is prepared_queries[1]
    assert calls == 2 and response.status_code == 200
    assert response.json()["items"] == [] and response.json()["excluded_candidates"] == 1
    assert "Private original text" not in response.text


@pytest.mark.parametrize(
    "provider,payload,expected",
    [
        ("gmail", {"threadId": "thread-1"}, "thread-1"),
        ("gmail", {"threadId": "thread-1", "version": {"kind": "provider-value"}}, "thread-1"),
        ("slack", {"threadId": "thread-1"}, None),
        ("gmail", {}, None),
        ("gmail", {"threadId": None}, None),
        ("gmail", {"threadId": 12345}, None),
        ("gmail", {"threadId": "bad/id"}, None),
        ("gmail", {"threadId": {"bad": "value"}}, None),
    ],
)
async def test_email_route_uses_visible_canonical_provider_payload(
    database, indexed, http_search, provider, payload, expected
):
    from airweave.models.entity import Entity

    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    async with database() as db:
        await db.execute(update(SourceConnection).values(short_name=provider))
        await db.execute(
            update(Entity).values(entity_definition_short_name="message", source_payload=payload)
        )
        await db.commit()
    candidate = hit(fence, locator.encode())
    candidate.airweave_system_metadata.source_name = provider
    vector.seed_results(SearchResults(results=[candidate]))
    response = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["items"][0]["email_thread_id"] == expected


async def test_filtered_search_requires_explicit_reprojection(database, indexed, http_search):
    fence, _, _ = indexed
    client, vector, _, _, _ = http_search
    async with database() as db:
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=1)
        )
        await db.commit()
    result = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "record_types": ["event"]},
    )
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "reindex_required"
    assert vector._calls == []


async def test_dates_and_types_prefilter_before_candidate_limit(indexed, http_search):
    fence, _, _ = indexed
    client, vector, _, _, _ = http_search
    response = await client.post(
        "/sync/search",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "mode": "keyword",
            "record_types": ["event"],
            "created_after": "2026-01-01T00:00:00.000001Z",
            "updated_before": "2026-02-01T00:00:00Z",
        },
    )
    assert response.status_code == 200, response.text
    conditions = vector._calls[0][1].filter_groups[0].conditions
    by_field = {condition.field.value: condition.value for condition in conditions}
    assert by_field["airweave_system_metadata.canonical_record_type"] == ["event"]
    assert by_field["airweave_system_metadata.source_created_known"] == 1
    assert by_field["airweave_system_metadata.source_created_us"] == 1767225600000001
    assert by_field["airweave_system_metadata.source_updated_known"] == 1


async def test_capture_discovery_coverage_is_separate_from_retrieval(
    database, source, indexed, http_search
):
    from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration

    service, fence = source
    async with database() as db:
        await service.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=CycleConfiguration(
                    fingerprint="d" * 64,
                    parents={"event": (None,)},
                    completion_policies={"event": "discovery_only"},
                ),
            ),
        )
    client, vector, _, _, _ = http_search
    vector.seed_results(SearchResults(results=[]))
    response = await client.post(
        "/sync/search",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "mode": "keyword",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["sources"][0]["capture"] == {
        "phase": "active",
        "mode": "full",
        "scope_summary": None,
        "last_full_capture": None,
        "provider_checkpoint_promoted_at": None,
        "policies": {"event": "discovery_only"},
        "discovery": "incomplete",
    }
    assert body["sources"][0]["extraction_unknown_records"] == 1
    assert body["retrieval_incomplete"]


@pytest.mark.parametrize(
    "outcome,reason", [("unsupported", "unsupported_format"), ("failed", "conversion_failed")]
)
async def test_partial_extraction_http_hit_original_and_nonindexed_part_gate(
    database, indexed, http_search, outcome, reason
):
    from airweave.domains.entities.canonical.extraction_models import (
        ExtractionCoverage,
        ExtractionOutcome,
    )
    from airweave.models.projection_generation import ProjectionGeneration

    fence, locator, _ = indexed
    coverage = ExtractionCoverage(
        parts=(
            ExtractionOutcome(part_index=0, key="/body", kind="body", outcome="indexed"),
            ExtractionOutcome(
                part_index=1,
                key="/payload/parts/1",
                kind="file",
                media_type="video/mp4",
                extension=".mp4",
                outcome=outcome,
                reason=reason,
            ),
        )
    )
    async with database() as db:
        await db.execute(
            update(ProjectionGeneration)
            .where(ProjectionGeneration.id == locator.generation)
            .values(extraction_coverage=coverage.persisted())
        )
        await db.commit()
    client, vector, _, _, _ = http_search
    valid = hit(fence, locator.encode())
    part = MatchedPart(part_index=0, key="/body", kind="body", title=valid.name)
    valid.raw_source_fields["content_provenance"] = ContentProvenance(
        part=part, content_start=10, content_end=40, preview="# Metadata is literal user text"
    ).model_dump(mode="json")
    invalid = hit(fence, locator.model_copy(update={"part_index": 1}).encode())
    invalid.textual_representation = "unsupported attachment must never escape"
    invalid.raw_source_fields["content_provenance"] = ContentProvenance(
        part=MatchedPart(part_index=1, key="/payload/parts/1", kind="file", title=invalid.name),
        content_start=0,
        content_end=40,
        preview="failed or unsupported part must never escape",
    ).model_dump(mode="json")
    vector.seed_results(SearchResults(results=[valid, invalid]))
    response = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert len(payload["items"]) == 1 and payload["retrieval_incomplete"]
    assert payload["items"][0]["extraction"] == coverage.model_dump(mode="json")
    assert payload["items"][0]["matched_part"] == part.model_dump(mode="json")
    assert payload["items"][0]["excerpts"] == ["# Metadata is literal user text"]
    assert "unsupported attachment must never escape" not in str(payload["items"])
    assert payload["sources"][0]["partially_indexed_records"] == 1
    response = await client.get(f"/sync/{fence.sync_id}/records/{locator.record_id}")
    assert response.status_code == 200, response.text
    assert response.json()["completeness"] == "complete"
    assert response.json()["extraction"] == coverage.model_dump(mode="json")
    async with database() as db:
        await db.execute(
            update(Entity).where(Entity.id == locator.record_id).values(record_revision=2)
        )
        await db.commit()
    response = await client.get(f"/sync/{fence.sync_id}/records/{locator.record_id}")
    assert response.json()["extraction"] is None


async def test_visibility_batches_exact_parts_generations_and_duplicate_chunks(database, indexed):
    from airweave.domains.search.canonical_visibility import visible_results
    from airweave.models.projection_generation import ProjectionGeneration

    fence, locator, connection = indexed
    registry = SimpleNamespace(
        get=lambda _: SimpleNamespace(
            source_class_ref=SimpleNamespace(canonical_record_types=("event",))
        )
    )
    async with database() as db:
        await db.execute(
            update(ProjectionGeneration)
            .where(ProjectionGeneration.id == locator.generation)
            .values(
                extraction_coverage={
                    "parts": [
                        {"part_index": 0, "key": "body", "kind": "body", "outcome": "indexed"},
                        {
                            "part_index": 1,
                            "key": "file",
                            "kind": "file",
                            "outcome": "unsupported",
                            "reason": "unsupported_format",
                        },
                    ],
                }
            )
        )
        await db.commit()
        valid = hit(fence, locator.encode())
        inputs = [
            valid,
            hit(fence, locator.model_copy(update={"part_index": 1}).encode()),
            hit(fence, locator.model_copy(update={"generation": uuid4()}).encode()),
            hit(fence, locator.model_copy(update={"pipeline_version": 1}).encode()),
            valid,
            valid.model_copy(
                update={
                    "airweave_system_metadata": valid.airweave_system_metadata.model_copy(
                        update={"sync_id": str(uuid4())}
                    )
                }
            ),
        ]
        assert await visible_results(
            db, fence.organization_id, connection.readable_collection_id, inputs, registry
        ) == [valid, valid]


@pytest.mark.parametrize(
    "mode,snippet",
    [
        ("keyword", "<sep /><hi>नमस्ते</hi> 原文 <script>alert(1)</script><sep />"),
        ("hybrid", "… <hi>नमस्ते</hi> 原文"),
        ("semantic", "… <hi>नमस्ते</hi> 原文"),
        ("hybrid", "An unmatched lead without term markers"),
        ("keyword", None),
    ],
)
async def test_unattested_lexical_fragment_is_not_content(
    database, indexed, http_search, mode, snippet
):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    http_search[3]._dense_embedder = FakeDenseEmbedder(dimensions=3)
    original = "Leading context. " * 200 + "नमस्ते 原文"
    candidate = hit(fence, locator.encode()).model_copy(
        update={
            "textual_representation": original,
            "query_snippet": snippet,
        }
    )
    vector.seed_results(SearchResults(results=[candidate]))
    response = await client.post(
        "/sync/search",
        json={
            "query": "नमस्ते",
            "sync_ids": [str(fence.sync_id)],
            "mode": mode,
        },
    )
    assert response.status_code == 200, response.text
    assert (
        response.json()["items"][0]["excerpts"] == []
    )  # Unattested lexical metadata is not content.
    assert response.json()["items"][0]["record_id"] == str(locator.record_id)
    assert candidate.textual_representation == original


@pytest.mark.parametrize("unavailable", [False, True])
async def test_collection_retrieval_overlaps_with_bounded_independent_sessions(
    database, indexed, http_search, unavailable
):
    """Seven collections must overlap, without seven simultaneous index requests."""
    fence, _, _ = indexed
    client, _, _, executor, _ = http_search
    syncs = [fence.sync_id]
    async with database() as db:
        deployment = await db.scalar(select(VectorDbDeploymentMetadata))
        for i in range(6):
            sync_id = uuid4()
            syncs.append(sync_id)
            db.add(
                Collection(
                    name=f"Parallel {i}",
                    readable_id=f"parallel-{i}",
                    organization_id=fence.organization_id,
                    vector_db_deployment_metadata_id=deployment.id,
                )
            )
            db.add(Sync(id=sync_id, organization_id=fence.organization_id, name=f"Parallel {i}"))
            await db.flush()
            db.add(
                SourceConnection(
                    name=f"Parallel {i}",
                    short_name="gmail",
                    organization_id=fence.organization_id,
                    readable_collection_id=f"parallel-{i}",
                    sync_id=sync_id,
                    is_authenticated=True,
                )
            )
        await db.commit()
    first_wave = asyncio.Event()
    active = peak = 0
    sessions_seen = []
    embeddings_seen = []

    async def retrieve(**kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        sessions_seen.append(kwargs["db"])
        embeddings_seen.append(kwargs["prepared_query"])
        if active == 4:
            first_wave.set()
        try:
            # A sequential implementation cannot release this rendezvous.
            await asyncio.wait_for(first_wave.wait(), timeout=2)
            await asyncio.sleep(0)
            if unavailable and kwargs["collection_readable_id"] == "parallel-3":
                raise HTTPException(404, "Collection was withdrawn")
            return SearchResults(results=[])
        finally:
            active -= 1

    executor.execute = retrieve
    response = await client.post(
        "/sync/search",
        json={
            "query": "budget",
            "sync_ids": [str(sync) for sync in syncs],
            "mode": "keyword",
        },
    )
    if unavailable:
        assert response.status_code == 404
        assert response.json() == {"detail": "Collection was withdrawn"}
    else:
        assert response.status_code == 200, response.text
        assert response.json()["items"] == []
    assert peak == 4 and len(sessions_seen) == 7
    assert len({id(session) for session in sessions_seen}) == 7
    assert embeddings_seen[0] is not None
    assert all(value is embeddings_seen[0] for value in embeddings_seen)


@pytest.mark.parametrize("changed", [False, True])
async def test_candidate_http_never_reranks_and_preserves_exact_bounded_text(
    database, source, indexed, http_search, changed
):
    fence, locator, _ = indexed
    client, vector, _, executor, _ = http_search
    original = hit(fence, locator.encode())
    original.textual_representation = "Original matching body " + "x" * 40000
    vector.seed_results(SearchResults(results=[original]))
    service = client._transport.app.dependency_overrides[deps.get_container]().owned_search
    service._reranker = SimpleNamespace(
        rerank=AsyncMock(
            side_effect=AssertionError("Candidates must not reach an external reranker")
        )
    )
    service._tokenizer = SimpleNamespace(count_tokens=lambda text: len(text))
    execute = executor.execute
    requests = 0

    async def retrieve(**kwargs):
        nonlocal requests
        requests += 1
        result = await execute(**kwargs)
        if changed:
            await capture(database, source[0], fence, observation(payload={"changed": True}))
        return result

    executor.execute = retrieve
    response = await client.post(
        "/sync/search/candidates",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "mode": "keyword",
        },
    )
    assert response.status_code == 200, response.text
    assert requests == 1
    service._reranker.rerank.assert_not_awaited()
    body = response.json()
    assert body["authority"] == "canonical_snapshot"
    assert body["order"] == "retrieval_rank"
    if changed:
        assert body["candidates"] == []
        assert "Original matching body" not in response.text
    else:
        (candidate,) = body["candidates"]
        assert candidate["projection"] == locator.model_dump(mode="json")
        assert candidate["hit"]["record_id"] == str(locator.record_id)
        assert candidate["text"] == original.textual_representation[:32000]
        assert candidate["text_truncated"] is True
        assert candidate["hit"]["excerpts"] == []


@pytest.mark.parametrize("operation", ["candidates", "rank"])
async def test_candidate_endpoint_rejects_user_context_before_retrieval(
    indexed, http_search, operation
):
    fence, _, _ = indexed
    client, _, ctx, executor, _ = http_search
    ctx.is_api_key_auth = False
    executor.execute = AsyncMock(
        side_effect=AssertionError("User context must not retrieve candidates")
    )
    body = {"query": "budget", "sync_ids": [str(fence.sync_id)]}
    body.update({"mode": "keyword"} if operation == "candidates" else {"candidates": []})
    response = await client.post(f"/sync/search/{operation}", json=body)
    assert response.status_code == 403
    executor.execute.assert_not_awaited()


@pytest.mark.parametrize("change", [None, "before", "during"])
async def test_two_phase_ranking_rechecks_without_retrieving_again(
    database, source, indexed, http_search, change
):
    from airweave.core.protocols.reranker import RerankerResult

    fence, locator, _ = indexed
    client, vector, _, executor, _ = http_search
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    response = await client.post(
        "/sync/search/candidates",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "mode": "keyword",
        },
    )
    assert response.status_code == 200, response.text
    candidates = response.json()["candidates"]
    assert len(candidates) == 1
    executor.execute = AsyncMock(
        side_effect=AssertionError("Ranking must not repeat index retrieval")
    )
    executor.prepare_query = AsyncMock(side_effect=AssertionError("Ranking must not embed again"))
    service = client._transport.app.dependency_overrides[deps.get_container]().owned_search

    async def change_record():
        await capture(database, source[0], fence, observation(payload={"changed": True}))

    async def rerank(query, documents, top_n):
        assert query == "budget" and len(documents) == top_n == 1
        if change == "during":
            await change_record()
        return [RerankerResult(0, 1.0)]

    model = AsyncMock(side_effect=rerank)
    service._reranker = SimpleNamespace(rerank=model)
    service._tokenizer = SimpleNamespace(count_tokens=lambda text: len(text))
    if change == "before":
        await change_record()
    response = await client.post(
        "/sync/search/rank",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "candidates": candidates,
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["record_ids"] == ([] if change else [str(locator.record_id)])
    assert response.json()["excluded_candidates"] == (1 if change else 0)
    assert model.await_count == (0 if change == "before" else 1)
    executor.execute.assert_not_awaited()
    executor.prepare_query.assert_not_awaited()
