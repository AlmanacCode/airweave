"""Exact native kinds filter before retrieval and are checked against retained SQL."""

# Imported pytest fixtures are requested by parameter name.
# ruff: noqa: F811
from uuid import uuid4

from sqlalchemy import select, update

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.search_metadata import stamp_search_metadata
from airweave.domains.entities.canonical.tests.helpers import publish_prepared
from airweave.domains.entities.canonical.tests.test_owned_search import (  # noqa: F401
    http_search,
    indexed,
)
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.native_ingestion.tests.test_ingestion import ingest
from airweave.domains.native_ingestion.tests.test_projection import knowledge
from airweave.domains.search.types import SearchResults
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.platform.destinations.vespa.transformer import EntityTransformer
from airweave.platform.entities._base import AirweaveSystemMetadata
from airweave.platform.entities.slack import SlackChannelEntity


async def test_exact_kind_reprojection_prefilter_and_sql_guard(database, indexed, http_search):
    fence, _, connection = indexed
    client, vector, _, _, _ = http_search
    request = {
        "query": "fundraising",
        "sync_ids": [str(fence.sync_id)],
        "mode": "keyword",
        "native_types": ["person"],
    }
    response = await client.post("/sync/search/candidates", json=request)
    assert response.status_code == 200, response.text
    assert response.json()["candidates"] == []  # Provider v2 needs no native upgrade.
    vector._calls.clear()
    async with database() as db:
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.id == connection.id)
            .values(short_name="almanac")
        )
        await db.commit()
    response = await client.post("/sync/search/candidates", json=request)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "reindex_required"
    assert not vector._calls
    async with database() as db:
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.id == connection.id)
            .values(
                short_name="almanac",
                config_fields={"owner_id": "owner-one", "dataset": "knowledge"},
            )
        )
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=3)
        )
        collection = await db.scalar(
            select(Collection.id).where(Collection.readable_id == connection.readable_collection_id)
        )
        await db.commit()
    for kind in ("page", "person"):
        item = knowledge(id=kind, type=kind).model_copy(
            update={"identity": RecordIdentity(record_type="knowledge", native_id=kind)}
        )
        await ingest(database, fence, item)
    store = CanonicalProjectionStore()
    async with database() as db:
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
    candidates = []
    for work in pending:
        if work.record.identity.record_type != "knowledge":
            continue
        generation = uuid4()
        async with database() as db:
            assert await publish_prepared(store, db, work, generation, 1, collection)
        locator = ProjectionLocator(
            record_id=work.record.id,
            revision=work.record.revision,
            pipeline_version=3,
            generation=generation,
            part_index=0,
        )
        candidate = hit(fence, locator.encode())
        candidate.airweave_system_metadata.source_name = "almanac"
        candidates.append(candidate)
        meta = AirweaveSystemMetadata(source_name="almanac")
        stamp_search_metadata(meta, work.record)
        entity = SlackChannelEntity(
            channel_id="fixture", title="Fixture", purpose="", topic="", breadcrumbs=[]
        )
        entity.airweave_system_metadata = meta
        assert (
            EntityTransformer()._build_system_metadata(entity)["native_type"]
            == work.record.identity.native_id
        )
    # The fake deliberately ignores filters: SQL must still reject the page.
    vector.seed_results(SearchResults(results=candidates))
    response = await client.post("/sync/search/candidates", json=request)
    assert response.status_code == 200, response.text
    body = response.json()
    assert [item["hit"]["identity"]["native_id"] for item in body["candidates"]] == ["person"]
    assert body["postfilter_excluded"] == 1
    conditions = vector._calls[0][1].filter_groups[0].conditions
    assert any(
        item.field.value == "airweave_system_metadata.native_type" and item.value == ["person"]
        for item in conditions
    )
