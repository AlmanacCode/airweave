"""Real SQL final publication checks precede grouping and result limits."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from airweave.api import deps
from airweave.core.protocols.reranker import RerankerResult
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import capture, observation, publish_prepared
from airweave.domains.entities.canonical.tests.test_owned_search import (  # noqa: F401
    http_search,
    indexed,
)
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.native_ingestion.models import SessionVersion
from airweave.domains.native_ingestion.tests.test_ingestion import ingest
from airweave.domains.native_ingestion.tests.test_projection import (
    message as native_message,
)
from airweave.domains.native_ingestion.tests.test_projection import (
    session as native_session,
)
from airweave.domains.search.types import SearchResults
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection


@pytest.mark.parametrize("withdraw", [False, True, "parent", "native_current"])
async def test_group_after_final_gate_promotes_survivor_and_limits_cards(
    database,
    source,
    indexed,  # noqa: F811
    http_search,  # noqa: F811
    withdraw,
):
    fence, _, connection = indexed
    capture_service, _ = source
    client, vector, _, _, _ = http_search
    messages = [
        observation(
            identity=RecordIdentity(record_type="message", native_id=f"m{i}"),
            payload={"threadId": "shared" if i < 3 else "other"},
        )
        for i in range(4)
    ]
    native = withdraw in ("parent", "native_current")
    if native:
        session = native_session()
        version = SessionVersion(revision=7, content_revision=11)
        session = session.model_copy(
            update={
                "version": version,
                "original": session.original | {"revision": 7, "content_revision": 11},
            }
        )
        parent = session.identity
        original = native_message("Original message")
        native_messages = [
            original.model_copy(
                update={
                    "version": version,
                    "identity": RecordIdentity(
                        record_type="message", native_id=f"m{i}", container_id=parent.native_id
                    ),
                    "original": original.original | {"id": f"m{i}", "ordinal": i},
                }
            )
            for i in range(4)
        ]
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.id == connection.id)
                .values(
                    short_name="almanac",
                    config_fields={"owner_id": "owner-one", "dataset": "sessions"},
                )
            )
            await db.commit()
        await ingest(database, fence, session, *native_messages)
    else:
        await capture(database, capture_service, fence, *messages)
    store = CanonicalProjectionStore()
    async with database() as db:
        collection_id = await db.scalar(
            select(Collection.id).where(Collection.readable_id == connection.readable_collection_id)
        )
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
    candidates = []
    for work in sorted(pending, key=lambda item: item.record.identity.native_id):
        generation = uuid4()
        async with database() as db:
            assert await publish_prepared(store, db, work, generation, 1, collection_id)
        locator = ProjectionLocator(
            record_id=work.record.id,
            revision=work.record.revision,
            pipeline_version=work.pipeline_version,
            generation=generation,
            part_index=0,
        )
        candidate = hit(fence, locator.encode())
        if native:
            candidate.airweave_system_metadata.source_name = "almanac"
        candidate.name = work.record.identity.native_id
        candidate.textual_representation = "exact " + candidate.name
        candidates.append(candidate)
    vector.seed_results(SearchResults(results=candidates))
    service = client._transport.app.dependency_overrides[deps.get_container]().owned_search
    service._tokenizer = type("Tokenizer", (), {"count_tokens": lambda self, text: len(text)})()
    if withdraw == "native_current":
        response = await client.post(
            "/sync/search/candidates",
            json={
                "query": "exact",
                "sync_ids": [str(fence.sync_id)],
                "mode": "keyword",
                "limit": 2,
            },
        )
        assert response.status_code == 200, response.text
        page = response.json()
        assert len(page["candidates"]) == 2 and page["shortlist_truncated"]
        _assert_ungrouped_native_versions(page)

    async def rerank(query, documents, top_n):
        if withdraw is True or withdraw == "parent":
            await capture(
                database,
                capture_service,
                fence,
                observation(
                    identity=parent if withdraw == "parent" else messages[0].identity,
                    kind="delete",
                    removal_reason="access_revoked",
                ),
            )
        return [RerankerResult(i, float(top_n - i)) for i in range(top_n)]

    service._reranker = type("Reranker", (), {"rerank": AsyncMock(side_effect=rerank)})()
    response = await client.post(
        "/sync/search",
        json={"query": "exact", "sync_ids": [str(fence.sync_id)], "mode": "keyword", "limit": 2},
    )
    assert response.status_code == 200, response.text
    items = response.json()["items"]
    if withdraw == "parent":
        assert items == []
        return
    if native:
        matches = [
            match for item in items for match in (item, *item["group"]["additional_matches"])
        ]
        assert matches
        for match in matches:
            assert match["native_version"] == {
                "kind": "session",
                "revision": 7,
                "content_revision": 11,
            }
            assert match["revision"] == 1
        return
    assert len(items) == 2
    assert all(item["native_version"] is None for item in items)
    first = items[0]
    assert first["identity"]["native_id"] == ("m1" if withdraw else "m0")
    assert first["title"] == first["identity"]["native_id"]
    assert first["excerpts"] == ["exact " + first["title"]]
    assert first["group"]["matched_records"] == (2 if withdraw else 3)
    assert first["group"]["native_id"] == "shared"
    assert items[1]["group"]["native_id"] == "other"
    assert all(m["identity"]["native_id"] != "m0" for m in first["group"]["additional_matches"])


def _assert_ungrouped_native_versions(page):
    for candidate in page["candidates"]:
        assert candidate["hit"]["native_version"] == {
            "kind": "session",
            "revision": 7,
            "content_revision": 11,
        }
        assert candidate["hit"]["group"]["matched_records"] == 1
        assert candidate["hit"]["group"]["additional_matches"] == []
