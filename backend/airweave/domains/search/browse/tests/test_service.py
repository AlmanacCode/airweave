"""Tests for BrowseService."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from airweave.api.context import ApiContext
from airweave.core.logging import logger
from airweave.core.shared_models import AuthMethod
from airweave.domains.collections.fakes.repository import FakeCollectionRepository
from airweave.domains.search.adapters.vector_db.fakes.vector_db import FakeVectorDB
from airweave.domains.search.agentic.tests.conftest import make_result
from airweave.domains.search.browse.service import BrowseService
from airweave.domains.search.tests.test_executor import _make_registry_entry
from airweave.domains.search.types.filters import (
    FilterableField,
    FilterCondition,
    FilterGroup,
    FilterOperator,
)
from airweave.domains.sources.fakes.registry import FakeSourceRegistry
from airweave.models.collection import Collection
from airweave.schemas.search_v2 import BrowseRequest


@pytest.fixture(autouse=True)
def isolated_publication_gate(monkeypatch):
    """Synthetic orchestration fixtures do not query the real publication database."""

    async def passthrough(db, organization_id, collection, results, registry):
        return results

    monkeypatch.setattr("airweave.domains.search.visible_vector_db.visible_results", passthrough)


DEFAULT_ORG_ID = uuid4()
DEFAULT_COLLECTION_ID = uuid4()
DEFAULT_READABLE_ID = "test-col"


def _make_ctx() -> ApiContext:
    from airweave.schemas.organization import Organization

    now = datetime.now(timezone.utc)
    org = Organization(
        id=str(DEFAULT_ORG_ID),
        name="Test Org",
        created_at=now,
        modified_at=now,
        enabled_features=[],
    )
    return ApiContext(
        request_id="test-req-001",
        organization=org,
        auth_method=AuthMethod.SYSTEM,
        auth_metadata={},
        logger=logger.with_context(request_id="test-req-001"),
    )


def _make_collection() -> Collection:
    now = datetime.now(timezone.utc)
    col = Collection(
        id=DEFAULT_COLLECTION_ID,
        name="Test Collection",
        readable_id=DEFAULT_READABLE_ID,
        organization_id=DEFAULT_ORG_ID,
        vector_db_deployment_metadata_id=uuid4(),
    )
    col.created_at = now
    col.modified_at = now
    return col


def _make_db():
    db = AsyncMock()
    db.scalars.return_value = [SimpleNamespace(short_name="legacy", sync_id=uuid4())]
    return db


def _make_service() -> tuple[BrowseService, FakeVectorDB, FakeCollectionRepository]:
    vector_db = FakeVectorDB()
    collection_repo = FakeCollectionRepository()
    registry = FakeSourceRegistry()
    registry.seed(_make_registry_entry("legacy"))
    svc = BrowseService(
        vector_db=vector_db, collection_repo=collection_repo, source_registry=registry
    )
    return svc, vector_db, collection_repo


class TestBrowseService:
    """Tests for BrowseService.browse()."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_response(self) -> None:
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        row = make_result(entity_id="ent-1", name="Row 1")
        vector_db.seed_filter_results([row])
        vector_db.seed_count(42)

        response = await svc.browse(
            _make_db(), _make_ctx(), DEFAULT_READABLE_ID, BrowseRequest(limit=10, offset=0)
        )

        assert response.total == 42
        assert response.limit == 10
        assert response.offset == 0
        assert len(response.results) == 1
        assert response.results[0].entity_id == "ent-1"

    @pytest.mark.asyncio
    async def test_collection_not_found_raises_404(self) -> None:
        svc, _, _ = _make_service()

        with pytest.raises(HTTPException) as exc_info:
            await svc.browse(_make_db(), _make_ctx(), "nonexistent", BrowseRequest())

        assert exc_info.value.status_code == 404
        assert "nonexistent" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_chunk_index_anchor_added_to_filter_groups(self) -> None:
        """The chunk_index=0 anchor must be AND-ed into every filter group."""
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        await svc.browse(_make_db(), _make_ctx(), DEFAULT_READABLE_ID, BrowseRequest())

        filter_call = next(c for c in vector_db._calls if c[0] == "filter_search")
        filter_groups = filter_call[1]
        assert len(filter_groups) == 1
        anchor_fields = [c.field for c in filter_groups[0].conditions]
        assert FilterableField.SYSTEM_METADATA_CHUNK_INDEX in anchor_fields

    @pytest.mark.asyncio
    async def test_sync_ids_translated_to_filter(self) -> None:
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        sync_id = str(uuid4())
        await svc.browse(
            _make_db(),
            _make_ctx(),
            DEFAULT_READABLE_ID,
            BrowseRequest(sync_ids=[sync_id]),
        )

        filter_call = next(c for c in vector_db._calls if c[0] == "filter_search")
        conditions = filter_call[1][0].conditions
        sync_conditions = [
            c for c in conditions if c.field == FilterableField.SYSTEM_METADATA_SYNC_ID
        ]
        assert len(sync_conditions) == 1
        assert sync_conditions[0].operator == FilterOperator.IN
        assert sync_conditions[0].value == [sync_id]

    @pytest.mark.asyncio
    async def test_entity_types_translated_to_filter(self) -> None:
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        await svc.browse(
            _make_db(),
            _make_ctx(),
            DEFAULT_READABLE_ID,
            BrowseRequest(entity_types=["NotionPageEntity", "SlackMessageEntity"]),
        )

        filter_call = next(c for c in vector_db._calls if c[0] == "filter_search")
        conditions = filter_call[1][0].conditions
        et_conditions = [
            c for c in conditions if c.field == FilterableField.SYSTEM_METADATA_ENTITY_TYPE
        ]
        assert len(et_conditions) == 1
        assert et_conditions[0].value == ["NotionPageEntity", "SlackMessageEntity"]

    @pytest.mark.asyncio
    async def test_user_filter_combined_with_anchor(self) -> None:
        """User-supplied filter groups should each get the anchor AND-ed in."""
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        user_filter = [
            FilterGroup(
                conditions=[
                    FilterCondition(
                        field=FilterableField.SYSTEM_METADATA_SOURCE_NAME,
                        operator=FilterOperator.EQUALS,
                        value="notion",
                    )
                ]
            ),
            FilterGroup(
                conditions=[
                    FilterCondition(
                        field=FilterableField.SYSTEM_METADATA_SOURCE_NAME,
                        operator=FilterOperator.EQUALS,
                        value="slack",
                    )
                ]
            ),
        ]
        await svc.browse(
            _make_db(),
            _make_ctx(),
            DEFAULT_READABLE_ID,
            BrowseRequest(filter=user_filter),
        )

        filter_call = next(c for c in vector_db._calls if c[0] == "filter_search")
        filter_groups = filter_call[1]
        assert len(filter_groups) == 2
        for group in filter_groups:
            anchor_present = any(
                c.field == FilterableField.SYSTEM_METADATA_CHUNK_INDEX for c in group.conditions
            )
            source_present = any(
                c.field == FilterableField.SYSTEM_METADATA_SOURCE_NAME for c in group.conditions
            )
            assert anchor_present and source_present

    @pytest.mark.asyncio
    async def test_name_query_passed_as_name_substring(self) -> None:
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        await svc.browse(
            _make_db(),
            _make_ctx(),
            DEFAULT_READABLE_ID,
            BrowseRequest(name_query="  Quick  "),
        )

        filter_call = next(c for c in vector_db._calls if c[0] == "filter_search")
        count_call = next(c for c in vector_db._calls if c[0] == "count")
        # filter_search signature: (op, filter_groups, collection_id, limit, offset, name_substring)
        assert filter_call[5] == "Quick"
        # count signature: (op, filter_groups, collection_id, name_substring)
        assert count_call[3] == "Quick"

    @pytest.mark.asyncio
    async def test_blank_name_query_treated_as_none(self) -> None:
        svc, vector_db, repo = _make_service()
        repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())

        await svc.browse(
            _make_db(),
            _make_ctx(),
            DEFAULT_READABLE_ID,
            BrowseRequest(name_query="   "),
        )

        filter_call = next(c for c in vector_db._calls if c[0] == "filter_search")
        assert filter_call[5] is None

    def test_name_query_rejects_single_character(self) -> None:
        """Single-character `name_query` is rejected to avoid full-scan triggers."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            BrowseRequest(name_query="a")

    def test_sync_ids_rejects_oversized_list(self) -> None:
        """`sync_ids` is capped at 100 entries."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            BrowseRequest(sync_ids=[str(uuid4()) for _ in range(101)])


@pytest.mark.asyncio
async def test_browse_filters_before_output_and_reports_unknown_canonical_count(monkeypatch):
    from airweave.domains.search.visible_vector_db import UnavailableExactCount

    svc, vector_db, repo = _make_service()
    repo.seed_readable(DEFAULT_READABLE_ID, _make_collection())
    fresh, stale = make_result("fresh"), make_result("stale")
    vector_db.seed_filter_results([fresh, stale])
    gate = AsyncMock(return_value=[fresh])
    monkeypatch.setattr("airweave.domains.search.visible_vector_db.visible_results", gate)
    monkeypatch.setattr(
        "airweave.domains.search.visible_vector_db.VisibleVectorDB.count",
        AsyncMock(side_effect=UnavailableExactCount("unknown")),
    )
    response = await svc.browse(_make_db(), _make_ctx(), DEFAULT_READABLE_ID, BrowseRequest())
    assert response.results == [fresh]
    assert response.total is None
    assert response.retrieval_incomplete is True
    assert gate.await_args.args[3] == [fresh, stale]
