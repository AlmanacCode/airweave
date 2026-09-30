"""Every vector content retrieval must pass authoritative publication validation."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from airweave.domains.search.adapters.vector_db.exceptions import VectorDBError
from airweave.domains.search.agentic.state import AgentState
from airweave.domains.search.agentic.tests.conftest import make_result
from airweave.domains.search.agentic.tools.read import ReadTool
from airweave.domains.search.types.results import SearchResults
from airweave.domains.search.visible_vector_db import UnavailableExactCount, VisibleVectorDB


def wrapper():
    delegate = AsyncMock()
    db = AsyncMock()
    ctx = SimpleNamespace(organization=SimpleNamespace(id=uuid4()))
    registry = Mock()
    return (
        VisibleVectorDB(delegate, db, ctx, "readable", "collection", registry),
        delegate,
        db,
        registry,
    )


@pytest.mark.asyncio
async def test_execute_rejects_stale_candidate_and_reports_underfilled_page(monkeypatch):
    visible, delegate, _, _ = wrapper()
    fresh, stale = make_result("fresh"), make_result("stale", content="do not expose")
    delegate.execute_query.return_value = SearchResults(results=[fresh, stale])
    gate = AsyncMock(return_value=[fresh])
    monkeypatch.setattr("airweave.domains.search.visible_vector_db.visible_results", gate)
    result = await visible.execute_query("compiled")
    assert result.results == [fresh]
    assert result.retrieval_incomplete is True
    assert result.excluded_candidates == 1
    assert gate.await_args.args[3] == [fresh, stale]


@pytest.mark.asyncio
async def test_filter_read_validates_before_returning_original_text(monkeypatch):
    visible, delegate, _, _ = wrapper()
    stale = make_result("stale", content="do not expose")
    delegate.filter_search.return_value = [stale]
    gate = AsyncMock(return_value=[])
    monkeypatch.setattr("airweave.domains.search.visible_vector_db.visible_results", gate)
    assert await visible.filter_search([], "collection") == []
    gate.assert_awaited_once()


@pytest.mark.asyncio
async def test_cached_read_does_not_fall_back_after_gate_removes_chunks(monkeypatch):
    visible, delegate, _, _ = wrapper()
    stale = make_result("stale", content="do not expose")
    delegate.filter_search.return_value = [stale]
    monkeypatch.setattr(
        "airweave.domains.search.visible_vector_db.visible_results", AsyncMock(return_value=[])
    )
    state = AgentState()
    state.results[stale.entity_id] = stale
    result = await ReadTool(visible, "collection").execute({"entity_ids": [stale.entity_id]}, state)
    assert result.entities == []
    assert result.not_found == ["stale"]


@pytest.mark.asyncio
async def test_cross_collection_reads_fail_before_delegate_request():
    visible, delegate, _, _ = wrapper()
    with pytest.raises(VectorDBError, match="Collection"):
        await visible.filter_search([], "other")
    with pytest.raises(VectorDBError, match="Collection"):
        await visible.compile_query(None, None, "other")
    with pytest.raises(VectorDBError, match="Collection"):
        await visible.count([], "other")
    assert delegate.mock_calls == []


@pytest.mark.asyncio
async def test_canonical_count_is_unavailable_instead_of_stale_index_total():
    visible, delegate, db, registry = wrapper()
    db.scalars.return_value = [SimpleNamespace(short_name="gmail", sync_id=uuid4())]
    registry.get.return_value = SimpleNamespace(
        source_class_ref=SimpleNamespace(canonical_record_types=("message",))
    )
    with pytest.raises(UnavailableExactCount):
        await visible.count([], "collection")
    delegate.count.assert_not_called()


@pytest.mark.asyncio
async def test_gate_failure_propagates_without_returning_unvalidated_data(monkeypatch):
    visible, delegate, _, _ = wrapper()
    delegate.filter_search.return_value = [make_result("unvalidated")]
    monkeypatch.setattr(
        "airweave.domains.search.visible_vector_db.visible_results",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    with pytest.raises(RuntimeError, match="database unavailable"):
        await visible.filter_search([], "collection")


@pytest.mark.asyncio
async def test_wrapper_close_preserves_shared_adapter():
    visible, delegate, _, _ = wrapper()
    await visible.close()
    delegate.close.assert_not_called()


@pytest.mark.asyncio
async def test_cached_read_never_masks_vector_failure_with_old_text():
    visible, delegate, _, _ = wrapper()
    stale = make_result("stale", content="must not appear after failure")
    delegate.filter_search.side_effect = VectorDBError("vector unavailable")
    state = AgentState()
    state.results[stale.entity_id] = stale
    with pytest.raises(VectorDBError, match="vector unavailable"):
        await ReadTool(visible, "collection").execute({"entity_ids": ["stale"]}, state)


@pytest.mark.asyncio
async def test_legacy_count_ands_authorized_syncs_into_each_or_group():
    from airweave.domains.search.types.filters import (
        FilterableField,
        FilterCondition,
        FilterGroup,
        FilterOperator,
    )

    visible, delegate, db, registry = wrapper()
    sync_id = uuid4()
    db.scalars.return_value = [SimpleNamespace(short_name="legacy", sync_id=sync_id)]
    registry.get.return_value = SimpleNamespace(source_class_ref=SimpleNamespace())
    delegate.count.return_value = 7
    groups = [
        FilterGroup(
            conditions=[
                FilterCondition(
                    field=FilterableField.NAME, operator=FilterOperator.EQUALS, value=name
                )
            ]
        )
        for name in ("A", "B")
    ]
    assert await visible.count(groups, "collection") == 7
    passed = delegate.count.await_args.args[0]
    assert len(passed) == 2
    for original, scoped in zip(groups, passed, strict=True):
        assert scoped.conditions[:-1] == original.conditions
        assert scoped.conditions[-1].field == FilterableField.SYSTEM_METADATA_SYNC_ID
        assert scoped.conditions[-1].operator == FilterOperator.IN
        assert scoped.conditions[-1].value == [str(sync_id)]


@pytest.mark.asyncio
async def test_no_authorized_source_count_does_not_use_index_total():
    visible, delegate, db, _ = wrapper()
    db.scalars.return_value = []
    assert await visible.count([], "collection") == 0
    delegate.count.assert_not_called()
