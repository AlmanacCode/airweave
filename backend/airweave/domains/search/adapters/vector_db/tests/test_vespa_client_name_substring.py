"""Tests for VespaVectorDB.count/filter_search `name_substring` plumbing.

We don't run a real Vespa here — we mock the `_app.query` call and assert the
generated YQL contains the expected regex `matches` clause. The branches under
test are:

- `_build_name_substring_clause`: escapes special regex/YQL characters.
- `count` and `filter_search` add the clause to YQL when `name_substring` is set.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from airweave.core.logging import logger
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB


def _make_client() -> tuple[VespaVectorDB, MagicMock]:
    """Build a VespaVectorDB with a MagicMock `_app` so no Vespa is needed."""
    app = MagicMock()
    response = MagicMock()
    response.is_successful.return_value = True
    response.json = {"root": {"fields": {"totalCount": 0}}}
    response.hits = []
    app.query.return_value = response

    ctx_logger = logger.with_context(request_id="test")
    client = VespaVectorDB(
        app=app,
        logger=ctx_logger,
        filter_translator=FilterTranslator(logger=ctx_logger),
    )
    return client, app


class TestBuildNameSubstringClause:
    """Direct tests for the YQL regex clause builder."""

    def test_simple_substring(self) -> None:
        client, _ = _make_client()
        clause = client._build_name_substring_clause("Quick")
        assert clause == 'name matches "(?i).*Quick.*"'

    def test_regex_chars_escaped(self) -> None:
        client, _ = _make_client()
        # `re.escape` escapes `+` and `.`; resulting backslashes must
        # be doubled for the YQL literal.
        clause = client._build_name_substring_clause("1.5+")
        # `.` -> `\.`, `+` -> `\+`. After YQL-escaping each `\` becomes `\\`.
        assert "1\\\\.5\\\\+" in clause
        assert clause.startswith('name matches "(?i).*')
        assert clause.endswith('.*"')

    def test_single_quote_not_escaped(self) -> None:
        """Single quote has no special meaning inside a double-quoted YQL literal."""
        client, _ = _make_client()
        clause = client._build_name_substring_clause("O'Brien")
        assert "O'Brien" in clause

    def test_double_quote_escaped(self) -> None:
        """Double quote must be escaped — it is the literal delimiter in our YQL."""
        client, _ = _make_client()
        clause = client._build_name_substring_clause('"hi"')
        # Each `"` in the input becomes `\"` in the YQL string literal.
        # Strip the surrounding `name matches "(?i).*` ... `.*"` wrapper and
        # check the inner escaped payload starts and ends with `\"`.
        assert clause.startswith('name matches "(?i).*\\"')
        assert clause.endswith('\\".*"')
        # And the YQL literal has exactly one opening + one closing `"` that
        # are not preceded by a backslash (i.e. exactly two unescaped quotes).
        unescaped = 0
        for i, ch in enumerate(clause):
            if ch == '"' and (i == 0 or clause[i - 1] != "\\"):
                unescaped += 1
        assert unescaped == 2


class TestCountWithNameSubstring:
    """`count()` includes the substring clause only when provided."""

    @pytest.mark.asyncio
    async def test_clause_present_when_name_substring_set(self) -> None:
        client, app = _make_client()
        await client.count(
            filter_groups=[],
            collection_id="col-123",
            name_substring="Quick",
        )
        yql = app.query.call_args.kwargs["body"]["yql"]
        assert 'name matches "(?i).*Quick.*"' in yql

    @pytest.mark.asyncio
    async def test_clause_absent_when_name_substring_none(self) -> None:
        client, app = _make_client()
        await client.count(filter_groups=[], collection_id="col-123")
        yql = app.query.call_args.kwargs["body"]["yql"]
        assert "name matches" not in yql

    @pytest.mark.asyncio
    async def test_clause_absent_when_name_substring_empty_string(self) -> None:
        client, app = _make_client()
        await client.count(filter_groups=[], collection_id="col-123", name_substring="")
        yql = app.query.call_args.kwargs["body"]["yql"]
        assert "name matches" not in yql


class TestFilterSearchWithNameSubstring:
    """`filter_search()` includes the substring clause only when provided."""

    @pytest.mark.asyncio
    async def test_clause_present_when_name_substring_set(self) -> None:
        client, app = _make_client()
        await client.filter_search(
            filter_groups=[],
            collection_id="col-123",
            name_substring="Quick",
        )
        yql = app.query.call_args.kwargs["body"]["yql"]
        assert 'name matches "(?i).*Quick.*"' in yql

    @pytest.mark.asyncio
    async def test_clause_absent_when_name_substring_none(self) -> None:
        client, app = _make_client()
        await client.filter_search(filter_groups=[], collection_id="col-123")
        yql = app.query.call_args.kwargs["body"]["yql"]
        assert "name matches" not in yql


@pytest.mark.parametrize("fragment", ["… <hi>原文</hi>", None, {"invalid": "shape"}])
def test_optional_query_snippet_never_replaces_full_chunk(fragment):
    client, _ = _make_client()
    result = client._convert_hits_to_results(
        [
            {
                "fields": {
                    "entity_id": "chunk",
                    "name": "Original",
                    "textual_representation": "Full retained chunk",
                    "query_snippet": fragment,
                }
            }
        ]
    ).results[0]
    assert result.textual_representation == "Full retained chunk"
    assert result.query_snippet == (fragment if isinstance(fragment, str) else None)
    assert "query_snippet" not in result.model_dump()
