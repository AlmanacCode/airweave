"""Engine coverage survives the real adapter; only remote HTTP is mocked."""

from unittest.mock import MagicMock

import pytest

from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.types import CompiledQuery


@pytest.mark.parametrize(
    "coverage,errors,expected",
    [
        ({"coverage": 100, "full": True}, [], False),
        ({"coverage": 75}, [], True),
        ({"coverage": 100, "degraded": {"timeout": True}}, [], True),
        ({"coverage": 100, "full": False}, [], True),
        ({"coverage": 100}, [{"code": 12}], True),
    ],
)
async def test_partial_engine_response_preserved(coverage, errors, expected):
    app = MagicMock()
    response = MagicMock()
    response.is_successful.return_value = True
    response.json = {"root": {"coverage": coverage, "errors": errors}}
    response.hits = []
    app.query.return_value = response
    client = VespaVectorDB(app, MagicMock(), MagicMock())
    result = await client.execute_query(
        CompiledQuery(
            vector_db="vespa",
            display="test",
            raw={"yql": "select * from sources * where true", "params": {}},
        )
    )
    assert result.engine_partial is expected
    assert result.retrieval_incomplete is expected
    assert result.engine_coverage_percent == coverage["coverage"]
