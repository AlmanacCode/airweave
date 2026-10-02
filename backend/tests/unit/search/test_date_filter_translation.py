"""Date bounds use the same seconds unit as Vespa documents, without truncation."""

from unittest.mock import Mock

import pytest

from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.types.filters import FilterCondition, FilterGroup


@pytest.mark.parametrize("field", ["created_at", "updated_at"])
@pytest.mark.parametrize(
    "instant", ["2024-01-01T00:00:00Z", "2023-12-31T16:00:00-08:00", "2024-01-01T00:00:00"]
)
def test_integral_boundary_uses_utc_seconds(field, instant):
    translator = FilterTranslator(Mock())
    assert translator._parse_datetime_to_epoch(instant, field) == 1704067200


def test_fractional_half_open_bounds_survive_typed_filter_translation():
    translator = FilterTranslator(Mock())
    result = translator.translate(
        [
            FilterGroup(
                conditions=[
                    FilterCondition(
                        field="created_at",
                        operator="greater_than_or_equal",
                        value="2024-01-01T00:00:00.5Z",
                    ),
                    FilterCondition(
                        field="created_at", operator="less_than", value="2024-01-01T00:00:01.5Z"
                    ),
                ]
            )
        ]
    )
    assert "created_at >= 1704067200.5" in result
    assert "created_at < 1704067201.5" in result
