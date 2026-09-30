"""Persistence failure must never masquerade as an empty or saved checkpoint."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from airweave.domains.syncs.cursors.service import SyncCursorService


@pytest.mark.parametrize(
    ("operation", "crud_operation", "extra"),
    [
        ("get_cursor_data", "get_by_sync_id", {}),
        ("get_cursor_field", "get_by_sync_id", {}),
        ("create_or_update_cursor", "create_or_update", {"cursor_data": {"position": "42"}}),
        ("update_cursor_data", "update_cursor_data", {"cursor_data": {"position": "42"}}),
        ("delete_cursor", "delete_by_sync_id", {}),
    ],
)
async def test_database_failure_propagates(operation, crud_operation, extra):
    failure = ConnectionError("database unavailable")
    with patch(
        f"airweave.domains.syncs.cursors.service.crud.sync_cursor.{crud_operation}",
        new=AsyncMock(side_effect=failure),
    ):
        with pytest.raises(ConnectionError) as raised:
            await getattr(SyncCursorService(), operation)(
                db=MagicMock(), sync_id=uuid4(), ctx=MagicMock(), **extra
            )
    assert raised.value is failure


async def test_missing_checkpoint_is_distinct_from_database_failure():
    with patch(
        "airweave.domains.syncs.cursors.service.crud.sync_cursor.get_by_sync_id",
        new=AsyncMock(return_value=None),
    ):
        assert await SyncCursorService().get_cursor_data(MagicMock(), uuid4(), MagicMock()) == {}
