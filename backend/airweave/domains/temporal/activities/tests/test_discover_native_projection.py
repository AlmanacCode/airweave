"""Discovery uses registered indexing capabilities, never provider authentication calls."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

from airweave.domains.entities.canonical.projection_models import ProjectionSourcePage
from airweave.domains.temporal.activities import discover_native_projection as module


async def test_discovers_native_and_canonical_capabilities_with_bounded_cursor(monkeypatch):
    entries = {
        name: SimpleNamespace(
            short_name=name,
            source_class_ref=SimpleNamespace(canonical_record_types=kinds),
        )
        for name, kinds in (
            ("slack", ("channel", "message", "file")),
            ("gmail", ("message",)),
            ("legacy", ()),
        )
    }
    registry = Mock()
    registry.list_all.return_value = list(entries.values())
    registry.get.side_effect = entries.__getitem__
    db = object()

    @asynccontextmanager
    async def sessions():
        yield db

    pending = AsyncMock(return_value=ProjectionSourcePage(sources=(), next_cursor=None))
    monkeypatch.setattr(module, "get_db_context", sessions)
    monkeypatch.setattr(
        module, "CanonicalProjectionStore", lambda: SimpleNamespace(pending_sources=pending)
    )
    after = uuid4()
    result = await module.DiscoverNativeProjectionActivity(registry).run(str(after))
    assert result == {"sources": [], "next_cursor": None}
    pending.assert_awaited_once_with(
        db, source_names=("almanac", "slack", "gmail"), after_id=after, limit=20
    )
