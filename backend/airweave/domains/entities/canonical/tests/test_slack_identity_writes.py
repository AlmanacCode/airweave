"""A different native Slack user must never begin source capture."""

from unittest.mock import MagicMock

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackSource


async def test_wrong_user_leaves_existing_cursor_and_originals_unchanged(database, source):
    async with database() as db:
        before = [(row.id, row.cursor_data) for row in (await db.scalars(select(SyncCursor))).all()]
    calls = []

    def profile(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"ok": True, "team_id": "T1", "user_id": "U2"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(profile)) as client:
        with pytest.raises(ValueError, match="does not match"):
            await SlackSource.create(
                auth=StaticTokenProvider("same-broker"),
                logger=MagicMock(),
                http_client=client,
                config=SlackConfig(expected_team_id="T1", expected_user_id="U1"),
            )
    assert calls == ["/api/auth.test"]
    async with database() as db:
        assert (await db.scalars(select(Entity))).all() == []
        after = [(row.id, row.cursor_data) for row in (await db.scalars(select(SyncCursor))).all()]
        assert after == before
