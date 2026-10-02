"""Unusable coverage flags must never become a complete channel inventory."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackSource


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["false", None])
async def test_malformed_history_limit_cannot_authorize_completion(flag):
    source = SlackSource(
        auth=StaticTokenProvider("synthetic"), logger=MagicMock(), http_client=MagicMock()
    )
    source.slack_config = SlackConfig(expected_team_id="T1", expected_user_id="U1")
    source._get = AsyncMock(return_value={"ok": True, "team_id": "T1", "user_id": "U1"})
    await source.validate()
    source._get = AsyncMock(return_value={"messages": [], "is_limited": flag})
    with pytest.raises(ValueError, match="invalid page"):
        await source.capture_page(
            CompletedScope(record_type="message", container_id="C1"),
            ScanContinuation(value={}),
            files=MagicMock(),
        )
