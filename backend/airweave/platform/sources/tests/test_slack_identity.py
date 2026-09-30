"""Private-channel visibility is bound to both native workspace and user."""

from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackSource


@pytest.mark.asyncio
async def test_native_user_change_clears_attestation_and_changes_fingerprint():
    profile = {"ok": True, "team_id": "T1", "user_id": "U1"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=profile))
    ) as client:
        source = await SlackSource.create(
            auth=StaticTokenProvider("same-broker"),
            logger=MagicMock(),
            http_client=client,
            config=SlackConfig(expected_team_id="T1", expected_user_id="U1"),
        )
        original = source.capture_cycle_configuration.fingerprint
        profile["user_id"] = "U2"
        with pytest.raises(ValueError, match="does not match"):
            await source.validate()
        with pytest.raises(ValueError, match="attested workspace"):
            await source.capture_page(
                CompletedScope(record_type="channel"), ScanContinuation(), files=MagicMock()
            )
        source.slack_config = SlackConfig(expected_team_id="T1", expected_user_id="U2")
        await source.validate()
        assert source.capture_cycle_configuration.fingerprint != original
        profile["ok"] = "true"
        with pytest.raises(ValueError, match="invalid native identity"):
            await source.validate()
        with pytest.raises(ValueError, match="attested workspace"):
            _ = source.capture_cycle_configuration


@pytest.mark.asyncio
async def test_missing_pair_cannot_start_capture():
    with pytest.raises(ValidationError, match="both team and user"):
        SlackConfig(expected_team_id="T1")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: pytest.fail("Unbound capture fetched provider data")
        )
    ) as client:
        source = await SlackSource.create(
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            http_client=client,
            config=SlackConfig(),
        )
        with pytest.raises(ValueError, match="attested workspace"):
            _ = source.capture_cycle_configuration
        with pytest.raises(ValueError, match="attested workspace"):
            await source.capture_page(
                CompletedScope(record_type="channel"), ScanContinuation(), files=MagicMock()
            )
