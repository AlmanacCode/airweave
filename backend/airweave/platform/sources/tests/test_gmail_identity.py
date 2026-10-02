"""Mailbox expectations belong to the binding, never the observed response."""

from unittest.mock import MagicMock

import httpx
import pytest

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import GmailConfig
from airweave.platform.sources.gmail import GmailSource


@pytest.mark.asyncio
async def test_attestation_and_changed_expectation():
    requests = []
    mailbox = "person@example.test"

    def profile(request):
        requests.append(request)
        return httpx.Response(200, json={"emailAddress": mailbox})

    async with httpx.AsyncClient(transport=httpx.MockTransport(profile)) as client:
        source = await GmailSource.create(
            auth=StaticTokenProvider("same-broker-account"),
            logger=MagicMock(),
            http_client=client,
            config=GmailConfig(expected_mailbox="person@example.test"),
        )
        original = source.capture_cycle_configuration.fingerprint
        assert requests[0].url.params["fields"] == "emailAddress"
        source.capture_config = GmailConfig(expected_mailbox="other@example.test")
        with pytest.raises(ValueError, match="attested mailbox"):
            _ = source.capture_cycle_configuration
        with pytest.raises(ValueError, match="does not match"):
            await source.validate()
        source.capture_config = GmailConfig(expected_mailbox="person@example.test")
        with pytest.raises(ValueError, match="attested mailbox"):
            _ = source.capture_cycle_configuration
        await source.validate()
        assert source.capture_cycle_configuration.fingerprint == original
        # A separately attested identity cannot reuse the old capture fingerprint.
        mailbox = "other@example.test"
        source.capture_config = GmailConfig(expected_mailbox=mailbox)
        await source.validate()
        assert source.capture_cycle_configuration.fingerprint != original
        mailbox = "private malformed value"
        with pytest.raises(ValueError, match="invalid mailbox identity") as error:
            await source.validate()
        assert mailbox not in str(error.value)


@pytest.mark.asyncio
async def test_direct_resume_without_expectation_cannot_fetch():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected provider call"))
    ) as client:
        source = await GmailSource.create(
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            http_client=client,
            config=GmailConfig(),
        )
        with pytest.raises(ValueError, match="attested mailbox"):
            await source.capture_page(
                CompletedScope(record_type="message"),
                ScanContinuation(value={}),
                files=MagicMock(),
            )
