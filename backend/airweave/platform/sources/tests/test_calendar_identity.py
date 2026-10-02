"""Native primary resource attestation gates capture independently of lifecycle wiring."""

from unittest.mock import MagicMock

import httpx
import pytest

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import GoogleCalendarConfig
from airweave.platform.sources.google_calendar import GoogleCalendarSource


@pytest.mark.asyncio
async def test_principal_change_invalidates_attestation_and_capture_identity():
    native_id = "opaque-primary-id"

    def profile(request):
        assert request.url.path.endswith("/calendars/primary")
        return httpx.Response(200, json={"id": native_id})

    async with httpx.AsyncClient(transport=httpx.MockTransport(profile)) as client:
        source = await GoogleCalendarSource.create(
            auth=StaticTokenProvider("same-broker-account"),
            logger=MagicMock(),
            http_client=client,
            config=GoogleCalendarConfig(expected_primary_calendar_id=native_id),
        )
        fingerprint = source.capture_cycle_configuration.fingerprint
        native_id = "different-primary"
        with pytest.raises(ValueError, match="does not match"):
            await source.validate()
        with pytest.raises(ValueError, match="attested primary"):
            await source.capture_page(
                CompletedScope(record_type="calendar"),
                ScanContinuation(value={}),
                files=MagicMock(),
            )
        source.calendar_config = GoogleCalendarConfig(expected_primary_calendar_id=native_id)
        await source.validate()
        assert source.capture_cycle_configuration.fingerprint != fingerprint
        native_id = "private invalid value"
        with pytest.raises(ValueError, match="invalid primary identity") as error:
            await source.validate()
        assert native_id not in str(error.value)


@pytest.mark.asyncio
async def test_unbound_legacy_source_cannot_start_or_resume_owned_capture():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected native call"))
    ) as client:
        source = await GoogleCalendarSource.create(
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            http_client=client,
            config=GoogleCalendarConfig(),
        )
        with pytest.raises(ValueError, match="attested primary"):
            await source.prepare_cycle(None)
        with pytest.raises(ValueError, match="attested primary"):
            await source.capture_page(
                CompletedScope(record_type="calendar"),
                ScanContinuation(value={}),
                files=MagicMock(),
            )


@pytest.mark.parametrize("created", ["0000-12-31T00:00:00.000Z", "2026-10-02T08:17:12.132Z"])
def test_occurrence_creation_time_preserves_native_year_zero(created):
    from airweave.platform.sources.records.google_calendar import record

    payload = {
        "id": "synthetic-occurrence",
        "created": created,
        "updated": "2026-10-02T08:17:12.132Z",
        "start": {"dateTime": "2026-11-01T09:00:00-08:00"},
    }
    captured = record("event_occurrence", payload, "calendar")
    assert captured.payload == payload
    assert (captured.source_created_at is None) == created.startswith("0000")
    assert captured.source_updated_at is not None


def test_unknown_invalid_calendar_creation_time_is_not_silently_discarded():
    from pydantic import ValidationError

    from airweave.platform.sources.records.google_calendar import record

    with pytest.raises(ValidationError):
        record("event_occurrence", {"id": "synthetic", "created": "invalid"}, "calendar")
