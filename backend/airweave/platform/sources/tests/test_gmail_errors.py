"""Native quota responses through both Gmail request boundaries."""

from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from airweave.domains.sources.exceptions import SourceEntityForbiddenError
from airweave.domains.sources.exceptions.classifier import classify_error
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import GmailConfig
from airweave.platform.sources.gmail import GmailSource
from airweave.platform.sources.gmail_errors import GmailThrottleError


def envelope(reason, *, domain="usageLimits", code=403):
    return {"error": {"code": code, "errors": [{"domain": domain, "reason": reason}]}}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get", "_get_capture_json"])
@pytest.mark.parametrize("reason", ["rateLimitExceeded", "userRateLimitExceeded"])
@pytest.mark.parametrize("delay", [None, "3", "180", "invalid", "NaN"])
async def test_native_quota_retry_is_bounded_and_preserves_evidence(method, reason, delay):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(
            403, json=envelope(reason), headers={} if delay is None else {"Retry-After": delay}
        )

    sleep = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = await GmailSource.create(
            auth=StaticTokenProvider("fixture"),
            logger=Mock(),
            http_client=client,
            config=GmailConfig(),
        )
        call = getattr(GmailSource, method).retry_with(sleep=sleep)
        with pytest.raises(GmailThrottleError) as caught:
            await call(source, "https://gmail.googleapis.com/gmail/v1/users/me/profile")
    assert caught.value.status_code == 403
    assert caught.value.retry_after == (float(delay) if delay in {"3", "180"} else None)
    assert calls == (1 if delay == "180" else 5)
    assert sleep.await_count == (0 if delay == "180" else 4)
    if delay == "3":
        assert [call.args[0] for call in sleep.await_args_list] == [3] * 4
    elif delay != "180":
        assert [call.args[0] for call in sleep.await_args_list] == [2, 2, 4, 8]
    assert classify_error(caught.value).category is None


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get", "_get_capture_json"])
@pytest.mark.parametrize(
    "body",
    [
        envelope("domainPolicy", domain="global"),
        envelope("dailyLimitExceeded"),
        envelope("unknown"),
        envelope("rateLimitExceeded", domain="unknown"),
        envelope("rateLimitExceeded", code=400),
        {"error": {"code": 403, "errors": []}},
        {"error": {"code": 403, "errors": [{"reason": "rateLimitExceeded"}]}},
        {
            "error": {
                "code": 403,
                "errors": [
                    {"domain": "usageLimits", "reason": "rateLimitExceeded"},
                    {"domain": "global", "reason": "domainPolicy"},
                ],
            }
        },
        "not json",
        b"{broken",
    ],
)
async def test_permission_unknown_and_malformed_errors_do_not_retry(method, body):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return (
            httpx.Response(403, content=body)
            if isinstance(body, bytes)
            else httpx.Response(403, json=body)
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = await GmailSource.create(
            auth=StaticTokenProvider("fixture"),
            logger=Mock(),
            http_client=client,
            config=GmailConfig(),
        )
        with pytest.raises(SourceEntityForbiddenError):
            await getattr(source, method)("https://gmail.googleapis.com/gmail/v1/users/me/profile")
    assert calls == 1


def test_exhausted_throttle_uses_transient_validation_response():
    from airweave.domains.sources.http_translation import http_exception_for_credential_validation

    error = GmailThrottleError(None)
    response = http_exception_for_credential_validation(error, source_short_name="gmail")
    assert response.status_code == 502
    assert not response.headers
    assert "try again later" in response.detail


@pytest.mark.parametrize("header", ["Wed, 01 Jan 2020 00:00:00 GMT", "-1", "inf"])
def test_retry_timing_does_not_invent_provider_delay(header):
    from airweave.platform.sources.gmail_errors import raise_gmail_throttle

    with pytest.raises(GmailThrottleError) as caught:
        raise_gmail_throttle(
            httpx.Response(403, json=envelope("rateLimitExceeded"), headers={"Retry-After": header})
        )
    assert caught.value.retry_after == (None if header == "inf" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get", "_get_capture_json"])
async def test_quota_then_success_returns_provider_result(method):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return (
            httpx.Response(403, json=envelope("userRateLimitExceeded"))
            if calls == 1
            else httpx.Response(200, json={"historyId": "verified"})
        )

    sleep = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = await GmailSource.create(
            auth=StaticTokenProvider("fixture"),
            logger=Mock(),
            http_client=client,
            config=GmailConfig(),
        )
        call = getattr(GmailSource, method).retry_with(sleep=sleep)
        assert await call(source, "https://gmail.googleapis.com/gmail/v1/users/me/profile") == {
            "historyId": "verified"
        }
    assert calls == 2
    sleep.assert_awaited_once_with(2)


def test_future_http_date_retry_after_uses_actual_delay(monkeypatch):
    from datetime import datetime, timezone

    from airweave.platform.sources import gmail_errors

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 30, 20, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(gmail_errors, "datetime", Clock)
    with pytest.raises(GmailThrottleError) as caught:
        gmail_errors.raise_gmail_throttle(
            httpx.Response(
                403,
                json=envelope("rateLimitExceeded"),
                headers={"Retry-After": "Wed, 30 Sep 2026 20:01:00 GMT"},
            )
        )
    assert caught.value.retry_after == 60
