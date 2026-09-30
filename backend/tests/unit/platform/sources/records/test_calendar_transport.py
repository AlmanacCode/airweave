"""Actual decorated native boundary with synthetic HTTP and managed-provider failures."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from airweave.domains.auth_provider.exceptions import (
    AuthProviderRateLimitError,
    AuthProviderServerError,
)
from airweave.domains.entities.canonical.page_source import InvalidScanContinuation
from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.platform.configs.config import GoogleCalendarConfig
from airweave.platform.sources.google_calendar import GoogleCalendarSource


async def source(client, auth=None):
    return await GoogleCalendarSource.create(
        auth=auth or StaticTokenProvider("synthetic"),
        logger=MagicMock(),
        http_client=client,
        config=GoogleCalendarConfig(),
    )


@pytest.mark.parametrize(
    "error", [AuthProviderServerError(status_code=503), AuthProviderRateLimitError(retry_after=60)]
)
async def test_managed_transient_retries_and_honors_delay(error):
    replies = [error, httpx.Response(200, json={"items": []})]
    calls = []

    async def handler(request):
        calls.append(request)
        value = replies.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    sleep = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        instance = await source(client)
        call = GoogleCalendarSource._get_capture_json.retry_with(sleep=sleep)
        assert await call(instance, "https://example.test") == {"items": []}
    assert len(calls) == 2
    if isinstance(error, AuthProviderRateLimitError):
        sleep.assert_awaited_once_with(60)


async def test_long_managed_retry_after_is_deferred_without_early_request():
    calls = []

    async def handler(request):
        calls.append(request)
        raise AuthProviderRateLimitError(retry_after=180)

    sleep = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        instance = await source(client)
        with pytest.raises(AuthProviderRateLimitError):
            await GoogleCalendarSource._get_capture_json.retry_with(sleep=sleep)(
                instance, "https://example.test"
            )
    assert len(calls) == 1
    sleep.assert_not_awaited()


async def test_one_auth_refresh_and_only_explicit_invalid_token_resets():
    class RefreshingToken(StaticTokenProvider):
        refreshes = 0

        @property
        def supports_refresh(self):
            return True

        async def force_refresh(self):
            self.refreshes += 1
            self._token = "refreshed"
            return self._token

    auth = RefreshingToken("initial")
    replies = [
        httpx.Response(401, json={}),
        httpx.Response(200, json={"items": []}),
        httpx.Response(
            400,
            json={
                "error": {
                    "errors": [
                        {"reason": "invalid", "location": "pageToken", "locationType": "parameter"}
                    ]
                }
            },
        ),
        httpx.Response(400, json={"error": {"message": "invalid unrelated parameter"}}),
    ]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: replies.pop(0))
    ) as client:
        instance = await source(client, auth)
        assert await instance._get_capture_json("https://example.test") == {"items": []}
        with pytest.raises(InvalidScanContinuation):
            await instance._get_capture_json("https://example.test", params={"pageToken": "old"})
        with pytest.raises(SourceError):
            await instance._get_capture_json("https://example.test", params={"pageToken": "old"})
    assert auth.refreshes == 1 and not replies


async def test_oversized_native_body_closes_before_full_download():
    class Chunks(httpx.AsyncByteStream):
        observed = 0
        closed = False

        def __aiter__(self):
            self.iterator = self.chunks()
            return self.iterator

        async def chunks(self):
            for _ in range(40):
                self.observed += 1
                yield b"x" * 1024 * 1024

        async def aclose(self):
            self.closed = True
            await self.iterator.aclose()

    chunks = Chunks()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=chunks))
    ) as client:
        instance = await source(client)
        with pytest.raises(FileSkippedException):
            await instance._get_capture_json("https://example.test")
    assert chunks.observed == 33 and chunks.closed
