"""Exercise the decorated request boundary, not just its exception predicate."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from tenacity import wait_none

from airweave.domains.auth_provider.exceptions import (
    AuthProviderAuthError,
    AuthProviderRateLimitError,
    AuthProviderServerError,
)
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceRateLimitError,
    SourceServerError,
)
from airweave.domains.sources.token_providers.protocol import AuthProviderKind
from airweave.platform.sources.gmail import GmailSource


def request(method, effects):
    upstream = AsyncMock(side_effect=effects)
    source = SimpleNamespace(
        _get_mime_json=upstream,
        _authed_headers=AsyncMock(return_value={}),
        http_client=SimpleNamespace(get=upstream),
        logger=Mock(),
    )
    decorated = getattr(GmailSource, method).retry_with(wait=wait_none())
    return source, upstream, decorated


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get", "_get_capture_json"])
@pytest.mark.parametrize(
    "error",
    [
        SourceServerError(status_code=503),
        AuthProviderServerError(status_code=503),
        AuthProviderRateLimitError(retry_after=60),
    ],
)
async def test_transient_server_error_retries_actual_request(method, error):
    # For _get, return a real response; canonical JSON returns an already parsed dict.
    success = (
        httpx.Response(200, json={"ok": True}, request=httpx.Request("GET", "https://example.test"))
        if method == "_get"
        else {"ok": True}
    )
    source, upstream, call = request(method, [error, success])
    source.short_name = "gmail"
    source.auth = SimpleNamespace(provider_kind=None)
    assert await call(source, "https://example.test") == {"ok": True}
    assert upstream.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get", "_get_capture_json"])
@pytest.mark.parametrize(
    "error",
    [
        SourceServerError(status_code=503),
        AuthProviderServerError(status_code=503),
        AuthProviderRateLimitError(),
    ],
)
async def test_server_error_exhausts_exactly_five_attempts(method, error):
    source, upstream, call = request(method, error)
    with pytest.raises(type(error)) as caught:
        await call(source, "https://example.test")
    assert caught.value is error
    assert upstream.await_count == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get", "_get_capture_json"])
@pytest.mark.parametrize(
    "error",
    [
        SourceAuthError(
            "fixture auth",
            source_short_name="gmail",
            status_code=401,
            token_provider_kind=AuthProviderKind.STATIC,
        ),
        AuthProviderAuthError("fixture proxy authentication"),
        AuthProviderRateLimitError(retry_after=180),
        SourceRateLimitError(retry_after=180),
        httpx.HTTPStatusError(
            "rate limit",
            request=httpx.Request("GET", "https://example.test"),
            response=httpx.Response(429, headers={"Retry-After": "180"}),
        ),
        httpx.HTTPStatusError(
            "unauthorized",
            request=httpx.Request("GET", "https://example.test"),
            response=httpx.Response(401),
        ),
        RuntimeError("budget"),
        asyncio.CancelledError(),
    ],
)
async def test_auth_budget_and_cancellation_do_not_retry(method, error):
    source, upstream, call = request(method, error)
    with pytest.raises(type(error)):
        await call(source, "https://example.test")
    assert upstream.await_count == 1


@pytest.mark.asyncio
async def test_proxy_retry_after_within_bound_is_honored():
    source, upstream, _ = request(
        "_get_capture_json",
        [
            AuthProviderRateLimitError(retry_after=60),
            {"ok": True},
        ],
    )
    sleep = AsyncMock()
    call = GmailSource._get_capture_json.retry_with(sleep=sleep)
    assert await call(source, "https://example.test") == {"ok": True}
    sleep.assert_awaited_once_with(60)
    assert upstream.await_count == 2
