"""Exercise credential custody, provider errors, and actual proxy binary shapes."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from airweave.core.shared_models import SourceConnectionErrorCategory
from airweave.domains.auth_provider.exceptions import (
    AuthProviderAuthError,
    AuthProviderRateLimitError,
    AuthProviderServerError,
)
from airweave.domains.sources.exceptions.classifier import classify_error
from airweave.domains.sources.token_providers.protocol import (
    ManagedAuthProvider,
    authorization_headers,
)
from airweave.platform.http_client.composio_transport import ComposioProxyError, ComposioTransport


@pytest.mark.asyncio
async def test_account_binding_headers_and_repeated_query():
    async def proxy(request):
        data = json.loads(request.content)
        assert data["connected_account_id"] == "ca_bound"
        assert data["endpoint"].endswith("?label=A&label=B")
        assert all(
            x["name"].lower() not in {"authorization", "cookie", "x-api-key"}
            for x in data["parameters"]
        )
        assert all(x["type"] == "header" and "in" not in x for x in data["parameters"])
        assert request.headers["x-api-key"] == "private-key"
        return httpx.Response(
            200,
            json={"status": 429, "data": {"error": "limited"}, "headers": {"retry-after": "17"}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(proxy)) as upstream:
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                api_key="private-key",
                connected_account_id="ca_bound",
                allowed_hosts={"gmail.googleapis.com"},
            )
        ) as client:
            result = await client.get(
                "https://gmail.googleapis.com/gmail/v1/users/me?label=A&label=B",
                headers={"authorization": "must-not-forward", "cookie": "secret"},
            )
            assert result.status_code == 429
            assert result.headers["retry-after"] == "17"
        assert not upstream.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,exception,category",
    [
        (
            401,
            AuthProviderAuthError,
            SourceConnectionErrorCategory.AUTH_PROVIDER_CREDENTIALS_INVALID,
        ),
        (429, AuthProviderRateLimitError, SourceConnectionErrorCategory.RATE_LIMITED),
        (503, AuthProviderServerError, None),
    ],
)
async def test_proxy_error_classification(status, exception, category):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status))
    ) as upstream:
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                api_key="key",
                connected_account_id="ca_bound",
                allowed_hosts={"slack.com"},
            )
        ) as client:
            with pytest.raises(exception) as caught:
                await client.get("https://slack.com/api/auth.test")
            assert classify_error(caught.value).category == category


@pytest.mark.asyncio
async def test_binary_envelope_download_does_not_leak_credentials(monkeypatch, caplog):
    checked = []
    monkeypatch.setattr(
        "airweave.platform.http_client.composio_transport.validate_url", checked.append
    )

    async def proxy(_):
        return httpx.Response(
            200,
            json={
                "status": 200,
                "data": {},
                "binary_data": {
                    "url": "https://files.example/file?signature=secret",
                    "size": 7,
                    "content_type": "application/pdf",
                    "expires_at": "2026-10-01",
                },
                "headers": {"content-encoding": "gzip"},
            },
        )

    async def download(request):
        assert "authorization" not in request.headers
        assert "x-api-key" not in request.headers
        assert "cookie" not in request.headers
        return httpx.Response(200, content=b"%PDF-ok")

    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(proxy)) as upstream,
    ):
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                download_transport=httpx.MockTransport(download),
                api_key="key",
                connected_account_id="ca_bound",
                allowed_hosts={"www.googleapis.com"},
            )
        ) as client:
            result = await client.get("https://www.googleapis.com/drive/v3/files/id/export")
            assert result.content == b"%PDF-ok"
            assert result.headers["content-type"] == "application/pdf"
            assert "content-encoding" not in result.headers
    assert checked == ["https://files.example/file?signature=secret"]
    assert "signature=secret" not in caplog.text


@pytest.mark.asyncio
async def test_reject_cross_provider_before_proxy():
    proxy = AsyncMock()
    async with httpx.AsyncClient(
        transport=ComposioTransport(
            client=proxy,
            api_key="key",
            connected_account_id="ca_bound",
            allowed_hosts={"slack.com"},
        )
    ) as client:
        with pytest.raises(ComposioProxyError):
            await client.get("https://evil.example/api")
    proxy.post.assert_not_called()


@pytest.mark.asyncio
async def test_explicit_managed_auth_has_no_token():
    auth = ManagedAuthProvider(
        api_key="secret", connected_account_id="ca_bound", allowed_hosts=frozenset({"slack.com"})
    )
    assert await authorization_headers(auth) == {}
    assert not hasattr(auth, "get_token")
    assert "secret" not in repr(auth)


@pytest.mark.asyncio
@pytest.mark.parametrize("data", ["plain text", {"items": [1]}])
async def test_plaintext_and_json_envelopes(data):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"status": 200, "data": data})
        )
    ) as upstream:
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                api_key="key",
                connected_account_id="ca_bound",
                allowed_hosts={"www.googleapis.com"},
            )
        ) as client:
            result = await client.get("https://www.googleapis.com/drive/v3/files/id/export")
            assert (result.text if isinstance(data, str) else result.json()) == data


@pytest.mark.asyncio
async def test_download_rejects_private_redirect():
    from airweave.platform.http_client.composio_transport import BinaryData
    from airweave.platform.utils.ssrf import SSRFViolation

    calls = []

    async def download(request):
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://169.254.169.254/latest"})

    async with ComposioTransport(
        api_key="key",
        connected_account_id="ca_bound",
        allowed_hosts={"www.googleapis.com"},
        download_transport=httpx.MockTransport(download),
    ) as transport:
        with pytest.raises(SSRFViolation):
            await transport._download(
                BinaryData(
                    url="https://files.example/file",
                    size=1,
                    content_type="application/pdf",
                    expires_at="2026-10-01",
                )
            )
    assert calls == ["https://files.example/file"]


@pytest.mark.asyncio
async def test_download_stream_enforces_bytes_even_when_size_is_wrong():
    from airweave.platform.http_client.composio_transport import DownloadStream

    response = httpx.Response(200, content=b"12345")
    stream = DownloadStream(response, max_bytes=3)
    try:
        with pytest.raises(ComposioProxyError, match="size limit"):
            async for _ in stream:
                pass
    finally:
        await stream.aclose()
    assert response.is_closed


@pytest.mark.asyncio
async def test_direct_oauth_authentication_still_works():
    from airweave.domains.sources.token_providers.static import StaticTokenProvider

    assert await authorization_headers(StaticTokenProvider("direct")) == {
        "Authorization": "Bearer direct"
    }


@pytest.mark.asyncio
async def test_managed_provider_account_binding(monkeypatch):
    from airweave.domains.auth_provider.exceptions import AuthProviderConfigError
    from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider

    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "key"}, config={"account_id": "ca_bound"}
    )
    fetch = AsyncMock(return_value={"toolkit": {"slug": "gmail"}, "status": "ACTIVE"})
    monkeypatch.setattr(provider, "_get_with_auth", fetch)
    result = await provider.get_auth_result("gmail", ["access_token"])
    assert result.credentials is None
    assert result.managed_auth.connected_account_id == "ca_bound"
    assert result.managed_auth.allowed_hosts == {"gmail.googleapis.com"}
    with pytest.raises(AuthProviderConfigError, match="toolkit"):
        await provider.get_auth_result("google_drive", ["access_token"])


@pytest.mark.asyncio
async def test_expired_account_is_auth_failure_not_generic_422():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                422, json={"error": {"code": 1706, "slug": "TOOL_AUTH_BadConnectedAccountState"}}
            )
        )
    ) as upstream:
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                api_key="key",
                connected_account_id="ca_bound",
                allowed_hosts={"slack.com"},
            )
        ) as client:
            with pytest.raises(AuthProviderAuthError, match="reauthorization"):
                await client.get("https://slack.com/api/auth.test")


@pytest.mark.asyncio
async def test_proxy_envelope_is_bounded_before_json_decode():
    closed = []

    class TooLarge(httpx.AsyncByteStream):
        def __aiter__(self):
            return self

        async def __anext__(self):
            if closed:
                raise StopAsyncIteration
            return b"x" * (1024 * 1024 + 5)

        async def aclose(self):
            closed.append(True)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=TooLarge()))
    ) as upstream:
        transport = ComposioTransport(
            client=upstream,
            api_key="key",
            connected_account_id="ca_bound",
            allowed_hosts={"gmail.googleapis.com"},
        )
        transport.MAX_BINARY_BYTES = 1
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(ComposioProxyError, match="size limit"):
                await client.get("https://gmail.googleapis.com/gmail/v1/users/me/messages/id")
    assert closed
    # Flush HTTPX's nested async iterator finalizers before this test loop closes.
    import asyncio

    await asyncio.get_running_loop().shutdown_asyncgens()
