"""Account-bound Composio HTTPX transport; credentials remain with Composio."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable
from contextlib import aclosing

import httpx
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue

from airweave.domains.auth_provider.exceptions import (
    AuthProviderAuthError,
    AuthProviderRateLimitError,
    AuthProviderServerError,
)
from airweave.platform.utils.ssrf import validate_url


class BinaryData(BaseModel):
    """Temporary download supplied by Composio, never a provider credential."""

    model_config = ConfigDict(extra="ignore")
    url: HttpUrl = Field(repr=False)
    content_type: str
    size: int = Field(ge=0)
    expires_at: str


class ProxyResponse(BaseModel):
    """Documented proxy envelope (binary data takes precedence over JSON data)."""

    model_config = ConfigDict(extra="ignore")
    status: int = Field(ge=100, le=599)
    data: JsonValue = None
    headers: dict[str, str] = Field(default_factory=dict)
    binary_data: BinaryData | None = None


class ComposioProxyError(httpx.TransportError):
    """Invalid proxy protocol or disallowed request, without sensitive payloads."""


class DownloadStream(httpx.AsyncByteStream):
    """Own the temporary download response until its consumer closes it."""

    def __init__(self, response: httpx.Response, max_bytes: int):
        """Retain the response and enforce an independent byte ceiling."""
        self.response = response
        self.max_bytes = max_bytes

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Yield bytes while enforcing the configured download bound."""
        count = 0
        async with aclosing(self.response.aiter_bytes()) as chunks:
            async for chunk in chunks:
                count += len(chunk)
                if count > self.max_bytes:
                    raise ComposioProxyError("Proxy download exceeds the file size limit")
                yield chunk

    async def aclose(self) -> None:
        """Close the underlying download connection."""
        await self.response.aclose()


class ComposioTransport(httpx.AsyncBaseTransport):
    """Route only this connection's approved hosts through the fixed proxy API."""

    MAX_BINARY_BYTES = 200 * 1024 * 1024

    def __init__(
        self,
        *,
        api_key: str,
        connected_account_id: str,
        allowed_hosts: Iterable[str],
        client: httpx.AsyncClient | None = None,
        download_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Bind one account; injected clients stay owned by their caller."""
        self._owns_client = client is None
        self._owns_download_transport = download_transport is None
        self._client = client or httpx.AsyncClient(timeout=180.0, follow_redirects=False)
        self._download_transport = download_transport or httpx.AsyncHTTPTransport()
        self._api_key = api_key
        self._account_id = connected_account_id
        self._hosts = frozenset(allowed_hosts)
        if not self._account_id or not self._hosts:
            raise ValueError("A bound account and provider hosts are required")

    async def aclose(self) -> None:
        """Close only clients constructed by this transport."""
        if self._owns_client:
            await self._client.aclose()
        if self._owns_download_transport:
            await self._download_transport.aclose()

    async def _download(self, data: BinaryData) -> httpx.Response:
        if data.size > self.MAX_BINARY_BYTES:
            raise ComposioProxyError("Proxy download exceeds the file size limit")
        url = str(data.url)
        for _ in range(6):
            parsed = httpx.URL(url)
            if parsed.scheme != "https" or parsed.userinfo or parsed.port not in (None, 443):
                raise ComposioProxyError("Invalid proxy download URL")
            validate_url(url)
            # Construct a new request: never inherit client auth, cookies or provider headers.
            request = httpx.Request(
                "GET",
                url,
                extensions={
                    "timeout": {name: 180.0 for name in ("connect", "read", "write", "pool")}
                },
            )
            # Use the public transport interface to avoid HTTPX logging the signed URL.
            # This layer owns redirects and streaming, and sends no ambient credentials.
            try:
                response = await self._download_transport.handle_async_request(request)
            except httpx.RequestError:
                raise ComposioProxyError("Temporary download request failed") from None
            if response.is_redirect:
                location = response.headers.get("location")
                await response.aclose()
                if not location:
                    raise ComposioProxyError("Proxy download redirect lacks location")
                url = str(parsed.join(location))
                continue
            if response.is_error:
                status = response.status_code
                await response.aclose()
                raise ComposioProxyError(f"Temporary download returned HTTP {status}")
            return response
        raise ComposioProxyError("Too many proxy download redirects")

    @staticmethod
    def _check_proxy_response(response: httpx.Response, request: httpx.Request) -> None:
        """Classify proxy failures independently of upstream provider status."""
        if response.status_code in (401, 403):
            raise AuthProviderAuthError(
                "Composio project authentication failed", provider_name="composio"
            )
        if response.status_code == 429:
            try:
                wait = float(response.headers.get("retry-after", "30"))
            except ValueError:
                wait = 30.0
            raise AuthProviderRateLimitError(provider_name="composio", retry_after=wait)
        if response.status_code >= 500:
            raise AuthProviderServerError(
                provider_name="composio", status_code=response.status_code
            )
        if response.status_code == 422:
            try:
                error = response.json().get("error", {})
            except (ValueError, AttributeError):
                error = {}
            if isinstance(error, dict) and error.get("code") == 1706:
                raise AuthProviderAuthError(
                    "Composio connected account requires reauthorization", provider_name="composio"
                )
        if not 200 <= response.status_code < 300:
            raise ComposioProxyError(
                f"Composio proxy returned HTTP {response.status_code}", request=request
            )

    async def _proxy_request(self, payload: dict) -> httpx.Response:
        """Bound encoded attachment JSON before decoding the proxy envelope."""
        limit = ((self.MAX_BINARY_BYTES + 2) // 3) * 4 + 1024 * 1024
        content = bytearray()
        try:
            async with self._client.stream(
                "POST",
                "https://backend.composio.dev/api/v3/tools/execute/proxy",
                headers={"x-api-key": self._api_key},
                json=payload,
            ) as response:
                async with aclosing(response.aiter_bytes()) as chunks:
                    async for chunk in chunks:
                        if len(content) + len(chunk) > limit:
                            raise ComposioProxyError(
                                "Composio response exceeds the file size limit"
                            )
                        content.extend(chunk)
                headers = dict(response.headers)
                for name in ("content-length", "content-encoding", "transfer-encoding"):
                    headers.pop(name, None)
                return httpx.Response(
                    response.status_code,
                    headers=headers,
                    content=bytes(content),
                    request=response.request,
                )
        except ComposioProxyError:
            raise
        except httpx.RequestError:
            raise AuthProviderServerError(
                "Composio proxy request failed", provider_name="composio"
            ) from None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Forward one provider request and preserve JSON or binary semantics."""
        url = request.url
        if (
            url.scheme != "https"
            or url.host not in self._hosts
            or url.port not in (None, 443)
            or url.userinfo
        ):
            raise ComposioProxyError("Provider URL outside the bound source", request=request)
        parameters = [
            {"name": name, "value": value, "type": "header"}
            for name, value in request.headers.multi_items()
            if name.lower()
            not in {"authorization", "host", "cookie", "content-length", "x-api-key"}
        ]
        payload = {
            "connected_account_id": self._account_id,
            "endpoint": str(url),
            "method": request.method,
            "parameters": parameters,
        }
        body = await request.aread()
        if body:
            try:
                payload["body"] = json.loads(body)
            except (ValueError, UnicodeDecodeError) as exc:
                raise ComposioProxyError(
                    "Proxy requires a JSON request body", request=request
                ) from exc
        response = await self._proxy_request(payload)
        self._check_proxy_response(response, request)
        try:
            envelope = ProxyResponse.model_validate(response.json())
        except ValueError:
            raise ComposioProxyError(
                "Invalid Composio response envelope", request=request
            ) from None
        headers = httpx.Headers(envelope.headers)
        for name in ("content-length", "content-encoding", "transfer-encoding", "set-cookie"):
            headers.pop(name, None)
        if envelope.binary_data is not None and 200 <= envelope.status < 300:
            data = envelope.binary_data
            headers["content-type"] = data.content_type
            if request.method == "HEAD":
                headers["content-length"] = str(data.size)
                return httpx.Response(
                    envelope.status, headers=headers, content=b"", request=request
                )
            downloaded = await self._download(data)
            return httpx.Response(
                envelope.status,
                headers=headers,
                stream=DownloadStream(downloaded, self.MAX_BINARY_BYTES),
                request=request,
            )
        if isinstance(envelope.data, str):
            content = envelope.data.encode("utf-8")
        else:
            content = json.dumps(envelope.data, ensure_ascii=False).encode("utf-8")
            headers["content-type"] = "application/json"
        return httpx.Response(envelope.status, headers=headers, content=content, request=request)
