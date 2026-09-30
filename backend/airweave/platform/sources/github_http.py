"""GitHub read transport: bounded trusted redirects and strict native pagination."""

from __future__ import annotations

import time
from contextlib import aclosing
from functools import partial
from urllib.parse import parse_qs, urlsplit

import httpx
from pydantic import JsonValue, TypeAdapter
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from airweave.domains.auth_provider.exceptions import AuthProviderRateLimitError
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceError,
    SourceRateLimitError,
    SourceServerError,
)
from airweave.domains.sources.token_providers.credential import DirectCredentialProvider
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.platform.configs.auth import GitHubAuthConfig
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)

BASE_URL = "https://api.github.com"
OBJECT = TypeAdapter(dict[str, JsonValue])
PAGE = TypeAdapter(list[dict[str, JsonValue]])


class GitHubUnavailable(Exception):
    """A non-rate-limited 403/404; never proof of upstream deletion."""


def trusted_url(url: str) -> bool:
    """Never send managed credentials to provider-supplied arbitrary locations."""
    parsed = urlsplit(url)
    return (
        parsed.scheme == "https"
        and parsed.netloc == "api.github.com"
        and not parsed.fragment
        and not parsed.username
        and not parsed.password
    )


class GitHubReader:
    """Existing shared client and auth own network access; this adds GitHub semantics only."""

    def __init__(self, client: AirweaveHttpClient, auth: SourceAuthProvider):
        """Reuse the source-scoped native transport and credential owner."""
        self.client = client
        self.auth = auth

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout | retry_if_exception_type(AuthProviderRateLimitError),
        wait=partial(wait_rate_limit_with_backoff, max_rate_limit_wait=None),
        reraise=True,
    )
    async def get(self, path: str, *, raw: bool = False, redirects: bool = False) -> httpx.Response:
        """Read a native endpoint without exposing provider error bodies in logs."""
        url = BASE_URL + path
        refreshed = False
        for _ in range(4):
            if not trusted_url(url):
                raise SourceError(
                    "GitHub returned an untrusted redirect", source_short_name="github"
                )
            headers = await self._headers(refresh=refreshed, raw=raw)
            response = await self.client.get(url, headers=headers, follow_redirects=False)
            if response.status_code == 401 and self.auth.supports_refresh and not refreshed:
                refreshed = True
                continue
            if response.status_code in {301, 302, 307, 308}:
                if not redirects:
                    raise SourceError(
                        "GitHub captured route changed; refresh its parent inventory",
                        source_short_name="github",
                    )
                url = response.headers.get("Location", "")
                continue
            self._status(response)
            return response
        raise SourceError(
            "GitHub redirect or authentication retry did not resolve", source_short_name="github"
        )

    async def _headers(self, *, refresh: bool = False, raw: bool = False) -> dict[str, str]:
        if isinstance(self.auth, DirectCredentialProvider):
            credentials = GitHubAuthConfig.model_validate(self.auth.credentials)
            headers = {"Authorization": f"Bearer {credentials.personal_access_token}"}
        else:
            headers = await authorization_headers(self.auth, refresh=refresh)
        headers.update(
            {
                "Accept": "application/vnd.github.raw+json"
                if raw
                else "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        return headers

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout | retry_if_exception_type(AuthProviderRateLimitError),
        wait=partial(wait_rate_limit_with_backoff, max_rate_limit_wait=None),
        reraise=True,
    )
    async def blob(self, path: str, *, max_bytes: int) -> bytes:
        """Bound bytes while the existing HTTP transport streams the native blob."""
        for refresh in (False, True):
            headers = await self._headers(refresh=refresh, raw=True)
            headers["Accept-Encoding"] = "identity"
            async with self.client.stream(
                "GET", BASE_URL + path, headers=headers, follow_redirects=False
            ) as response:
                if response.status_code == 401 and not refresh and self.auth.supports_refresh:
                    continue
                if response.status_code != 200:
                    error_body = bytearray()
                    async with aclosing(response.aiter_raw()) as chunks:
                        async for chunk in chunks:
                            error_body.extend(chunk[: 65536 - len(error_body)])
                            if len(error_body) >= 65536:
                                break
                    # Status handling sees only a bounded error envelope, never logs its body.
                    buffered = httpx.Response(
                        response.status_code, headers=response.headers, content=bytes(error_body)
                    )
                    self._status(buffered)
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise ValueError("GitHub ignored the requested identity blob encoding")
                length = response.headers.get("content-length")
                if length is not None and int(length) > max_bytes:
                    raise ValueError("GitHub blob exceeds its retained byte bound")
                content = bytearray()
                async with aclosing(response.aiter_raw()) as chunks:
                    async for chunk in chunks:
                        if len(content) + len(chunk) > max_bytes:
                            raise ValueError("GitHub blob exceeds its retained byte bound")
                        content.extend(chunk)
                return bytes(content)
        raise SourceError(
            "GitHub authorization refresh did not resolve", source_short_name="github"
        )

    def _status(self, response: httpx.Response) -> None:
        status = response.status_code
        if status in {403, 429}:
            message = ""
            if "json" in response.headers.get("content-type", ""):
                value = OBJECT.validate_python(response.json()).get("message")
                message = value.lower() if isinstance(value, str) else ""
            if (
                status == 429
                or "retry-after" in response.headers
                or response.headers.get("x-ratelimit-remaining") == "0"
                or "rate limit" in message
            ):
                try:
                    delay = float(response.headers.get("retry-after", "0"))
                    if delay <= 0:
                        delay = float(response.headers.get("x-ratelimit-reset", "0")) - time.time()
                except ValueError:
                    delay = 60
                raise SourceRateLimitError(source_short_name="github", retry_after=max(60, delay))
        if status == 401:
            raise SourceAuthError(
                "GitHub authorization is unavailable",
                source_short_name="github",
                status_code=401,
                token_provider_kind=self.auth.provider_kind,
            )
        if status in {403, 404}:
            raise GitHubUnavailable("GitHub resource is outside the currently readable scope")
        if status >= 500:
            raise SourceServerError(
                "GitHub is temporarily unavailable", source_short_name="github", status_code=status
            )
        if status != 200:
            raise SourceError(f"GitHub read failed with HTTP {status}", source_short_name="github")

    async def object(self, path: str, *, redirects: bool = False) -> dict[str, JsonValue]:
        """Validate only the JSON boundary, retaining every provider field."""
        return OBJECT.validate_python((await self.get(path, redirects=redirects)).json())

    async def page(
        self, path: str, page: int, *, extra: str = ""
    ) -> tuple[list[dict[str, JsonValue]], bool]:
        """Follow only the exact next page of this fixed endpoint/query."""
        query = f"per_page=100&page={page}" + ("&" + extra if extra else "")
        response = await self.get(path + "?" + query)
        records = PAGE.validate_python(response.json())
        if len(records) > 100:
            raise ValueError("GitHub exceeded its requested page size")
        following = response.links.get("next", {}).get("url")
        if following is None:
            return records, True
        parsed = urlsplit(following)
        expected = parse_qs(query)
        expected["page"] = [str(page + 1)]
        # Redirected repository names are accepted only by root identity resolution;
        # mutable child routes cannot quietly switch their captured parent context.
        if not trusted_url(following) or parsed.path != path or parse_qs(parsed.query) != expected:
            raise ValueError("GitHub returned a non-advancing or out-of-scope page link")
        if not records:
            raise ValueError("GitHub returned an empty page with a continuation")
        return records, False
