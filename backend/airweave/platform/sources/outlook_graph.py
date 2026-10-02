"""Outlook's native mailbox boundary over the existing managed/direct transport."""

from contextlib import aclosing
from urllib.parse import unquote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError

from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.protocol import (
    ManagedAuthProvider,
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.composio_transport import ComposioProxyError
from airweave.platform.sources.http_helpers import raise_for_status


class OutlookBoundaryError(SourceError):
    """Unsafe mailbox scope or invalid principal evidence; never a skipped object."""


class OutlookDeltaExpiredError(SourceError):
    """Documented native delta expiration, without retaining private error bodies."""


class GraphError(BaseModel):
    """Only the native error code is interpreted; messages are never exposed."""

    model_config = ConfigDict(extra="ignore", strict=True)
    code: str


class GraphErrorEnvelope(BaseModel):
    """Structured expiry evidence, distinct from transport and authentication errors."""

    model_config = ConfigDict(extra="ignore", strict=True)
    error: GraphError


class GraphPrincipal(BaseModel):
    """Only the native, case-sensitive principal ID establishes mailbox identity."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    id: str = Field(min_length=1, pattern=r"^\S+$")


class OutlookGraphClient:
    """Validate native scope and identity; credential custody stays in the transport."""

    def __init__(
        self,
        auth: SourceAuthProvider,
        http_client: AirweaveHttpClient,
        source_name: str,
        expected_principal_id: str | None,
    ) -> None:
        """Bind the expected principal supplied by the trusted connection owner."""
        self.auth = auth
        self.http_client = http_client
        self.source_name = source_name
        self.expected_principal_id = expected_principal_id
        self.verified_principal_id: str | None = None
        if isinstance(auth, ManagedAuthProvider) and expected_principal_id is None:
            raise self._boundary_error("Managed Outlook requires a trusted expected principal ID")

    def _boundary_error(self, message: str) -> OutlookBoundaryError:
        return OutlookBoundaryError(
            message,
            source_short_name=self.source_name,
        )

    def _validate_url(self, url: str) -> None:
        try:
            parsed = urlsplit(url)
            path = unquote(parsed.path)
            valid = (
                parsed.scheme == "https"
                and parsed.netloc == "graph.microsoft.com"
                and not parsed.fragment
                and (path == "/v1.0/me" or path.startswith("/v1.0/me/"))
                and "\\" not in path
                and not any(segment in (".", "..") for segment in path.split("/"))
                and "%" not in path
                and not any(ord(char) < 32 for char in url)
            )
        except ValueError:
            valid = False
        if not valid:
            raise self._boundary_error("Microsoft Graph URL is outside the bound mailbox")

    async def _request(
        self, url: str, params: dict | None = None, *, immutable_ids: bool = False
    ) -> httpx.Response:
        self._validate_url(url)
        headers = await authorization_headers(self.auth)
        # Derived folder types remain explicit even when the request selects only id.
        headers["Accept"] = "application/json;odata.metadata=minimal"
        if immutable_ids:
            headers["Prefer"] = 'IdType="ImmutableId"'
        try:
            return await self.http_client.get(
                url, headers=headers, params=params, follow_redirects=False
            )
        except ComposioProxyError:
            raise
        except httpx.TimeoutException:
            raise httpx.TimeoutException("Microsoft Graph request timed out") from None
        except httpx.RequestError:
            raise httpx.RequestError("Microsoft Graph request failed") from None

    def _check_response(self, response: httpx.Response) -> None:
        # Native errors may echo tokens or private data. Keep status/retry timing only.
        safe = httpx.Response(
            response.status_code,
            headers={"Retry-After": response.headers["Retry-After"]}
            if "Retry-After" in response.headers
            else {},
            json={},
        )
        raise_for_status(
            safe, source_short_name=self.source_name, token_provider_kind=self.auth.provider_kind
        )

    async def verify_principal(self) -> None:
        """Re-attest on validation/enumeration; a failed check invalidates old evidence."""
        self.verified_principal_id = None
        if self.expected_principal_id is None:
            return  # Legacy direct accounts need not acquire a new User.Read permission.
        response = await self._request("https://graph.microsoft.com/v1.0/me", {"$select": "id"})
        if response.status_code == 401 and self.auth.supports_refresh:
            await authorization_headers(self.auth, refresh=True)
            response = await self._request("https://graph.microsoft.com/v1.0/me", {"$select": "id"})
        self._check_response(response)
        try:
            principal = GraphPrincipal.model_validate(response.json())
        except (ValidationError, ValueError):
            raise self._boundary_error(
                "Microsoft Graph returned an invalid principal identity"
            ) from None
        if principal.id != self.expected_principal_id:
            raise self._boundary_error(
                "Microsoft Graph principal does not match the expected identity"
            )
        self.verified_principal_id = principal.id

    async def get(
        self, url: str, params: dict | None = None, *, immutable_ids: bool = False
    ) -> dict:
        """Preserve legacy IDs while rejecting requests without current attestation."""
        if self.expected_principal_id is not None and (
            self.verified_principal_id != self.expected_principal_id
        ):
            raise self._boundary_error("Microsoft Graph principal must be attested before reading")
        response = await self._request(url, params, immutable_ids=immutable_ids)
        if response.status_code == 401:
            self.verified_principal_id = None
            if self.auth.supports_refresh:
                await authorization_headers(self.auth, refresh=True)
                await self.verify_principal()
                response = await self._request(url, params, immutable_ids=immutable_ids)
        if response.status_code == 401:
            self.verified_principal_id = None
        if urlsplit(url).path.lower().endswith("/messages/delta") and response.status_code in (
            400,
            404,
            410,
        ):
            try:
                error = GraphErrorEnvelope.model_validate(response.json())
            except ValueError:
                error = None
            if error is not None and error.error.code == "syncStateNotFound":
                raise OutlookDeltaExpiredError(
                    "Microsoft Graph delta state expired", source_short_name=self.source_name
                )
        self._check_response(response)
        try:
            data = TypeAdapter(dict[str, JsonValue]).validate_python(response.json())
            if "error" in data:
                raise self._boundary_error("Microsoft Graph returned an error envelope")
            return data
        except ValueError:
            raise self._boundary_error(
                "Microsoft Graph returned an invalid JSON response"
            ) from None

    async def mime_bytes(self, url: str, *, max_bytes: int) -> bytes:
        """Bound original MIME reads; canonical requests alone opt into immutable IDs."""
        if self.expected_principal_id is None or (
            self.verified_principal_id != self.expected_principal_id
        ):
            raise self._boundary_error("Canonical MIME requires an attested principal")
        self._validate_url(url)
        for attempt in range(2):
            headers = await authorization_headers(self.auth)
            headers["Prefer"] = 'IdType="ImmutableId"'
            try:
                async with self.http_client.stream(
                    "GET", url, headers=headers, follow_redirects=False
                ) as response:
                    if response.status_code == 401:
                        self.verified_principal_id = None
                        if not attempt and self.auth.supports_refresh:
                            await authorization_headers(self.auth, refresh=True)
                            await self.verify_principal()
                            continue
                    self._check_response(response)
                    return await self._read_mime_response(response, max_bytes)
            except ComposioProxyError:
                raise
            except httpx.TimeoutException:
                raise httpx.TimeoutException("Microsoft Graph MIME request timed out") from None
            except httpx.RequestError:
                raise httpx.RequestError("Microsoft Graph MIME request failed") from None
        raise self._boundary_error("Microsoft Graph MIME authentication failed")

    async def _read_mime_response(self, response: httpx.Response, max_bytes: int) -> bytes:
        media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media_type not in ("message/rfc822", "text/plain", "application/octet-stream"):
            raise self._boundary_error("Microsoft Graph returned a non-MIME response")
        content = bytearray()
        async with aclosing(response.aiter_bytes()) as chunks:
            async for chunk in chunks:
                if len(content) + len(chunk) > max_bytes:
                    raise FileSkippedException("Original MIME exceeds byte limit", "message")
                content.extend(chunk)
        if not content:
            raise self._boundary_error("Microsoft Graph returned empty MIME")
        return bytes(content)
