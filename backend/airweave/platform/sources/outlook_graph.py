"""Outlook's native mailbox boundary over the existing managed/direct transport."""

from urllib.parse import unquote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError

from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.protocol import (
    ManagedAuthProvider,
    SourceAuthProvider,
    authorization_headers,
)
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.composio_transport import ComposioProxyError
from airweave.platform.sources.http_helpers import raise_for_status


class OutlookBoundaryError(SourceError):
    """Unsafe mailbox scope or invalid principal evidence; never a skipped object."""


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

    async def _request(self, url: str, params: dict | None = None) -> httpx.Response:
        self._validate_url(url)
        headers = await authorization_headers(self.auth)
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

    async def get(self, url: str, params: dict | None = None) -> dict:
        """Preserve legacy IDs while rejecting requests without current attestation."""
        if self.expected_principal_id is not None and (
            self.verified_principal_id != self.expected_principal_id
        ):
            raise self._boundary_error("Microsoft Graph principal must be attested before reading")
        response = await self._request(url, params)
        if response.status_code == 401:
            self.verified_principal_id = None
            if self.auth.supports_refresh:
                await authorization_headers(self.auth, refresh=True)
                await self.verify_principal()
                response = await self._request(url, params)
        if response.status_code == 401:
            self.verified_principal_id = None
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
