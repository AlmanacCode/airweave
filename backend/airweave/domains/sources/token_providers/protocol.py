"""Source auth protocols — the contracts sources use to obtain credentials.

Hierarchy::

    SourceAuthProvider (Protocol)          — base: provider_kind + supports_refresh
    ├── TokenProviderProtocol (Protocol)   — get_token() + force_refresh()
    └── DirectCredentialProvider[T]        — .credentials typed property (concrete class)

Token-based implementations:
    - ``OAuthTokenProvider``  — proactive refresh, DB-backed credential store
    - ``StaticTokenProvider`` — raw string (API keys, PATs, validation)
    - ``AuthProviderTokenProvider`` — delegates to Pipedream / Composio

All implementations raise exceptions from
``domains.sources.token_providers.exceptions``.
"""

from enum import Enum
from typing import Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class AuthProviderKind(str, Enum):
    """Discriminator for auth provider type."""

    OAUTH = "oauth"
    STATIC = "static"
    AUTH_PROVIDER = "auth_provider"
    CREDENTIAL = "credential"


@runtime_checkable
class SourceAuthProvider(Protocol):
    """Base auth contract provided to every source at construction.

    Sources narrow the type in their ``create()`` override to declare
    what kind of auth they require:
    - ``auth: TokenProviderProtocol``  — token-based (90% of sources)
    - ``auth: DirectCredentialProvider[MyAuthConfig]`` — structured creds
    """

    @property
    def provider_kind(self) -> AuthProviderKind:
        """The kind of auth this provider supplies."""
        pass

    @property
    def supports_refresh(self) -> bool:
        """Whether ``force_refresh()`` can produce a new token.

        Callers should check this before calling ``force_refresh()``
        to avoid using exceptions for control flow.
        """
        pass


@runtime_checkable
class TokenProviderProtocol(SourceAuthProvider, Protocol):
    """Provides a string auth token with optional refresh capability.

    Sources call ``get_token()`` for a valid token and
    ``force_refresh()`` after a 401 (if ``supports_refresh`` is True).
    """

    async def get_token(self) -> str:
        """Return a valid token, refreshing proactively if stale.

        Raises:
            TokenExpiredError: If the token is known-dead and cannot be refreshed.
        """
        ...

    async def force_refresh(self) -> str:
        """Force an immediate token refresh (e.g. after a 401).

        Check ``supports_refresh`` first. If ``False``, raise
        ``SourceAuthError`` in the source instead of calling this.

        Raises:
            TokenRefreshNotSupportedError: If refresh is not supported.
            TokenCredentialsInvalidError: If refresh token is revoked.
            TokenProviderServerError: If upstream fails during refresh.
        """
        ...


class ManagedAuthProvider(BaseModel):
    """Bound credential custody; deliberately does not expose a bearer token."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    api_key: SecretStr = Field(repr=False)
    connected_account_id: str = Field(min_length=1)
    allowed_hosts: frozenset[str] = Field(min_length=1)
    provider_kind: Literal[AuthProviderKind.AUTH_PROVIDER] = AuthProviderKind.AUTH_PROVIDER
    supports_refresh: Literal[False] = False


class ManagedToolAuthProvider(BaseModel):
    """Explicit account-bound tool sessions for a provider without a REST proxy."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    api_key: SecretStr = Field(repr=False)
    connected_account_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    provider_kind: Literal[AuthProviderKind.AUTH_PROVIDER] = AuthProviderKind.AUTH_PROVIDER
    supports_refresh: Literal[False] = False


async def authorization_headers(
    auth: SourceAuthProvider, *, refresh: bool = False
) -> dict[str, str]:
    """Resolve direct credentials or explicitly defer authentication to transport."""
    if isinstance(auth, ManagedAuthProvider):
        if refresh:
            raise ValueError("Managed authentication refresh belongs to the auth provider")
        return {}
    if not isinstance(auth, TokenProviderProtocol):
        raise TypeError("This request requires token or managed authentication")
    token = await auth.force_refresh() if refresh else await auth.get_token()
    return {"Authorization": f"Bearer {token}"}
