"""Error classifier for credential/auth failures on source connections.

Maps source exceptions to SourceConnectionErrorCategory values.

Five exception hierarchies can signal user-actionable errors:
1. AuthProviderError       — raised by auth providers (Composio, Pipedream) directly
2. TokenProviderError      — raised by token providers wrapping auth provider errors
3. SourceAuthError         — raised by sources on HTTP 401/403 responses
4. UsageLimitExceededError — raised by the usage limit checker (billing)
5. SourceRateLimitError /
   SourceEntityForbiddenError("rate limit") — upstream rate-limit signals
"""

from __future__ import annotations

from airweave.core.shared_models import SourceConnectionErrorCategory
from airweave.domains.auth_provider.exceptions import (
    AuthProviderAccountNotFoundError,
    AuthProviderAuthError,
    AuthProviderError,
    AuthProviderRateLimitError,
    AuthProviderServerError,
)
from airweave.domains.source_connections.types import ErrorClassification
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceEntityForbiddenError,
    SourceRateLimitError,
    SourceTokenRefreshError,
)
from airweave.domains.sources.token_providers.exceptions import (
    TokenCredentialsInvalidError,
    TokenExpiredError,
    TokenProviderAccountGoneError,
    TokenProviderError,
)
from airweave.domains.sources.token_providers.protocol import AuthProviderKind
from airweave.domains.usage.exceptions import UsageLimitExceededError

# All exception types the classifier can recognise directly or via __cause__
_CLASSIFIABLE = (
    AuthProviderError,
    SourceAuthError,
    SourceTokenRefreshError,
    TokenProviderError,
    UsageLimitExceededError,
    SourceRateLimitError,
    SourceEntityForbiddenError,
)


def classify_error(exc: Exception) -> ErrorClassification:  # noqa: C901
    """Classify an exception into an error category for UI remediation.

    Args:
        exc: The exception that caused the sync failure.

    Returns:
        ErrorClassification with category and message, or empty if not a
        credential error.
    """
    # Unwrap chained exceptions (e.g. SourceValidationError wrapping a classifiable cause)
    if (
        not isinstance(exc, _CLASSIFIABLE)
        and exc.__cause__
        and isinstance(exc.__cause__, _CLASSIFIABLE)
    ):
        return classify_error(exc.__cause__)

    # --- AuthProviderError hierarchy (direct auth provider failures) ---
    if isinstance(exc, AuthProviderAccountNotFoundError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.AUTH_PROVIDER_ACCOUNT_GONE,
            message=str(exc),
        )

    if isinstance(exc, AuthProviderAuthError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.AUTH_PROVIDER_CREDENTIALS_INVALID,
            message=str(exc),
        )

    if isinstance(exc, AuthProviderRateLimitError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.RATE_LIMITED, message=str(exc)
        )
    if isinstance(exc, AuthProviderServerError):
        return ErrorClassification(category=None, message=None)

    # Catch-all for remaining AuthProviderError subtypes (e.g. MissingFieldsError,
    # ConfigError) — these are auth provider issues the user needs to address.
    if isinstance(exc, AuthProviderError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.AUTH_PROVIDER_CREDENTIALS_INVALID,
            message=str(exc),
        )

    # --- Legacy SourceTokenRefreshError ---
    if isinstance(exc, SourceTokenRefreshError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
            message=str(exc),
        )

    # --- Legacy SourceAuthError ---
    if isinstance(exc, SourceAuthError):
        return _classify_by_provider_kind(exc.token_provider_kind, exc)

    # --- TokenProviderError hierarchy ---
    if isinstance(exc, TokenProviderAccountGoneError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.AUTH_PROVIDER_ACCOUNT_GONE,
            message=str(exc),
        )

    if isinstance(exc, TokenExpiredError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
            message=str(exc),
        )

    if isinstance(exc, TokenCredentialsInvalidError):
        return _classify_by_provider_kind(exc.provider_kind, exc)

    # --- UsageLimitExceededError (billing / plan limit) ---
    if isinstance(exc, UsageLimitExceededError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.USAGE_LIMIT_EXCEEDED,
            message=str(exc),
        )

    # --- Rate-limit signals from upstream APIs ---
    # SourceRateLimitError is the canonical 429 case. SourceEntityForbiddenError
    # carries 403 messages, some of which are actually disguised rate limits
    # (notably GitHub, which returns 403 with "API rate limit exceeded").
    if isinstance(exc, SourceRateLimitError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.RATE_LIMITED,
            message=str(exc),
        )

    if isinstance(exc, SourceEntityForbiddenError) and _is_rate_limit_403(str(exc)):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.RATE_LIMITED,
            message=str(exc),
        )

    # Not a classified error — return empty classification
    return ErrorClassification(category=None, message=None)


def _is_rate_limit_403(message: str) -> bool:
    """Detect APIs (notably GitHub) that return 403 for rate-limit responses."""
    lowered = message.lower()
    return "rate limit" in lowered or "abuse detection" in lowered


def _classify_by_provider_kind(
    kind: str,
    exc: Exception,
) -> ErrorClassification:
    """Map a provider_kind string to an error category."""
    if kind in (AuthProviderKind.CREDENTIAL, AuthProviderKind.STATIC):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.API_KEY_INVALID,
            message=str(exc),
        )

    if kind == AuthProviderKind.OAUTH:
        return ErrorClassification(
            category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
            message=str(exc),
        )

    if kind == AuthProviderKind.AUTH_PROVIDER:
        return ErrorClassification(
            category=SourceConnectionErrorCategory.AUTH_PROVIDER_CREDENTIALS_INVALID,
            message=str(exc),
        )

    return ErrorClassification(category=None, message=None)
