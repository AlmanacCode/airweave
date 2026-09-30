"""Auth result types for auth provider credential fetch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from airweave.domains.sources.token_providers.protocol import (
        ManagedAuthProvider,
        ManagedToolAuthProvider,
    )


@dataclass
class AuthResult:
    """Result of auth provider credential fetch.

    Direct credentials and managed request access are distinct alternatives.
    source_config carries non-secret config fields (e.g., instance_url)
    that the auth provider extracted alongside credentials.
    """

    credentials: Optional[Dict[str, Any]] = None
    source_config: Optional[Dict[str, Any]] = None
    managed_auth: ManagedAuthProvider | ManagedToolAuthProvider | None = None

    @classmethod
    def direct(cls, credentials: Dict[str, Any]) -> "AuthResult":
        """Create an auth result with credentials."""
        return cls(credentials=credentials)
