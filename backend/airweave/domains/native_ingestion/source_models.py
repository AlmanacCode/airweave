"""Immutable native source identity and backend-only ensure/read contracts."""

import json
from uuid import UUID, uuid5

from pydantic import Field

from airweave.domains.native_ingestion.models import NativeModel, NativeSourceBinding

# Versioned protocol constants: changing these would create duplicate source identities.
_SOURCE_NAMESPACE_V1 = UUID("b13e2720-64ab-4ac5-9a0f-6264e2fb783f")
_SYNC_NAMESPACE_V1 = UUID("01b71224-aad4-4a85-914b-a42fc1e8a554")


def native_source_id(organization_id: UUID, binding: NativeSourceBinding) -> UUID:
    """Organization, owner and dataset define identity; collection is immutable configuration."""
    name = json.dumps(
        [str(organization_id), binding.owner_id, binding.dataset],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return uuid5(_SOURCE_NAMESPACE_V1, name)


def native_sync_id(source_id: UUID) -> UUID:
    """One sync owns each native source's retained records and writer fence."""
    return uuid5(_SYNC_NAMESPACE_V1, str(source_id))


class EnsureNativeSource(NativeSourceBinding):
    """The backend attests the owner; no provider or credential selectors are accepted."""

    collection: str = Field(min_length=1, max_length=255)

    def binding(self) -> NativeSourceBinding:
        """Exclude configuration from stable source identity."""
        return NativeSourceBinding(owner_id=self.owner_id, dataset=self.dataset)


class NativeSource(NativeModel):
    """Provisioned identifiers, never proof of completed import or indexed coverage."""

    source_connection_id: UUID
    sync_id: UUID
    organization_id: UUID
    binding: NativeSourceBinding
    collection: str
    available: bool


class WithdrawNativeSource(NativeModel):
    """Backend attests explicit owner loss; there is deliberately no implicit renewal."""

    owner_id: str = Field(min_length=1)
