"""Server-only personal tenant enrollment contract and deterministic retry identities."""

from datetime import timedelta
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr, field_serializer

# Never change this namespace for an existing owner. Stored owner binding remains authority.
OWNED_TENANT_NAMESPACE_V1 = UUID("03041776-d584-56d8-a5f6-1d6c555b4e2c")
SCOPED_KEY_VALIDITY = timedelta(days=90)


class EnsureOwnedTenant(BaseModel):
    """Only Almanac's authenticated backend may supply its current owner's subject."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    owner_user_id: str = Field(min_length=1, max_length=255, pattern=r"^\S+$")
    existing_only: bool = Field(default=False, strict=True)


class OwnedTenantIdentity(BaseModel):
    """Retry identifiers, never an authorization proof."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    organization_id: UUID
    collection_id: UUID
    collection: str
    api_key_id: UUID

    @classmethod
    def for_owner(cls, owner: str) -> "OwnedTenantIdentity":
        """Derive versioned identities while stored binding still controls ownership."""
        organization = uuid5(OWNED_TENANT_NAMESPACE_V1, owner)
        return cls(
            organization_id=organization,
            collection_id=uuid5(organization, "collection:v1"),
            collection="owned-" + organization.hex,
            api_key_id=uuid5(organization, "scoped-api-key:v1"),
        )


class OwnedTenant(BaseModel):
    """Credential appears only in the authorized no-store response, never the repr."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    owner_user_id: str
    organization_id: UUID
    collection: str
    api_key_id: UUID
    api_key: SecretStr
    expires_at: AwareDatetime

    @field_serializer("api_key", when_used="json")
    def credential_for_backend(self, value: SecretStr) -> str:
        """Deliberate private HTTP boundary; normal Python representation stays masked."""
        return value.get_secret_value()


class StoredCredential(BaseModel):
    """Existing encrypted APIKey payload, validated before renewal or return."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    key: SecretStr = Field(min_length=32)
