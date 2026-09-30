"""Attio native response boundaries and bounded durable page progress."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StrictBool


class AttioModel(BaseModel):
    """Validate consumed fields while preserving raw payloads separately."""

    model_config = ConfigDict(extra="ignore")


class AttioPrincipal(AttioModel):
    """Native token identity; active is the only provider-guaranteed field."""

    active: StrictBool
    workspace_id: UUID | None = None
    token_level: Literal["workspace", "user"] | None = None
    authorized_by_workspace_member_id: UUID | None = None


class AttioIdentity(AttioModel):
    """Native composite IDs, validated by record kind at acquisition."""

    workspace_id: UUID
    object_id: UUID | None = None
    list_id: UUID | None = None
    record_id: UUID | None = None
    entry_id: UUID | None = None
    note_id: UUID | None = None


class AttioNative(AttioModel):
    """Identity and provenance shared by native CRM representations."""

    id: AttioIdentity
    created_at: AwareDatetime | None = None
    parent_record_id: UUID | None = None
    parent_object: str | None = None
    api_slug: str | None = None


class AttioPage(AttioModel):
    """A bounded native page; overflow fails instead of truncating."""

    data: list[dict[str, JsonValue]] = Field(max_length=500)


class AttioProgress(BaseModel):
    """Durable offset plus the previous page IDs for overlap detection."""

    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    offset: int = Field(default=0, ge=0)
    previous_ids: tuple[str, ...] = Field(default=(), max_length=500)
