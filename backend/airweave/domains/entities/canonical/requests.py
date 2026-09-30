"""Validated source capture commands. Provider payloads remain opaque JSON."""

import hashlib
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

ScopeRemovalReason = Literal["scope_removed", "access_revoked"]
RecordKind = Annotated[str, Field(min_length=1, max_length=200)]


class ScanVersion(BaseModel):
    """Exact durable scope version observed before provider I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sweep_id: UUID
    revision: int = Field(ge=1)


class RecordIdentity(BaseModel):
    """Native identity inside one sync, including provider-required container."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_type: RecordKind
    native_id: str = Field(min_length=1)
    container_id: str | None = None

    @property
    def entity_key(self) -> str:
        """Unambiguous encoding; native IDs remain separately available for actions."""
        return json.dumps(
            [self.container_id, self.native_id], ensure_ascii=False, separators=(",", ":")
        )


def parent_container_key(parent: RecordIdentity) -> str:
    """Optional source identity policy: bounded full-parent locator, stable across rebuilds.

    New connectors choosing this policy use it for every child, including root-owned
    children. Native parent identity remains in CaptureRecord.parent and native JSON.
    Existing connectors may retain their audited provider-global container IDs.
    """
    material = json.dumps(
        parent.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return "parent:" + hashlib.sha256(material.encode()).hexdigest()


class BlobReference(BaseModel):
    """An already-written immutable blob required by this record."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    key: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(ge=0)
    media_type: str | None = None
    source_path: str | None = Field(
        default=None, description="RFC 6901 JSON Pointer into the unchanged native payload"
    )


class CaptureRecord(BaseModel):
    """One current provider observation; sparse deletions are valid records."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    identity: RecordIdentity
    parent: RecordIdentity | None = None
    allow_reparent: bool = Field(
        default=False,
        description="Audited provider evidence that this same object moved; never a retry override",
    )
    payload: dict[str, JsonValue]
    payload_schema_version: int = Field(default=1, ge=1)
    kind: Literal["upsert", "delete"] = "upsert"
    removal_reason: (
        Literal["provider_deleted", "scope_removed", "access_revoked", "absent"] | None
    ) = None
    completeness: Literal["complete", "metadata_only", "partial"] = "complete"
    content_hash: str | None = None
    source_created_at: AwareDatetime | None = None
    source_updated_at: AwareDatetime | None = None
    observed_at: AwareDatetime
    blobs: tuple[BlobReference, ...] = ()

    @model_validator(mode="after")
    def validate_removal(self) -> "CaptureRecord":
        """Keep provider deletion distinct from inaccessible or deselected scope."""
        if self.kind == "delete" and self.removal_reason is None:
            raise ValueError("delete observations require removal_reason")
        if self.kind == "upsert" and self.removal_reason is not None:
            raise ValueError("upsert observations cannot have removal_reason")
        return self


class WriterFence(BaseModel):
    """Only the current source run may persist records or its checkpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    organization_id: UUID
    sync_id: UUID
    job_id: UUID
    epoch: int = Field(ge=1)
    attempt_id: UUID
    attempt_number: int = Field(ge=1)


class CaptureBatch(BaseModel):
    """A bounded atomic capture batch; order within the batch is preserved."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    records: tuple[CaptureRecord, ...] = Field(max_length=500)


class CompletedScope(BaseModel):
    """Evidence for one exact type/container and, for nested pages, its full owner."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_type: RecordKind
    container_id: str | None = None
    parent: RecordIdentity | None = None


class ReconcileScope(BaseModel):
    """Only call after successful complete enumeration, never partial failure."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    scope: CompletedScope
    removal_reason: Literal["absent", "scope_removed"] = "absent"
    observed_at: AwareDatetime
    limit: int = Field(default=250, ge=1, le=500)


class StartedScope(CompletedScope):
    """Start/restart exact-scope full enumeration, discarding earlier attempt sightings."""


class RemovedScope(CompletedScope):
    """Provider-confirmed scope loss; applies to children even if seen in this run."""

    removal_reason: ScopeRemovalReason
    observed_at: AwareDatetime
