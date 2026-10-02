"""Versioned native snapshots, distinct from provider observations and originals."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    ScanVersion,
    WriterFence,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation


class NativeModel(BaseModel):
    """Strict immutable boundary for publisher-owned data."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class RecordVersion(NativeModel):
    """Almanac knowledge record revision, independent of capture revision."""

    kind: Literal["record"] = "record"
    revision: int = Field(strict=True, ge=1)


class SessionVersion(NativeModel):
    """Metadata and original-content revisions; replay epochs are not history."""

    kind: Literal["session"] = "session"
    revision: int = Field(strict=True, ge=1)
    content_revision: int = Field(strict=True, ge=0)


NativeVersion = Annotated[RecordVersion | SessionVersion, Field(discriminator="kind")]


class NativeSourceBinding(NativeModel):
    """Server-provisioned owner and dataset; never supplied by an untrusted reader."""

    owner_id: str = Field(min_length=1)
    dataset: Literal["knowledge", "sessions"]


class NativeSnapshotMetadata(NativeModel):
    """Typed native identity and version, independently readable without original bytes."""

    authority: Literal["almanac"] = "almanac"
    representation: Literal["snapshot"] = "snapshot"
    schema_version: Literal[1] = 1
    owner_id: str = Field(min_length=1)
    identity: RecordIdentity
    parent: RecordIdentity | None = None
    version: NativeVersion
    operation: Literal["upsert", "delete"] = "upsert"

    @model_validator(mode="after")
    def identity_contract(self) -> "NativeSnapshotMetadata":
        """Keep version domains and session child identity explicit."""
        if self.identity.record_type == "knowledge":
            if self.version.kind != "record" or self.parent is not None:
                raise ValueError("Knowledge snapshots require record versions and no parent")
        elif self.identity.record_type == "session":
            if self.version.kind != "session" or self.parent is not None:
                raise ValueError("Session snapshots require session versions and no parent")
        elif self.identity.record_type == "message":
            if (
                self.version.kind != "session"
                or self.parent is None
                or self.parent.record_type != "session"
                or self.parent.container_id is not None
                or self.identity.container_id != self.parent.native_id
            ):
                raise ValueError("Original messages require their session parent and version")
        else:
            raise ValueError("Unsupported native snapshot record type")
        if self.identity.record_type != "message" and self.identity.container_id is not None:
            raise ValueError("Native roots have no container")
        return self


class NativeSnapshot(NativeSnapshotMetadata):
    """An attested copy of native JSON, not a second editable authority."""

    original: dict[str, JsonValue]
    source_created_at: AwareDatetime | None = None
    source_updated_at: AwareDatetime | None = None


class IngestNativeBatch(NativeModel):
    """Internal service command; the authenticated edge must issue the writer fence."""

    fence: WriterFence
    observed_at: AwareDatetime
    snapshots: tuple[NativeSnapshot, ...] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def unique_records(self) -> "IngestNativeBatch":
        """One version per identity makes atomic admission unambiguous."""
        keys = [(item.identity.record_type, item.identity.entity_key) for item in self.snapshots]
        if len(set(keys)) != len(keys):
            raise ValueError("A native batch must contain unique record identities")
        return self


class NativeAdmission(NativeModel):
    """Admitted changes and exact retained retries under the current writer lock."""

    records: tuple[CaptureRecord, ...]
    unchanged_ids: tuple[UUID, ...]


class IngestNativePage(IngestNativeBatch):
    """Native originals plus the existing scan CAS; empty final pages are valid."""

    snapshots: tuple[NativeSnapshot, ...] = Field(max_length=500)
    scope: CompletedScope
    cycle_id: UUID
    expected: ScanVersion
    continuation: ScanContinuation
    final: bool = False
