"""Backend-attested device source identity; no client writer fences or blob storage keys."""

import json
from typing import Literal
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

from airweave.domains.entities.canonical.page_receipts import PageAcknowledgement
from airweave.domains.entities.canonical.requests import BlobReference, ScanVersion, WriterFence

SourceKind = Literal["imessage", "apple_notes", "apple_contacts"]
SOURCE_NAMESPACE = UUID("82134916-5819-4761-818e-e67a59742314")
SYNC_NAMESPACE = UUID("c51644ca-f658-4c39-a91a-03512e5a5e49")
RUN_NAMESPACE = UUID("2b26e9e5-bfc8-49bb-ac76-a55a5bd2d7ca")


class DeviceModel(BaseModel):
    """Strict immutable shaped boundary for the trusted Almanac backend gateway."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class DeviceBinding(DeviceModel):
    """Logical source identity, separate from the current device and its local store."""

    owner_id: str = Field(min_length=1, max_length=255)
    account_id: str = Field(min_length=1, max_length=255)
    source_kind: SourceKind

    @property
    def record_type(self) -> str:
        """Each current reader emits roots; native relationships remain in original payload."""
        return {
            "imessage": "imessage_message",
            "apple_notes": "apple_note",
            "apple_contacts": "apple_contact",
        }[self.source_kind]


def device_source_id(organization_id: UUID, binding: DeviceBinding) -> UUID:
    """Persisted identity includes the backend-attested owner/account/source kind."""
    return uuid5(
        SOURCE_NAMESPACE,
        json.dumps(
            [str(organization_id), binding.owner_id, binding.account_id, binding.source_kind],
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    )


def device_sync_id(source_id: UUID) -> UUID:
    """One canonical sync per immutable logical device source."""
    return uuid5(SYNC_NAMESPACE, str(source_id))


def device_run_id(source_id: UUID, request_key: str) -> UUID:
    """Existing SyncJob primary key is the bounded source-scoped run receipt identity."""
    if not 1 <= len(request_key) <= 128:
        raise ValueError("Device run key must contain 1 to 128 characters")
    return uuid5(RUN_NAMESPACE, json.dumps([str(source_id), request_key], separators=(",", ":")))


class DeviceEnrollment(DeviceModel):
    """Single active publisher; stored in existing SourceConnection configuration."""

    schema_version: Literal[1] = 1
    binding: DeviceBinding
    generation: int = Field(default=0, strict=True, ge=0)
    device_id: UUID | None = None
    store_generation: UUID | None = None
    active: bool = False

    @model_validator(mode="after")
    def coherent_publisher(self) -> "DeviceEnrollment":
        """An active enrollment always identifies a concrete remote publisher generation."""
        if self.active and (
            self.generation < 1 or self.device_id is None or self.store_generation is None
        ):
            raise ValueError("Active device enrollment lacks publisher identity")
        return self


class EnsureDeviceSource(DeviceBinding):
    """Source account identity is attested by the gateway, never inferred from names."""

    collection: str = Field(min_length=1, max_length=255)

    def binding(self) -> DeviceBinding:
        """Exclude the collection placement from immutable logical source identity."""
        return DeviceBinding(
            owner_id=self.owner_id, account_id=self.account_id, source_kind=self.source_kind
        )


class DeviceSourceState(DeviceModel):
    """Enrollment state is not proof of source permission, upload or search publication."""

    source_id: UUID
    sync_id: UUID
    organization_id: UUID
    collection: str
    enrollment: DeviceEnrollment
    retained_read_authority: bool


class BindDevice(DeviceModel):
    """CAS replacement explicitly fences the previous device and every in-flight run."""

    owner_id: str = Field(min_length=1, max_length=255)
    expected_generation: int = Field(strict=True, ge=0)
    device_id: UUID
    store_generation: UUID


class RevokeDevice(DeviceModel):
    """Withdraw publisher enrollment and source read authority, without deleting originals."""

    owner_id: str = Field(min_length=1, max_length=255)
    expected_generation: int = Field(strict=True, ge=0)


class DevicePrincipal(DeviceModel):
    """Gateway attests the signed-in owner/device; device fields alone are not credentials."""

    owner_id: str = Field(min_length=1, max_length=255)
    device_id: UUID
    generation: int = Field(strict=True, ge=1)
    store_generation: UUID


class DeviceBeginRequest(DevicePrincipal):
    """Pin acquisition policy before the first native page; server owns scan identity."""

    acquisition_mode: Literal["delta", "contacts_snapshot"] = "delta"
    replaces_run_id: UUID | None = None


class DeviceUploadIntent(DevicePrincipal):
    """Immutable source observation and attachment selector; never a caller storage key."""

    native_id: str = Field(min_length=1, max_length=1024)
    original: dict[str, JsonValue]
    attachment_index: int = Field(strict=True, ge=0, le=499)
    sha256: str = Field(pattern="^[a-f0-9]{64}$")
    size_bytes: int = Field(strict=True, ge=0, le=8 * 1024 * 1024)
    media_type: str | None = Field(default=None, max_length=255)

    @model_validator(mode="after")
    def bounded_original(self) -> "DeviceUploadIntent":
        """Bound intent metadata independently from the binary upload."""
        if len(self.model_dump_json().encode()) > 2 * 1024 * 1024:
            raise ValueError("Upload intent exceeds 2MiB")
        return self


class DeviceUploadHandle(DeviceModel):
    """Opaque admission status; a handle does not itself grant retained download access."""

    handle: UUID
    sha256: str
    size_bytes: int
    uploaded: bool


class PendingDeviceUpload(DeviceModel):
    """Bounded manifest in the existing job receipt, with no independent database table."""

    handle: UUID
    intent_digest: str
    original_digest: str
    native_id: str
    attachment_index: int
    sha256: str
    size_bytes: int
    media_type: str | None = None
    blob: BlobReference | None = None

    def state(self) -> DeviceUploadHandle:
        """Return no internal key or writer identity."""
        return DeviceUploadHandle(
            handle=self.handle,
            sha256=self.sha256,
            size_bytes=self.size_bytes,
            uploaded=self.blob is not None,
        )


class DeviceObservation(DeviceModel):
    """One native current observation. No wiki version, blob refs, SQL or arbitrary path API."""

    native_id: str = Field(min_length=1, max_length=1024)
    original: dict[str, JsonValue] = Field(default_factory=dict)
    uploads: tuple[UUID, ...] = Field(default=(), max_length=64)
    kind: Literal["upsert", "delete"] = "upsert"
    removal_reason: Literal["provider_deleted", "scope_removed", "access_revoked"] | None = None
    completeness: Literal["complete", "metadata_only", "partial"] = "partial"
    source_created_at: AwareDatetime | None = None
    source_updated_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def explicit_withdrawal(self) -> "DeviceObservation":
        """Incomplete discovery never supplies deletion evidence."""
        if (self.kind == "delete") != (self.removal_reason is not None):
            raise ValueError("Only explicit delete observations require a removal reason")
        return self


class CommitDevicePage(DevicePrincipal):
    """The gateway preserves this complete body unchanged on an uncertain response."""

    page_id: UUID
    expected: ScanVersion
    observations: tuple[DeviceObservation, ...] = Field(max_length=500)
    cursor: dict[str, JsonValue] = Field(default_factory=dict)
    final: bool = False

    @model_validator(mode="after")
    def bounded_page(self) -> "CommitDevicePage":
        """Bound transfer bytes and reject duplicate native identities before writes."""
        if len({item.native_id for item in self.observations}) != len(self.observations):
            raise ValueError("Device page contains duplicate native identities")
        if len(json.dumps(self.cursor, allow_nan=False, ensure_ascii=False).encode()) > 32768:
            raise ValueError("Device cursor exceeds 32KiB")
        if (
            len(
                json.dumps(
                    self.model_dump(mode="json"), allow_nan=False, ensure_ascii=False
                ).encode()
            )
            > 2 * 1024 * 1024
        ):
            raise ValueError("Device page exceeds 2MiB")
        return self


class DeviceCompletedScan(DeviceModel):
    """Immutable terminal scan facts in the existing job receipt, not a second store."""

    version: ScanVersion
    final_page: PageAcknowledgement
    cursor: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded_cursor(self) -> "DeviceCompletedScan":
        """Retain only the already bounded native continuation, never source bodies."""
        if len(json.dumps(self.cursor, ensure_ascii=False, allow_nan=False).encode()) > 65536:
            raise ValueError("Completed cursor exceeds64KiB")
        return self


class DeviceRunReceipt(DeviceModel):
    """Existing SyncJob metadata binds immutable intent to a server-only canonical writer."""

    schema_version: Literal[1] = 1
    source_id: UUID
    request_key: str = Field(min_length=1, max_length=128)
    principal: DevicePrincipal
    fence: WriterFence
    cycle_id: UUID
    uploads: tuple[PendingDeviceUpload, ...] = Field(default=(), max_length=64)
    acquisition_mode: Literal["delta", "contacts_snapshot"] = "delta"
    replaces_run_id: UUID | None = None
    completed_scan: DeviceCompletedScan | None = None


class DeviceRunState(DeviceModel):
    """No writer fence or source contents are exposed to the gateway."""

    run_id: UUID
    source_id: UUID
    principal: DevicePrincipal
    status: str
    acquisition_mode: Literal["delta", "contacts_snapshot"] = "delta"
    final_page_version: ScanVersion | None = None
    version: ScanVersion | None
    phase: Literal["collecting", "reconciling", "complete"] | None
    cursor: dict[str, JsonValue] = Field(default_factory=dict)
    last_page: PageAcknowledgement | None = None
    coverage: Literal["bounded"] = "bounded"
    indexing: Literal["not_verified"] = "not_verified"


class DeviceCaptureAuthority(DeviceModel):
    """Remote binding identity used by the local immutable pending-page journal."""

    binding_id: UUID
    generation: int = Field(strict=True, ge=1)
    local_store_generation: UUID


class DevicePageAck(DeviceModel):
    """Exact bytes accepted under this enrollment plus the shared canonical receipt."""

    authority: DeviceCaptureAuthority
    page_id: UUID
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    acknowledgement: PageAcknowledgement
