"""Immutable upload manifests share the device job and its canonical writer fence."""

import hashlib
import json
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.device_ingestion.models import (
    DeviceObservation,
    DevicePrincipal,
    DeviceRunReceipt,
    DeviceUploadHandle,
    DeviceUploadIntent,
    PendingDeviceUpload,
)
from airweave.domains.device_ingestion.store import (
    DeviceAdmissionError,
    DeviceIngestionStore,
    LockedDeviceSource,
)
from airweave.domains.entities.canonical.apple_payloads import (
    NativeMessage,
    NativeNote,
    validate_device_original,
)
from airweave.domains.entities.canonical.page_receipts import page_digest
from airweave.domains.entities.canonical.requests import BlobReference
from airweave.models.sync_job import SyncJob


def original_digest(original: dict[str, JsonValue]) -> str:
    """Compare original JSON values without modifying their retained representation."""
    return hashlib.sha256(
        json.dumps(
            original, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


async def active_run(
    store: DeviceIngestionStore,
    db: AsyncSession,
    organization_id: UUID,
    source_id: UUID,
    request_key: str,
    publisher: DevicePrincipal,
) -> tuple[LockedDeviceSource, SyncJob, DeviceRunReceipt]:
    """Admission is serialized with revoke and independently fences a replaced writer."""
    bound = await store.require(db, organization_id, source_id, publisher.owner_id)
    store.attest(bound, publisher)
    job, saved = await store.load_run(db, bound, request_key)
    if saved.principal != publisher or job.status != "running":
        raise DeviceAdmissionError("Upload belongs to an inactive run")
    await store.canonical._fenced_sync(db, saved.fence)
    return bound, job, saved


async def declare(
    store: DeviceIngestionStore,
    db: AsyncSession,
    organization_id: UUID,
    source_id: UUID,
    request_key: str,
    handle: UUID,
    intent: DeviceUploadIntent,
) -> DeviceUploadHandle:
    """Validate attachment membership before admitting immutable upload intent."""
    publisher = DevicePrincipal.model_validate(
        intent.model_dump(include={"owner_id", "device_id", "generation", "store_generation"})
    )
    bound, job, saved = await active_run(
        store, db, organization_id, source_id, request_key, publisher
    )
    parsed = validate_device_original(bound.enrollment.binding.source_kind, intent.original)
    if parsed.native_id != intent.native_id or not isinstance(parsed, (NativeMessage, NativeNote)):
        raise DeviceAdmissionError("Upload must identify a native message or note")
    if isinstance(parsed, NativeNote) and (parsed.locked or parsed.marked_for_deletion):
        raise DeviceAdmissionError("Withdrawn note cannot admit attachment bytes")
    if intent.attachment_index >= len(parsed.attachments):
        raise DeviceAdmissionError("Upload attachment is absent from the native original")
    digest = page_digest(intent)
    existing = next((item for item in saved.uploads if item.handle == handle), None)
    if existing is not None:
        if existing.intent_digest != digest:
            raise DeviceAdmissionError("Upload handle has conflicting intent")
        return existing.state()
    if (
        len(saved.uploads) >= 64
        or sum(item.size_bytes for item in saved.uploads) + intent.size_bytes > 64 * 1024 * 1024
    ):
        raise DeviceAdmissionError("Device run upload budget exhausted")
    pending = PendingDeviceUpload(
        handle=handle,
        intent_digest=digest,
        original_digest=original_digest(intent.original),
        native_id=intent.native_id,
        attachment_index=intent.attachment_index,
        sha256=intent.sha256,
        size_bytes=intent.size_bytes,
        media_type=intent.media_type,
    )
    job.sync_metadata = saved.model_copy(update={"uploads": (*saved.uploads, pending)}).model_dump(
        mode="json"
    )
    await db.flush()
    return pending.state()


def resolve_uploads(
    item: DeviceObservation, uploads: tuple[PendingDeviceUpload, ...]
) -> tuple[BlobReference, ...]:
    """Only uploaded handles belonging to this unchanged native observation may be committed."""
    if len(set(item.uploads)) != len(item.uploads) or (item.kind == "delete" and item.uploads):
        raise DeviceAdmissionError("Invalid observation upload handles")
    refs = []
    indexes = set()
    for handle in item.uploads:
        pending = next((entry for entry in uploads if entry.handle == handle), None)
        if (
            pending is None
            or pending.blob is None
            or pending.native_id != item.native_id
            or pending.original_digest != original_digest(item.original)
        ):
            raise DeviceAdmissionError("Upload handle does not belong to this observation")
        if pending.attachment_index in indexes:
            raise DeviceAdmissionError("Attachment has duplicate upload handles")
        indexes.add(pending.attachment_index)
        refs.append(
            pending.blob.model_copy(
                update={"source_path": f"/original/attachments/{pending.attachment_index}"}
            )
        )
    return tuple(refs)
