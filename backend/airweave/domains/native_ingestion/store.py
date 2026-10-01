"""Native admission composed with the existing canonical writer transaction."""

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import CaptureResult
from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.models import (
    IngestNativeBatch,
    NativeSnapshot,
    NativeSourceBinding,
    NativeVersion,
)
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection


def compare_versions(previous: NativeVersion, incoming: NativeVersion) -> int:
    """Partial ordering: reject crossed session revisions instead of guessing."""
    if previous.kind != incoming.kind:
        raise NativeAdmissionError("Native version domain changed")
    old = (previous.revision,)
    new = (incoming.revision,)
    if previous.kind == "session" and incoming.kind == "session":
        old += (previous.content_revision,)
        new += (incoming.content_revision,)
    lower = any(left < right for left, right in zip(new, old, strict=True))
    higher = any(left > right for left, right in zip(new, old, strict=True))
    if lower and higher:
        raise NativeAdmissionError("Native version components are incomparable")
    return -1 if lower else 1 if higher else 0


def _admit(entity: Entity, snapshot: NativeSnapshot) -> bool:
    """Validate retained version state; False acknowledges an exact retained retry."""
    try:
        previous = NativeSnapshot.model_validate(entity.source_payload)
    except ValidationError as error:
        raise NativeAdmissionError("Retained native version state is malformed") from error
    if previous.identity != snapshot.identity or previous.owner_id != snapshot.owner_id:
        raise NativeAdmissionError("Retained native identity does not match its source")
    comparison = compare_versions(previous.version, snapshot.version)
    if comparison < 0:
        raise NativeAdmissionError("Native snapshot version is stale")
    if comparison == 0:
        if previous != snapshot:
            raise NativeAdmissionError("Native snapshot version has conflicting content")
        # A retry acknowledges retention, never restores local visibility.
        return False
    if entity.removal_reason in ("access_revoked", "scope_removed", "absent"):
        raise NativeAdmissionError("Native source visibility requires explicit renewal")
    return True


async def _validate_message_parent(
    db: AsyncSession, request: IngestNativeBatch, snapshot: NativeSnapshot
) -> None:
    """A late transcript page cannot join a newer session snapshot."""
    if snapshot.identity.record_type != "message":
        return
    parent = snapshot.parent
    assert parent is not None  # NativeSnapshot validates message identity before admission.
    attested = next((item for item in request.snapshots if item.identity == parent), None)
    if attested is None:
        payload = await db.scalar(
            select(Entity.source_payload).where(
                Entity.organization_id == request.fence.organization_id,
                Entity.sync_id == request.fence.sync_id,
                Entity.entity_definition_short_name == parent.record_type,
                Entity.entity_id == parent.entity_key,
            )
        )
        try:
            attested = NativeSnapshot.model_validate(payload)
        except ValidationError as error:
            raise NativeAdmissionError("Message requires an attested native session") from error
    if (
        attested.identity != parent
        or attested.owner_id != snapshot.owner_id
        or (snapshot.operation == "upsert" and attested.operation != "upsert")
        or attested.version != snapshot.version
    ):
        raise NativeAdmissionError("Message version must match its attested native session")


class NativeIngestionStore:
    """Compose canonical internals inside the service's single UnitOfWork.

    `_fenced_sync` acquires the existing writer lock and validates the active run.
    `_capture_locked` persists admitted snapshots under that same lock. Neither
    method commits; duplicating either would split version checks from capture.
    """

    def __init__(self, canonical: CanonicalRecordStore):
        """Reuse canonical locking, visibility and revision publication."""
        self.canonical = canonical

    async def ingest(self, db: AsyncSession, request: IngestNativeBatch) -> CaptureResult:
        """Run inside the caller's UnitOfWork; never commit independently."""
        sync = await self.canonical._fenced_sync(db, request.fence)
        source = (
            await db.execute(
                select(SourceConnection)
                .where(
                    SourceConnection.organization_id == request.fence.organization_id,
                    SourceConnection.sync_id == sync.id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if source is None or source.short_name != "almanac":
            raise NativeAdmissionError("Writer is not bound to an Almanac native source")
        try:
            binding = NativeSourceBinding.model_validate(source.config_fields)
        except ValidationError as error:
            raise NativeAdmissionError("Native source binding is malformed") from error
        accepted = []
        unchanged = 0
        for snapshot in request.snapshots:
            dataset = "knowledge" if snapshot.identity.record_type == "knowledge" else "sessions"
            if snapshot.owner_id != binding.owner_id or dataset != binding.dataset:
                raise NativeAdmissionError("Snapshot does not belong to the bound native source")
            entity = await db.scalar(
                select(Entity)
                .where(
                    Entity.organization_id == request.fence.organization_id,
                    Entity.sync_id == sync.id,
                    Entity.entity_definition_short_name == snapshot.identity.record_type,
                    Entity.entity_id == snapshot.identity.entity_key,
                )
                .execution_options(populate_existing=True)
            )
            if entity is not None and not _admit(entity, snapshot):
                unchanged += 1
                continue
            await _validate_message_parent(db, request, snapshot)
            accepted.append(
                CaptureRecord(
                    identity=snapshot.identity,
                    parent=snapshot.parent,
                    payload=snapshot.model_dump(mode="json"),
                    kind=snapshot.operation,
                    removal_reason="provider_deleted" if snapshot.operation == "delete" else None,
                    observed_at=request.observed_at,
                    source_created_at=snapshot.source_created_at,
                    source_updated_at=snapshot.source_updated_at,
                )
            )
        result = await self.canonical._capture_locked(
            db, sync, CaptureBatch(fence=request.fence, records=tuple(accepted))
        )
        return result.model_copy(update={"unchanged": result.unchanged + unchanged})
