"""Native admission composed with the existing canonical writer transaction."""

from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import CaptureResult
from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord
from airweave.domains.entities.canonical.scan_models import CommitScanPage, ScanResult
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore, content_is_available
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.models import (
    IngestNativeBatch,
    IngestNativePage,
    NativeAdmission,
    NativeSnapshot,
    NativeSourceBinding,
    NativeVersion,
)
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


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


def _validate_message_parent(
    snapshot: NativeSnapshot,
    submitted: dict[tuple[str, str], NativeSnapshot],
    retained: dict[tuple[str, str], Entity],
) -> None:
    """A late transcript page cannot join a newer session snapshot."""
    if snapshot.identity.record_type != "message":
        return
    parent = snapshot.parent
    assert parent is not None  # NativeSnapshot validates message identity before admission.
    key = (parent.record_type, parent.entity_key)
    attested = submitted.get(key)
    if attested is None:
        entity = retained.get(key)
        try:
            attested = NativeSnapshot.model_validate(entity.source_payload if entity else None)
        except ValidationError as error:
            raise NativeAdmissionError("Message requires an attested native session") from error
    if (
        attested.identity != parent
        or attested.owner_id != snapshot.owner_id
        or (snapshot.operation == "upsert" and attested.operation != "upsert")
        or attested.version != snapshot.version
    ):
        raise NativeAdmissionError("Message version must match its attested native session")


def _observation(snapshot: NativeSnapshot, request: IngestNativeBatch) -> CaptureRecord:
    return CaptureRecord(
        identity=snapshot.identity,
        parent=snapshot.parent,
        payload=snapshot.model_dump(mode="json"),
        kind=snapshot.operation,
        removal_reason="provider_deleted" if snapshot.operation == "delete" else None,
        observed_at=request.observed_at,
        source_created_at=snapshot.source_created_at,
        source_updated_at=snapshot.source_updated_at,
    )


def _admit_snapshots(
    snapshots: tuple[NativeSnapshot, ...],
    binding: NativeSourceBinding,
    submitted: dict[tuple[str, str], NativeSnapshot],
    retained: dict[tuple[str, str], Entity],
    request: IngestNativeBatch,
    *,
    sightings: bool,
) -> NativeAdmission:
    """Apply version and membership rules to one preloaded bounded batch."""
    accepted = []
    unchanged = []
    for snapshot in snapshots:
        if sightings and snapshot.operation != "upsert":
            raise NativeAdmissionError("Snapshot pages enumerate active originals, not tombstones")
        dataset = "knowledge" if snapshot.identity.record_type == "knowledge" else "sessions"
        if snapshot.owner_id != binding.owner_id or dataset != binding.dataset:
            raise NativeAdmissionError("Snapshot does not belong to the bound native source")
        entity = retained.get((snapshot.identity.record_type, snapshot.identity.entity_key))
        if entity is not None and not _admit(entity, snapshot):
            if sightings:
                _validate_message_parent(snapshot, submitted, retained)
                if entity.deleted_at is not None:
                    raise NativeAdmissionError("A new sweep requires active original sightings")
            unchanged.append(entity.id)
            continue
        _validate_message_parent(snapshot, submitted, retained)
        accepted.append(_observation(snapshot, request))
    return NativeAdmission(records=tuple(accepted), unchanged_ids=tuple(unchanged))


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
        admitted = await self.admit_locked(db, sync, request)
        result = await self.canonical._capture_locked(
            db, sync, CaptureBatch(fence=request.fence, records=admitted.records)
        )
        return result.model_copy(
            update={"unchanged": result.unchanged + len(admitted.unchanged_ids)}
        )

    async def admit_locked(
        self, db: AsyncSession, sync: Sync, request: IngestNativeBatch, *, sightings: bool = False
    ) -> NativeAdmission:
        """Caller must hold the canonical writer lock through capture and commit."""
        if (
            sync.id != request.fence.sync_id
            or sync.organization_id != request.fence.organization_id
        ):
            raise NativeAdmissionError("Native admission lock belongs to another source")
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
        submitted = {
            (item.identity.record_type, item.identity.entity_key): item
            for item in request.snapshots
        }
        keys = set(submitted)
        keys.update(
            (item.parent.record_type, item.parent.entity_key)
            for item in request.snapshots
            if item.parent is not None
        )
        # The sync writer lock already serializes mutations. Fetch exact identities
        # and parent attestations once rather than adding a round trip per record.
        rows = await db.scalars(
            select(Entity)
            .where(
                Entity.organization_id == request.fence.organization_id,
                Entity.sync_id == sync.id,
                tuple_(Entity.entity_definition_short_name, Entity.entity_id).in_(sorted(keys)),
            )
            .execution_options(populate_existing=True)
        )
        retained = {(row.entity_definition_short_name, row.entity_id): row for row in rows}
        admitted = _admit_snapshots(
            request.snapshots, binding, submitted, retained, request, sightings=sightings
        )
        unchanged = admitted.unchanged_ids
        if sightings and unchanged:
            available = set(
                await db.scalars(
                    select(Entity.id).where(Entity.id.in_(unchanged), content_is_available())
                )
            )
            if available != set(unchanged):
                raise NativeAdmissionError("Withdrawn native originals cannot be marked seen")
        return admitted

    async def page(self, db: AsyncSession, request: IngestNativePage) -> ScanResult:
        """Use canonical scan CAS and one capture callback within the caller's transaction."""
        observations = tuple(_observation(item, request) for item in request.snapshots)

        async def capture_page(
            session: AsyncSession, sync: Sync, batch: CaptureBatch, *, seen_id: UUID
        ) -> CaptureResult:
            # CanonicalScanStore has already validated these exact observations/scope.
            if batch.records != observations or batch.fence != request.fence:
                raise NativeAdmissionError("Native scan capture command changed")
            admitted = await self.admit_locked(session, sync, request, sightings=True)
            result = await self.canonical._capture_locked(
                session,
                sync,
                CaptureBatch(fence=batch.fence, records=admitted.records),
                seen_id=seen_id,
            )
            if admitted.unchanged_ids:
                await session.execute(
                    update(Entity)
                    .where(Entity.id.in_(admitted.unchanged_ids))
                    .values(last_seen_run_id=seen_id)
                )
            return result.model_copy(
                update={"unchanged": result.unchanged + len(admitted.unchanged_ids)}
            )

        return await CanonicalScanStore(self.canonical).page(
            db,
            CommitScanPage(
                fence=request.fence,
                scope=request.scope,
                cycle_id=request.cycle_id,
                expected=request.expected,
                records=observations,
                continuation=request.continuation,
                final=request.final,
            ),
            capture_page=capture_page,
        )
