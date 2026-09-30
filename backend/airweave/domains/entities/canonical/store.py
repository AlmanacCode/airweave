"""SQL persistence for canonical records. Never commits a caller's transaction."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import (
    CaptureResult,
    ChangePage,
    ObservedChange,
    ReconcileResult,
    SourceRecord,
)
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CaptureRecord,
    ReconcileScope,
    RecordIdentity,
    WriterFence,
)
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob


class CanonicalStoreError(Exception):
    """Stable boundary error, translated by the API edge."""

    code = "canonical_store_error"


class SourceNotFound(CanonicalStoreError):
    """No source in the authenticated organization."""

    code = "source_not_found"


class StaleWriter(CanonicalStoreError):
    """A cancelled or superseded source activity cannot write."""

    code = "stale_writer"


class WriterBusy(CanonicalStoreError):
    """Another source run still owns the capture writer."""

    code = "writer_busy"


def capture_fingerprint(record: CaptureRecord) -> str:
    """Observation times never create changes; sparse tombstone payloads do."""
    material = record.model_dump(mode="json", exclude={"observed_at"})
    serialized = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    return hashlib.sha256(serialized.encode()).hexdigest()


def source_record(entity: Entity) -> SourceRecord:
    """Canonical rows only; legacy metadata must be recaptured before reading."""
    return SourceRecord(
        id=entity.id,
        sync_id=entity.sync_id,
        identity=RecordIdentity(
            record_type=entity.entity_definition_short_name,
            native_id=entity.native_id,
            container_id=entity.container_id,
        ),
        revision=entity.record_revision,
        payload=entity.source_payload,
        payload_schema_version=entity.payload_schema_version,
        capture_hash=entity.capture_hash,
        content_hash=entity.content_hash,
        completeness=entity.completeness,
        observed_at=entity.observed_at,
        source_created_at=entity.source_created_at,
        source_updated_at=entity.source_updated_at,
        deleted_at=entity.deleted_at,
        removal_reason=entity.removal_reason,
        blobs=entity.blob_references or (),
        indexed_revision=entity.indexed_revision,
        indexed_pipeline_version=entity.indexed_pipeline_version,
    )


class CanonicalRecordStore:
    """One short Sync row lock serializes commits and allocates journal positions."""

    async def _sync(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, *, lock: bool = False
    ) -> Sync:
        statement = select(Sync).where(Sync.id == sync_id, Sync.organization_id == organization_id)
        if lock:
            statement = statement.with_for_update().execution_options(populate_existing=True)
        sync = (await db.execute(statement)).scalar_one_or_none()
        if sync is None:
            raise SourceNotFound("Source does not exist in this organization")
        return sync

    async def activate_writer(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        job_id: UUID,
        *,
        attempt_id: UUID,
        attempt_number: int,
    ) -> WriterFence:
        """Claim a pending/running job; an identical activation is idempotent."""
        if attempt_number < 1:
            raise ValueError("attempt_number must be positive")
        sync = await self._sync(db, organization_id, sync_id, lock=True)
        job = (
            await db.execute(
                select(SyncJob)
                .where(
                    SyncJob.id == job_id,
                    SyncJob.sync_id == sync_id,
                    SyncJob.organization_id == organization_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if job is None or job.status not in ("pending", "running"):
            raise StaleWriter("Writer must be an active job of this source")
        identical = (
            sync.writer_job_id == job_id
            and sync.writer_attempt_id == attempt_id
            and sync.writer_attempt_number == attempt_number
        )
        if sync.writer_job_id == job_id and not identical:
            if attempt_number <= sync.writer_attempt_number:
                raise StaleWriter("Attempt has been superseded or conflicts with current attempt")
        if sync.writer_job_id != job_id:
            if sync.writer_job_id is not None:
                status = await db.scalar(
                    select(SyncJob.status).where(SyncJob.id == sync.writer_job_id)
                )
                if status in ("pending", "running", "cancelling"):
                    raise WriterBusy("Another source run still owns capture")
        if not identical:
            sync.writer_epoch += 1
            sync.writer_job_id = job_id
            sync.writer_attempt_id = attempt_id
            sync.writer_attempt_number = attempt_number
        await db.flush()
        return WriterFence(
            organization_id=organization_id,
            sync_id=sync_id,
            job_id=job_id,
            epoch=sync.writer_epoch,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
        )

    async def _fenced_sync(self, db: AsyncSession, fence: WriterFence) -> Sync:
        sync = await self._sync(db, fence.organization_id, fence.sync_id, lock=True)
        if (
            sync.writer_epoch != fence.epoch
            or sync.writer_job_id != fence.job_id
            or sync.writer_attempt_id != fence.attempt_id
            or sync.writer_attempt_number != fence.attempt_number
        ):
            raise StaleWriter("Source run has been superseded")
        status = await db.scalar(
            select(SyncJob.status).where(SyncJob.id == fence.job_id).with_for_update()
        )
        if status not in ("pending", "running"):
            raise StaleWriter("Source run is no longer active")
        return sync

    async def capture(self, db: AsyncSession, batch: CaptureBatch) -> CaptureResult:
        """Apply observations and append snapshots under the same writer lock."""
        sync = await self._fenced_sync(db, batch.fence)
        return await self._capture_locked(db, sync, batch)

    async def _capture_locked(
        self, db: AsyncSession, sync: Sync, batch: CaptureBatch
    ) -> CaptureResult:
        changes = []
        unchanged = 0
        for observation in batch.records:
            identity = observation.identity
            entity = (
                await db.execute(
                    select(Entity)
                    .where(
                        Entity.organization_id == batch.fence.organization_id,
                        Entity.sync_id == sync.id,
                        Entity.entity_id == identity.entity_key,
                        Entity.entity_definition_short_name == identity.record_type,
                    )
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            fingerprint = capture_fingerprint(observation)
            if entity is not None and entity.capture_hash == fingerprint:
                entity.last_seen_run_id = batch.fence.attempt_id
                entity.observed_at = observation.observed_at
                unchanged += 1
                continue
            if entity is None:
                entity = Entity(
                    id=uuid4(),
                    organization_id=batch.fence.organization_id,
                    sync_id=sync.id,
                    entity_id=identity.entity_key,
                    entity_definition_short_name=identity.record_type,
                    record_revision=0,
                )
                db.add(entity)
            entity.sync_job_id = batch.fence.job_id
            entity.native_id = identity.native_id
            entity.container_id = identity.container_id
            entity.source_payload = observation.payload
            entity.payload_schema_version = observation.payload_schema_version
            entity.record_revision += 1
            entity.capture_hash = fingerprint
            entity.content_hash = observation.content_hash
            entity.hash = observation.content_hash or fingerprint
            entity.source_created_at = observation.source_created_at
            entity.source_updated_at = observation.source_updated_at
            entity.observed_at = observation.observed_at
            entity.deleted_at = observation.observed_at if observation.kind == "delete" else None
            entity.removal_reason = observation.removal_reason
            entity.completeness = observation.completeness
            entity.blob_references = [blob.model_dump(mode="json") for blob in observation.blobs]
            entity.last_seen_run_id = batch.fence.attempt_id
            entity.projection_error = None
            # Flush before reading default/index state and before another observation of this ID.
            await db.flush()
            sync.observed_change_sequence += 1
            record = source_record(entity)
            change = ObservedChange(
                sequence=sync.observed_change_sequence, kind=observation.kind, record=record
            )
            db.add(
                EntityChange(
                    organization_id=batch.fence.organization_id,
                    sync_id=sync.id,
                    entity_record_id=entity.id,
                    sequence=change.sequence,
                    record_revision=record.revision,
                    kind=observation.kind,
                    snapshot=record.model_dump(mode="json"),
                )
            )
            changes.append(change)
        await db.flush()
        return CaptureResult(
            changes=tuple(changes), sequence=sync.observed_change_sequence, unchanged=unchanged
        )

    async def reconcile_scope(self, db: AsyncSession, request: ReconcileScope) -> ReconcileResult:
        """Tombstone a bounded batch absent from one successfully enumerated scope."""
        sync = await self._fenced_sync(db, request.fence)
        statement = (
            select(Entity)
            .where(
                Entity.organization_id == request.fence.organization_id,
                Entity.sync_id == sync.id,
                Entity.entity_definition_short_name == request.scope.record_type,
                Entity.container_id.is_not_distinct_from(request.scope.container_id),
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                Entity.last_seen_run_id.is_distinct_from(request.fence.attempt_id),
            )
            .order_by(Entity.id)
            .limit(request.limit + 1)
        )
        entities = list((await db.scalars(statement)).all())
        observations = tuple(
            CaptureRecord(
                identity=source_record(entity).identity,
                payload=entity.source_payload,
                payload_schema_version=entity.payload_schema_version,
                kind="delete",
                removal_reason="absent",
                completeness=entity.completeness,
                content_hash=entity.content_hash,
                source_created_at=entity.source_created_at,
                source_updated_at=entity.source_updated_at,
                observed_at=request.observed_at,
                blobs=entity.blob_references or (),
            )
            for entity in entities[: request.limit]
        )
        result = await self._capture_locked(
            db, sync, CaptureBatch(fence=request.fence, records=observations)
        )
        return ReconcileResult(capture=result, has_more=len(entities) > request.limit)

    async def save_checkpoint(
        self, db: AsyncSession, fence: WriterFence, cursor_data: dict
    ) -> None:
        """Call only after capture barrier and successful exact-scope reconciliation."""
        await self._fenced_sync(db, fence)
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == fence.sync_id))
        if cursor is None:
            cursor = SyncCursor(
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
            )
            db.add(cursor)
        cursor.cursor_data = cursor_data
        cursor.last_updated = datetime.now(timezone.utc)
        await db.flush()

    async def read(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, record_id: UUID
    ) -> SourceRecord | None:
        """Return current state including tombstones; never return another tenant's row."""
        entity = await db.scalar(
            select(Entity).where(
                Entity.id == record_id,
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Entity.record_revision > 0,
            )
        )
        return source_record(entity) if entity is not None else None

    async def changes(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        *,
        after: int = 0,
        limit: int = 100,
        high_watermark: int | None = None,
    ) -> ChangePage:
        """Read immutable snapshots, bounded by a committed per-sync watermark."""
        if after < 0 or not 1 <= limit <= 500:
            raise ValueError("Invalid change page bounds")
        sync = await self._sync(db, organization_id, sync_id)
        upper = sync.observed_change_sequence if high_watermark is None else high_watermark
        if upper < after or upper > sync.observed_change_sequence:
            raise ValueError("Invalid change high watermark")
        rows = list(
            (
                await db.scalars(
                    select(EntityChange)
                    .where(
                        EntityChange.organization_id == organization_id,
                        EntityChange.sync_id == sync_id,
                        EntityChange.sequence > after,
                        EntityChange.sequence <= upper,
                    )
                    .order_by(EntityChange.sequence)
                    .limit(limit + 1)
                )
            ).all()
        )
        page = rows[:limit]
        more = len(rows) > limit
        return ChangePage(
            changes=tuple(
                ObservedChange(
                    sequence=row.sequence,
                    kind=row.kind,
                    record=SourceRecord.model_validate(row.snapshot),
                )
                for row in page
            ),
            next_sequence=page[-1].sequence if more else upper,
            high_watermark=upper,
            has_more=more,
        )
