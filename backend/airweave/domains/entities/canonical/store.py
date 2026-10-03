"""SQL persistence for canonical records. Never commits a caller's transaction."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, any_, exists, func, literal, not_, or_, select, update
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from airweave.domains.entities.canonical.checkpoint import CanonicalCheckpoint
from airweave.domains.entities.canonical.cycle_models import CYCLE_KEY
from airweave.domains.entities.canonical.mail_facts_v1 import gmail_metadata_v1
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
    RemovedScope,
    StartedScope,
    WriterFence,
)
from airweave.domains.entities.canonical.wispr_facts_v1 import meeting_started_at_v1
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob


def _derive_query_facts(entity: Entity, observation: CaptureRecord, provider: str | None) -> None:
    """Rebuild source-owned query facts alongside this exact canonical revision."""
    if provider == "gmail" and observation.identity.record_type == "message":
        facts = gmail_metadata_v1(
            observation.payload, observation.identity.native_id, observation.source_created_at
        )
        entity.gmail_metadata = facts.model_dump(mode="json") if facts is not None else None
        entity.gmail_metadata_revision = entity.record_revision
    if provider == "wispr" and observation.identity.record_type == "meeting":
        entity.meeting_started_at = meeting_started_at_v1(
            observation.payload, observation.identity.native_id
        )


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


def _require_current_generation(sync: Sync, job: SyncJob) -> None:
    """A prior credential or unverified connection cannot acquire or use a writer."""
    generation = sync.provisioning_generation
    if job.provisioning_generation != generation or (
        generation > 0
        and (sync.provisioning_ready_generation != generation or sync.status != "active")
    ):
        raise StaleWriter("Source connection generation is not admitted for capture")


def capture_fingerprint(record: CaptureRecord) -> str:
    """Observation times never create changes; sparse tombstone payloads do."""
    material = record.model_dump(
        mode="json", exclude={"observed_at", "allow_reparent", "descendant_visibility_fields"}
    )
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
        parent=(
            RecordIdentity(
                record_type=entity.parent_record_type,
                native_id=entity.parent_native_id,
                container_id=entity.parent_container_id,
            )
            if entity.parent_record_type is not None
            else None
        ),
        revision=entity.record_revision,
        payload=entity.source_payload,
        payload_schema_version=entity.payload_schema_version,
        capture_hash=entity.capture_hash,
        content_hash=entity.content_hash,
        completeness=entity.completeness,
        observed_at=entity.observed_at,
        first_observed_at=entity.first_observed_at,
        revision_observed_at=entity.revision_observed_at,
        first_stored_at=entity.first_stored_at,
        source_created_at=entity.source_created_at,
        source_updated_at=entity.source_updated_at,
        deleted_at=entity.deleted_at,
        removal_reason=entity.removal_reason,
        blobs=entity.blob_references or (),
        indexed_revision=entity.indexed_revision,
        indexed_pipeline_version=entity.indexed_pipeline_version,
    )


def ancestor_chain(subject=Entity):
    """Finite scoped ancestor walk; repeated UUIDs stop corrupt record cycles."""
    parent = aliased(Entity)
    available = and_(
        parent.record_revision > 0,
        parent.deleted_at.is_(None),
        or_(
            parent.removal_reason.is_(None),
            parent.removal_reason.not_in(("scope_removed", "access_revoked")),
        ),
    )
    chain = (
        select(
            parent.id,
            parent.organization_id,
            parent.sync_id,
            parent.parent_record_type,
            parent.parent_native_id,
            parent.parent_container_id,
            parent.parent_visibility_epoch,
            parent.removal_reason,
            array([parent.id]).label("path"),
            literal(False).label("cycle"),
            and_(available, parent.visibility_epoch == subject.parent_visibility_epoch).label(
                "valid"
            ),
        )
        .where(
            parent.organization_id == subject.organization_id,
            parent.sync_id == subject.sync_id,
            parent.entity_definition_short_name == subject.parent_record_type,
            parent.native_id == subject.parent_native_id,
            parent.container_id.is_not_distinct_from(subject.parent_container_id),
        )
        .correlate(subject)
        .cte(recursive=True, nesting=True)
    )
    return chain.union_all(
        select(
            parent.id,
            parent.organization_id,
            parent.sync_id,
            parent.parent_record_type,
            parent.parent_native_id,
            parent.parent_container_id,
            parent.parent_visibility_epoch,
            parent.removal_reason,
            chain.c.path + array([parent.id]),
            (parent.id == any_(chain.c.path)).label("cycle"),
            and_(
                chain.c.valid, available, parent.visibility_epoch == chain.c.parent_visibility_epoch
            ),
        )
        .join(
            chain,
            and_(
                parent.organization_id == chain.c.organization_id,
                parent.sync_id == chain.c.sync_id,
                parent.entity_definition_short_name == chain.c.parent_record_type,
                parent.native_id == chain.c.parent_native_id,
                parent.container_id.is_not_distinct_from(chain.c.parent_container_id),
            ),
        )
        .where(not_(chain.c.cycle))
    )


def active_parent_exists():
    """All parent attestations must reach an available root without a record cycle."""
    chain = ancestor_chain()
    return exists(
        select(chain.c.id).where(
            chain.c.parent_record_type.is_(None), chain.c.valid, not_(chain.c.cycle)
        )
    ).correlate(Entity)


def parent_is_visible():
    """Deny children of missing/deleted containers immediately, before cleanup catches up."""
    return or_(Entity.parent_record_type.is_(None), active_parent_exists())


def content_is_available():
    """Shared SQL gate for read, historical payload access and search postvalidation."""
    return and_(
        parent_is_visible(),
        or_(
            Entity.removal_reason.is_(None),
            Entity.removal_reason.not_in(("scope_removed", "access_revoked")),
        ),
    )


def with_content_access(record: SourceRecord, available: bool) -> SourceRecord:
    """Retain citation identity/version while explicitly withholding revoked content."""
    if available:
        return record
    return record.model_copy(update={"content_access": "unavailable", "payload": {}, "blobs": ()})


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

    async def admit_job(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, job_id: UUID
    ) -> int:
        """Read one current job/source generation before external source construction.

        This is an admission snapshot, not a replacement for the locked write fence.
        Callers compare it again after construction; every write rechecks independently.
        """
        row = (
            await db.execute(
                select(Sync, SyncJob)
                .join(SyncJob, SyncJob.sync_id == Sync.id)
                .where(
                    Sync.id == sync_id,
                    Sync.organization_id == organization_id,
                    SyncJob.id == job_id,
                    SyncJob.organization_id == organization_id,
                )
                .execution_options(populate_existing=True)
            )
        ).one_or_none()
        if row is None:
            raise StaleWriter("Source job is unavailable for capture")
        sync, job = row
        if job.status not in ("pending", "running"):
            raise StaleWriter("Source run is no longer active")
        _require_current_generation(sync, job)
        return sync.provisioning_generation

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
        _require_current_generation(sync, job)
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
        job = await db.scalar(
            select(SyncJob)
            .where(
                SyncJob.id == fence.job_id,
                SyncJob.sync_id == fence.sync_id,
                SyncJob.organization_id == fence.organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if job is None or job.status not in ("pending", "running"):
            raise StaleWriter("Source run is no longer active")
        _require_current_generation(sync, job)
        return sync

    async def capture(self, db: AsyncSession, batch: CaptureBatch) -> CaptureResult:
        """Apply observations and append snapshots under the same writer lock."""
        sync = await self._fenced_sync(db, batch.fence)
        return await self._capture_locked(db, sync, batch)

    async def _parent_attestation(
        self, db: AsyncSession, sync: Sync, observation: CaptureRecord, entity: Entity | None
    ) -> int | None:
        """Resolve provider parent identity only inside the source writer transaction."""
        if observation.parent is None:
            return None
        parent_identity = observation.parent
        if observation.identity == parent_identity:
            raise CanonicalStoreError("A record cannot parent itself")
        parent = await db.scalar(
            select(Entity).where(
                Entity.organization_id == sync.organization_id,
                Entity.sync_id == sync.id,
                Entity.entity_definition_short_name == parent_identity.record_type,
                Entity.entity_id == parent_identity.entity_key,
                Entity.record_revision > 0,
            )
        )
        if observation.kind == "delete" and parent is None:
            return None
        if parent is None:
            raise CanonicalStoreError("Capture parent must exist before its child")
        if entity is not None:
            chain = ancestor_chain()
            contains_child = exists(select(chain.c.id).where(chain.c.id == entity.id)).correlate(
                Entity
            )
            cycles = await db.scalar(select(contains_child).where(Entity.id == parent.id))
            if cycles:
                raise CanonicalStoreError("Record parent would create a cycle")
        if observation.kind == "delete" and entity is not None:
            return entity.parent_visibility_epoch
        available = await db.scalar(select(content_is_available()).where(Entity.id == parent.id))
        if parent.deleted_at is not None or not available:
            if observation.kind == "delete":
                return None
            raise CanonicalStoreError("Capture parent is unavailable")
        return parent.visibility_epoch

    async def _capture_locked(
        self,
        db: AsyncSession,
        sync: Sync,
        batch: CaptureBatch,
        *,
        seen_id: UUID | None = None,
        mark_seen: bool = True,
    ) -> CaptureResult:
        changes = []
        unchanged = 0
        provider = await db.scalar(
            select(SourceConnection.short_name)
            .where(
                SourceConnection.sync_id == sync.id,
                SourceConnection.organization_id == sync.organization_id,
            )
            .limit(1)
        )
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
            if entity is not None and observation.kind == "delete" and observation.parent is None:
                # Native deletion feeds may contain only an object ID. Missing parent data
                # is not evidence that the object moved out of its known authorization chain.
                observation = observation.model_copy(
                    update={"parent": source_record(entity).parent}
                )
            parent_epoch = await self._parent_attestation(db, sync, observation, entity)
            fingerprint = capture_fingerprint(observation)
            was_available = (
                (await db.scalar(select(content_is_available()).where(Entity.id == entity.id)))
                if entity is not None
                else True
            )
            was_deleted = entity is not None and entity.deleted_at is not None
            parent_changed = entity is not None and (
                (entity.parent_record_type, entity.parent_native_id, entity.parent_container_id)
                != (
                    (
                        observation.parent.record_type,
                        observation.parent.native_id,
                        observation.parent.container_id,
                    )
                    if observation.parent
                    else (None, None, None)
                )
            )
            if parent_changed and not observation.allow_reparent:
                raise CanonicalStoreError(
                    "Existing identity belongs to a different parent; "
                    "explicit provider move required"
                )
            attestation_changed = (
                entity is not None and entity.parent_visibility_epoch != parent_epoch
            )
            access_changed = (
                entity is not None
                and bool(observation.descendant_visibility_fields)
                and (
                    entity.source_payload is None
                    or any(
                        (key in entity.source_payload, entity.source_payload.get(key))
                        != (key in observation.payload, observation.payload.get(key))
                        for key in observation.descendant_visibility_fields
                    )
                )
            )
            if (
                entity is not None
                and entity.capture_hash == fingerprint
                and not attestation_changed
                and not access_changed
                and not (observation.kind == "upsert" and not was_available)
            ):
                if mark_seen:
                    entity.last_seen_run_id = seen_id or batch.fence.attempt_id
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
                    visibility_epoch=1,
                )
                db.add(entity)
            if observation.kind == "upsert" and (
                was_deleted or not was_available or parent_changed or access_changed
            ):
                entity.visibility_epoch += 1
            entity.parent_visibility_epoch = parent_epoch
            entity.sync_job_id = batch.fence.job_id
            entity.native_id = identity.native_id
            entity.container_id = identity.container_id
            entity.parent_record_type = (
                observation.parent.record_type if observation.parent else None
            )
            entity.parent_native_id = observation.parent.native_id if observation.parent else None
            entity.parent_container_id = (
                observation.parent.container_id if observation.parent else None
            )
            entity.source_payload = observation.payload
            entity.payload_schema_version = observation.payload_schema_version
            entity.first_observed_at = (
                observation.observed_at if entity.record_revision == 0 else entity.first_observed_at
            )
            entity.revision_observed_at = observation.observed_at
            entity.record_revision += 1
            entity.capture_hash = fingerprint
            entity.content_hash = observation.content_hash
            entity.hash = observation.content_hash or fingerprint
            entity.source_created_at = observation.source_created_at
            _derive_query_facts(entity, observation, provider)
            entity.source_updated_at = observation.source_updated_at
            entity.observed_at = observation.observed_at
            entity.deleted_at = observation.observed_at if observation.kind == "delete" else None
            entity.removal_reason = observation.removal_reason
            entity.completeness = observation.completeness
            entity.blob_references = [blob.model_dump(mode="json") for blob in observation.blobs]
            if mark_seen:
                entity.last_seen_run_id = seen_id or batch.fence.attempt_id
            entity.projection_error = None
            # Flush before reading default/index state and before another observation of this ID.
            await db.flush()
            sync.observed_change_sequence += 1
            record = source_record(entity)
            change = ObservedChange(
                sequence=sync.observed_change_sequence, kind=observation.kind, record=record
            )
            journal = EntityChange(
                organization_id=batch.fence.organization_id,
                sync_id=sync.id,
                entity_record_id=entity.id,
                sequence=change.sequence,
                record_revision=record.revision,
                kind=observation.kind,
                snapshot=record.model_dump(mode="json"),
                created_at=func.timezone("UTC", func.transaction_timestamp()),
            )
            db.add(journal)
            if record.revision == 1:
                # The journal's DB transaction time is authoritative, not the
                # legacy Entity/Python clock or the collector's observation time.
                await db.flush()
                await db.refresh(journal, attribute_names=["created_at"])
                entity.first_stored_at = journal.created_at.replace(tzinfo=timezone.utc)
                record = source_record(entity)
                journal.snapshot = record.model_dump(mode="json")
                change = change.model_copy(update={"record": record})
            changes.append(change)
        await db.flush()
        return CaptureResult(
            changes=tuple(changes), sequence=sync.observed_change_sequence, unchanged=unchanged
        )

    async def reconcile_scope(self, db: AsyncSession, request: ReconcileScope) -> ReconcileResult:
        """Tombstone a bounded batch absent from one successfully enumerated scope."""
        sync = await self._fenced_sync(db, request.fence)
        return await self._reconcile_scope_locked(db, sync, request)

    async def _reconcile_scope_locked(
        self,
        db: AsyncSession,
        sync: Sync,
        request: ReconcileScope,
        *,
        seen_id: UUID | None = None,
        parent_scoped: bool = False,
    ) -> ReconcileResult:
        """Share exact-scope reconciliation with durable whole-scope scans."""
        statement = (
            select(Entity)
            .where(
                Entity.organization_id == request.fence.organization_id,
                Entity.sync_id == sync.id,
                Entity.entity_definition_short_name == request.scope.record_type,
                Entity.container_id.is_not_distinct_from(request.scope.container_id),
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                Entity.last_seen_run_id.is_distinct_from(seen_id or request.fence.attempt_id),
            )
            .order_by(Entity.id)
            .limit(request.limit + 1)
        )
        if parent_scoped:
            parent = request.scope.parent
            statement = statement.where(
                Entity.parent_record_type.is_not_distinct_from(
                    parent.record_type if parent else None
                ),
                Entity.parent_native_id.is_not_distinct_from(parent.native_id if parent else None),
                Entity.parent_container_id.is_not_distinct_from(
                    parent.container_id if parent else None
                ),
            )
        entities = list((await db.scalars(statement)).all())
        observations = tuple(
            CaptureRecord(
                identity=source_record(entity).identity,
                parent=source_record(entity).parent,
                payload=entity.source_payload,
                payload_schema_version=entity.payload_schema_version,
                kind="delete",
                removal_reason=request.removal_reason,
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
            db, sync, CaptureBatch(fence=request.fence, records=observations), seen_id=seen_id
        )
        return ReconcileResult(capture=result, has_more=len(entities) > request.limit)

    async def start_scope(self, db: AsyncSession, fence: WriterFence, scope: StartedScope) -> None:
        """Restart a full-scan scope without inheriting partial incremental sightings."""
        await self._fenced_sync(db, fence)
        await db.execute(
            update(Entity)
            .where(
                Entity.organization_id == fence.organization_id,
                Entity.sync_id == fence.sync_id,
                Entity.entity_definition_short_name == scope.record_type,
                Entity.container_id.is_not_distinct_from(scope.container_id),
                Entity.record_revision > 0,
            )
            .values(last_seen_run_id=None)
        )
        await db.flush()

    async def remove_scope(
        self,
        db: AsyncSession,
        fence: WriterFence,
        scope: RemovedScope,
        *,
        limit: int = 250,
    ) -> ReconcileResult:
        """Tombstone a bounded exact scope regardless of this attempt's sightings."""
        if not 1 <= limit <= 500:
            raise ValueError("Scope removal batch limit must be between 1 and 500")
        sync = await self._fenced_sync(db, fence)
        entities = list(
            (
                await db.scalars(
                    select(Entity)
                    .where(
                        Entity.organization_id == fence.organization_id,
                        Entity.sync_id == fence.sync_id,
                        Entity.entity_definition_short_name == scope.record_type,
                        Entity.container_id.is_not_distinct_from(scope.container_id),
                        Entity.record_revision > 0,
                        Entity.deleted_at.is_(None),
                    )
                    .order_by(Entity.id)
                    .limit(limit + 1)
                )
            ).all()
        )
        observations = tuple(
            CaptureRecord(
                identity=source_record(entity).identity,
                parent=source_record(entity).parent,
                payload=entity.source_payload,
                payload_schema_version=entity.payload_schema_version,
                kind="delete",
                removal_reason=scope.removal_reason,
                completeness=entity.completeness,
                content_hash=entity.content_hash,
                source_created_at=entity.source_created_at,
                source_updated_at=entity.source_updated_at,
                observed_at=scope.observed_at,
                blobs=entity.blob_references or (),
            )
            for entity in entities[:limit]
        )
        result = await self._capture_locked(
            db, sync, CaptureBatch(fence=fence, records=observations)
        )
        return ReconcileResult(capture=result, has_more=len(entities) > limit)

    async def reconcile_parents(
        self,
        db: AsyncSession,
        fence: WriterFence,
        *,
        limit: int = 250,
    ) -> ReconcileResult:
        """Durable unfinished removal is derived from child rows, not transient events."""
        if not 1 <= limit <= 500:
            raise ValueError("Parent reconciliation limit must be between 1 and 500")
        sync = await self._fenced_sync(db, fence)
        rows = list(
            (
                await db.scalars(
                    select(Entity)
                    .where(
                        Entity.organization_id == fence.organization_id,
                        Entity.sync_id == fence.sync_id,
                        Entity.record_revision > 0,
                        Entity.deleted_at.is_(None),
                        Entity.parent_record_type.is_not(None),
                        ~active_parent_exists(),
                    )
                    .order_by(Entity.id)
                    .limit(limit + 1)
                )
            ).all()
        )
        observations = []
        for entity in rows[:limit]:
            record = source_record(entity)
            chain = ancestor_chain()
            nearest_reason = (
                select(chain.c.removal_reason)
                .where(chain.c.removal_reason.in_(("access_revoked", "scope_removed")))
                .order_by(func.cardinality(chain.c.path))
                .limit(1)
                .scalar_subquery()
            )
            reason = await db.scalar(select(nearest_reason).where(Entity.id == entity.id))
            observations.append(
                CaptureRecord(
                    identity=record.identity,
                    parent=record.parent,
                    payload=record.payload,
                    payload_schema_version=record.payload_schema_version,
                    kind="delete",
                    removal_reason="access_revoked"
                    if reason == "access_revoked"
                    else "scope_removed",
                    completeness=record.completeness,
                    content_hash=record.content_hash,
                    source_created_at=record.source_created_at,
                    source_updated_at=record.source_updated_at,
                    observed_at=datetime.now(timezone.utc),
                    blobs=record.blobs,
                )
            )
        result = await self._capture_locked(
            db, sync, CaptureBatch(fence=fence, records=tuple(observations))
        )
        return ReconcileResult(capture=result, has_more=len(rows) > limit)

    async def save_checkpoint(
        self, db: AsyncSession, fence: WriterFence, cursor_data: dict
    ) -> None:
        """Call only after capture barrier and successful exact-scope reconciliation."""
        sync = await self._fenced_sync(db, fence)
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == fence.sync_id))
        if CYCLE_KEY in cursor_data or (cursor is not None and CYCLE_KEY in cursor.cursor_data):
            raise CanonicalStoreError("Cycle-owned progress must use explicit cycle finalization")
        if cursor is None:
            cursor = SyncCursor(
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
            )
            db.add(cursor)
        # Source-supplied or previously loaded values cannot overwrite this reserved stamp.
        cursor.cursor_data = {
            **cursor_data,
            "canonical_checkpoint": CanonicalCheckpoint(
                writer_attempt_id=fence.attempt_id,
                observed_change_sequence=sync.observed_change_sequence,
            ).model_dump(mode="json"),
        }
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
        if entity is None:
            return None
        available = await db.scalar(select(content_is_available()).where(Entity.id == entity.id))
        return with_content_access(source_record(entity), bool(available))

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
        availability = (
            dict(
                (
                    await db.execute(
                        select(Entity.id, content_is_available()).where(
                            Entity.organization_id == organization_id,
                            Entity.sync_id == sync_id,
                            Entity.id.in_([row.entity_record_id for row in page]),
                        )
                    )
                ).all()
            )
            if page
            else {}
        )
        return ChangePage(
            changes=tuple(
                ObservedChange(
                    sequence=row.sequence,
                    kind=row.kind,
                    record=with_content_access(
                        SourceRecord.model_validate(row.snapshot),
                        bool(availability.get(row.entity_record_id, False)),
                    ),
                )
                for row in page
            ),
            next_sequence=page[-1].sequence if more else upper,
            high_watermark=upper,
            has_more=more,
        )
