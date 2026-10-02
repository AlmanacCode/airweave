"""Compose native access CAS with the canonical journal and parent visibility rules."""

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord
from airweave.domains.entities.canonical.store import content_is_available, source_record
from airweave.domains.native_ingestion.access_models import (
    NativeAccessChange,
    NativeRecordAccess,
    WithdrawNativeRecord,
)
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.models import IngestNativeBatch
from airweave.domains.native_ingestion.store import NativeIngestionStore
from airweave.models.entity import Entity


class NativeAccessStore:
    """The caller owns the transaction; no independent access journal or retry state."""

    def __init__(self, imports: NativeImportStore):
        """Reuse import locks, native admission and canonical capture."""
        self.imports = imports
        self.ingestion = NativeIngestionStore(imports.canonical)

    async def _record(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, record_id: UUID
    ) -> Entity:
        row = await db.scalar(
            select(Entity)
            .where(
                Entity.id == record_id,
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Entity.record_revision > 0,
            )
            .execution_options(populate_existing=True)
        )
        if row is None:
            raise NativeAdmissionError("Native record is unavailable in this source")
        return row

    async def _parent_epoch(self, db: AsyncSession, row: Entity) -> int | None:
        """Native messages have one session parent; no arbitrary ancestor token scheme."""
        if row.parent_record_type is None:
            return None
        record = source_record(row)
        assert record.parent is not None
        return await db.scalar(
            select(Entity.visibility_epoch).where(
                Entity.organization_id == row.organization_id,
                Entity.sync_id == row.sync_id,
                Entity.entity_definition_short_name == record.parent.record_type,
                Entity.entity_id == record.parent.entity_key,
                Entity.record_revision > 0,
            )
        )

    async def read(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        record_id: UUID,
    ) -> NativeRecordAccess:
        """Read current CAS state even when the original is inaccessible or import terminal."""
        bound, _, _ = await self.imports.load(db, organization_id, source_id, request_key)
        row = await self._record(db, organization_id, bound.sync.id, record_id)
        record = source_record(row)
        available = await db.scalar(
            select(
                content_is_available() & source_is_readable(organization_id, bound.sync.id)
            ).where(Entity.id == row.id)
        )
        return NativeRecordAccess(
            record_id=row.id,
            identity=record.identity,
            revision=row.record_revision,
            parent_visibility_epoch=await self._parent_epoch(db, row),
            available=bool(available) and row.deleted_at is None,
            removal_reason=row.removal_reason,
        )

    async def change(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        record_id: UUID,
        request: NativeAccessChange,
    ) -> NativeRecordAccess:
        """Fresh source evidence and retained revision must agree under the active writer."""
        imported = await self.imports.active(db, organization_id, source_id, request_key)
        sync = await self.imports.canonical._fenced_sync(db, imported.fence)
        row = await self._record(db, organization_id, sync.id, record_id)
        if row.record_revision != request.expected_revision:
            raise NativeAdmissionError("Native record changed; read current access before retrying")
        record = source_record(row)
        now = datetime.now(timezone.utc)
        if isinstance(request, WithdrawNativeRecord):
            # Access is a local observation. Preserve original content/version and blobs.
            observations = (
                CaptureRecord(
                    identity=record.identity,
                    parent=record.parent,
                    payload=record.payload,
                    payload_schema_version=record.payload_schema_version,
                    content_hash=record.content_hash,
                    completeness=record.completeness,
                    blobs=record.blobs,
                    kind="delete",
                    removal_reason=request.reason,
                    observed_at=now,
                    source_created_at=record.source_created_at,
                    source_updated_at=record.source_updated_at,
                ),
            )
        else:
            parent_epoch = await self._parent_epoch(db, row)
            if request.expected_parent_epoch != parent_epoch or (
                record.parent is not None and parent_epoch is None
            ):
                raise NativeAdmissionError(
                    "Native parent access changed; obtain fresh source evidence"
                )
            if (
                request.snapshot.identity != record.identity
                or request.snapshot.operation != "upsert"
            ):
                raise NativeAdmissionError("Renewal requires this exact active native original")
            admitted = await self.ingestion.admit_locked(
                db,
                sync,
                IngestNativeBatch(
                    fence=imported.fence,
                    observed_at=now,
                    snapshots=(request.snapshot,),
                ),
                revalidated_ids=(row.id,),
            )
            observations = admitted.records
        await self.imports.canonical._capture_locked(
            db,
            sync,
            CaptureBatch(fence=imported.fence, records=observations),
            mark_seen=False,
        )
        return await self.read(db, organization_id, source_id, request_key, record_id)
