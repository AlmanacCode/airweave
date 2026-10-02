"""Read publication recovery from existing jobs and metadata from canonical Entity rows."""

from uuid import UUID

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import content_is_available
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_models import NativeImportReceipt
from airweave.domains.native_ingestion.import_store import import_state, native_import_id, receipt
from airweave.domains.native_ingestion.models import NativeSnapshotMetadata
from airweave.domains.native_ingestion.publication_models import (
    NativeInventoryPage,
    NativeInventoryRecord,
    NativePublication,
)
from airweave.domains.native_ingestion.source_store import LockedNativeSource, NativeSourceStore
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


class NativePublicationStore:
    """The service owns the transaction; source locks serialize withdrawal with these reads."""

    def __init__(self, sources: NativeSourceStore):
        """Reuse the sole native owner/organization binding boundary."""
        self.sources = sources

    async def _readable(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, owner_id: str
    ) -> LockedNativeSource:
        bound = await self.sources.require(db, organization_id, source_id)
        if bound.binding.owner_id != owner_id or not await db.scalar(
            select(source_is_readable(organization_id, bound.sync.id))
        ):
            raise HTTPException(404, "Native source is unavailable for this owner")
        return bound

    async def current(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, owner_id: str
    ) -> NativePublication:
        """Never adopt an unknown job, malformed receipt or superseded writer attempt."""
        bound = await self._readable(db, organization_id, source_id, owner_id)
        if bound.sync.writer_job_id is None:
            if (
                bound.sync.writer_epoch != 0
                or bound.sync.writer_attempt_id is not None
                or bound.sync.writer_attempt_number != 0
            ):
                raise NativeAdmissionError("Native publication writer is missing")
            return NativePublication(source=bound.response(), current=None)
        job = await db.scalar(
            select(SyncJob)
            .where(
                SyncJob.id == bound.sync.writer_job_id,
                SyncJob.organization_id == organization_id,
                SyncJob.sync_id == bound.sync.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if job is None:
            raise NativeAdmissionError("Native publication writer does not match its source")
        try:
            metadata = NativeImportReceipt.model_validate(job.sync_metadata)
        except ValidationError as error:
            raise NativeAdmissionError("Native import receipt is malformed") from error
        saved = receipt(job, source_id, metadata.request_key)
        if (
            job.id != native_import_id(source_id, saved.request_key)
            or saved.fence.epoch != bound.sync.writer_epoch
            or saved.fence.attempt_id != bound.sync.writer_attempt_id
            or saved.fence.attempt_number != bound.sync.writer_attempt_number
            or job.status not in ("running", "completed", "cancelled", "failed")
            or (job.status == "completed" and saved.summary is None)
        ):
            raise NativeAdmissionError("Native publication receipt contradicts its current writer")
        return NativePublication(source=bound.response(), current=import_state(job, saved))

    async def inventory(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        owner_id: str,
        *,
        limit: int,
        after: UUID | None,
    ) -> NativeInventoryPage:
        """Bounded live keyset including withdrawn rows, for exact upstream existence checks."""
        if not 1 <= limit <= 100:
            raise NativeAdmissionError("Native inventory limit must be between 1 and 100")
        bound = await self._readable(db, organization_id, source_id, owner_id)
        parent = aliased(Entity)
        parent_epoch = (
            select(parent.visibility_epoch)
            .where(
                parent.organization_id == Entity.organization_id,
                parent.sync_id == Entity.sync_id,
                parent.entity_definition_short_name == Entity.parent_record_type,
                parent.native_id == Entity.parent_native_id,
                parent.container_id.is_not_distinct_from(Entity.parent_container_id),
                parent.record_revision > 0,
            )
            .correlate(Entity)
            .scalar_subquery()
        )
        # Select only typed outer metadata. Original JSON, blob locators and text
        # bytes never leave PostgreSQL for administrative reconciliation.
        native_metadata = func.jsonb_build_object(
            "authority",
            Entity.source_payload["authority"],
            "representation",
            Entity.source_payload["representation"],
            "schema_version",
            Entity.source_payload["schema_version"],
            "owner_id",
            Entity.source_payload["owner_id"],
            "identity",
            Entity.source_payload["identity"],
            "parent",
            Entity.source_payload["parent"],
            "version",
            Entity.source_payload["version"],
            "operation",
            Entity.source_payload["operation"],
        )
        statement = (
            select(
                Entity.id,
                Entity.entity_id,
                Entity.entity_definition_short_name,
                Entity.native_id,
                Entity.container_id,
                Entity.parent_record_type,
                Entity.parent_native_id,
                Entity.parent_container_id,
                Entity.record_revision,
                Entity.removal_reason,
                parent_epoch.label("parent_epoch"),
                (Entity.deleted_at.is_(None) & content_is_available()).label("available"),
                native_metadata.label("metadata"),
            )
            .where(
                Entity.organization_id == organization_id,
                Entity.sync_id == bound.sync.id,
                Entity.record_revision > 0,
            )
            .order_by(Entity.id)
            .limit(limit + 1)
        )
        if after is not None:
            statement = statement.where(Entity.id > after)
        rows = (await db.execute(statement)).all()
        records = []
        for row in rows[:limit]:
            try:
                metadata = NativeSnapshotMetadata.model_validate(row.metadata)
                identity = RecordIdentity(
                    record_type=row.entity_definition_short_name,
                    native_id=row.native_id,
                    container_id=row.container_id,
                )
                parent_identity = (
                    RecordIdentity(
                        record_type=row.parent_record_type,
                        native_id=row.parent_native_id,
                        container_id=row.parent_container_id,
                    )
                    if row.parent_record_type is not None
                    else None
                )
            except ValidationError as error:
                raise NativeAdmissionError(
                    "Retained native inventory metadata is malformed"
                ) from error
            if (
                metadata.identity != identity
                or identity.entity_key != row.entity_id
                or metadata.parent != parent_identity
                or metadata.owner_id != bound.binding.owner_id
                or (metadata.version.kind == "record") != (bound.binding.dataset == "knowledge")
            ):
                raise NativeAdmissionError("Retained native inventory does not match its source")
            records.append(
                NativeInventoryRecord(
                    record_id=row.id,
                    identity=identity,
                    revision=row.record_revision,
                    parent_visibility_epoch=row.parent_epoch,
                    available=row.available,
                    removal_reason=row.removal_reason,
                    version=metadata.version,
                )
            )
        has_more = len(rows) > limit
        return NativeInventoryPage(
            source=bound.response(),
            records=tuple(records),
            has_more=has_more,
            next_after=records[-1].record_id if has_more else None,
        )
