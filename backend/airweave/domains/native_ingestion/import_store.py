"""Native import start/recovery in the existing source writer transaction."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID, uuid4, uuid5

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.shared_models import SyncStatus
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CycleConfiguration,
    RestartCycle,
)
from airweave.domains.entities.canonical.cycle_store import (
    begin_cycle,
    cursor_row,
    cycle_state,
    restart_cycle,
)
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError, NativeImportNotFound
from airweave.domains.native_ingestion.import_models import (
    NativeImportReceipt,
    NativeImportState,
    StartNativeImport,
)
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.models.sync_job import SyncJob

# Persisted v1 identity: changing this namespace would break retry recovery.
IMPORT_NAMESPACE = UUID("8aab29e2-c0a6-40cd-a540-5316f1b6c4b3")


def native_import_id(source_id: UUID, request_key: str) -> UUID:
    """Unambiguous, source-scoped identity backed by the existing job primary key."""
    if not 1 <= len(request_key) <= 128:
        raise NativeAdmissionError("Import request key must contain 1 to 128 characters")
    return uuid5(IMPORT_NAMESPACE, json.dumps([str(source_id), request_key], separators=(",", ":")))


def receipt(job: SyncJob, source_id: UUID, request_key: str) -> NativeImportReceipt:
    """Reject unrelated jobs and malformed state instead of adopting or restarting them."""
    try:
        value = NativeImportReceipt.model_validate(job.sync_metadata)
    except ValidationError as error:
        raise NativeAdmissionError("Native import receipt is malformed") from error
    if (
        value.source_id != source_id
        or value.request_key != request_key
        or value.fence.job_id != job.id
        or value.fence.sync_id != job.sync_id
        or value.fence.organization_id != job.organization_id
    ):
        raise NativeAdmissionError("Native import receipt does not match its job")
    return value


def import_state(job: SyncJob, value: NativeImportReceipt) -> NativeImportState:
    """Never return the internal writer fence to the publisher."""
    return NativeImportState(
        source_id=value.source_id,
        import_id=job.id,
        request_key=value.request_key,
        request=value.request,
        status=job.status,
        cycle_id=value.cycle_id,
    )


class NativeImportStore:
    """No commits: caller owns one UnitOfWork for job, writer and cycle creation."""

    def __init__(self, sources: NativeSourceStore, canonical: CanonicalRecordStore):
        """Reuse native source ownership and canonical writer/cycle machinery."""
        self.sources = sources
        self.canonical = canonical

    async def start(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: StartNativeImport,
    ) -> NativeImportState:
        """Recover exact requests, otherwise create one fenced snapshot import."""
        bound = await self.sources.require(db, organization_id, source_id)
        job_id = native_import_id(source_id, request_key)
        job = await db.scalar(select(SyncJob).where(SyncJob.id == job_id).with_for_update())
        if job is not None:
            if job.sync_id != bound.sync.id or job.organization_id != organization_id:
                raise NativeAdmissionError("Import job belongs to another source")
            saved = receipt(job, source_id, request_key)
            if saved.request != request:
                raise NativeAdmissionError("Import request key has conflicting intent")
            return import_state(job, saved)

        if not bound.source.is_authenticated or bound.sync.status != SyncStatus.ACTIVE:
            raise NativeAdmissionError("Native source is unavailable for import")
        previous_job_id = bound.sync.writer_job_id
        job = SyncJob(
            id=job_id,
            sync_id=bound.sync.id,
            organization_id=organization_id,
            provisioning_generation=bound.sync.provisioning_generation,
            status="running",
            started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        db.add(job)
        await db.flush()
        fence = await self.canonical.activate_writer(
            db,
            organization_id,
            bound.sync.id,
            job_id,
            attempt_id=uuid4(),
            attempt_number=1,
        )
        plan = {"native_import_id": str(job_id), **request.model_dump(mode="json")}
        fingerprint = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        parents = (
            {"knowledge": (None,)}
            if bound.binding.dataset == "knowledge"
            else {"session": (None,), "message": ("session",)}
        )
        policy = "exhaustive" if request.coverage == "complete" else "discovery_only"
        configuration = CycleConfiguration(
            fingerprint=fingerprint,
            parents=parents,
            completion_policies={kind: policy for kind in parents},
        )
        previous = cycle_state(await cursor_row(db, fence))
        if previous is not None and previous.source_plan.get("native_import_id") != str(
            previous_job_id
        ):
            raise NativeAdmissionError("Existing capture cycle belongs to another import lifecycle")
        if previous is not None and previous.phase == "active":
            # Activation already rejects a different live writer. A new import after
            # a terminal job explicitly replaces its incomplete enumeration only.
            cycle = await restart_cycle(
                db,
                RestartCycle(
                    fence=fence,
                    expected=previous.version,
                    configuration=configuration,
                    source_plan=plan,
                ),
            )
        else:
            cycle = await begin_cycle(
                db,
                BeginCycle(
                    fence=fence,
                    expected=previous.version if previous else None,
                    configuration=configuration,
                    source_plan=plan,
                ),
            )
        saved = NativeImportReceipt(
            source_id=source_id,
            request_key=request_key,
            request=request,
            fence=fence,
            cycle_id=cycle.version.cycle_id,
        )
        job.sync_metadata = saved.model_dump(mode="json")
        await db.flush()
        return import_state(job, saved)

    async def read(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
    ) -> NativeImportState:
        """Authorize actual source/job association, including terminal import retries."""
        bound = await self.sources.require(db, organization_id, source_id)
        job = await db.scalar(
            select(SyncJob).where(
                SyncJob.id == native_import_id(source_id, request_key),
                SyncJob.sync_id == bound.sync.id,
                SyncJob.organization_id == organization_id,
            )
        )
        if job is None:
            raise NativeImportNotFound("Native import does not exist")
        return import_state(job, receipt(job, source_id, request_key))

    async def active(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
    ) -> NativeImportReceipt:
        """Resolve stored authority for a mutation, never a publisher-supplied fence."""
        bound = await self.sources.require(db, organization_id, source_id)
        if not bound.source.is_authenticated or bound.sync.status != SyncStatus.ACTIVE:
            raise NativeAdmissionError("Native source is unavailable for import")
        job = await db.scalar(
            select(SyncJob)
            .where(
                SyncJob.id == native_import_id(source_id, request_key),
                SyncJob.sync_id == bound.sync.id,
                SyncJob.organization_id == organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if job is None:
            raise NativeImportNotFound("Native import does not exist")
        saved = receipt(job, source_id, request_key)
        await self.canonical._fenced_sync(db, saved.fence)
        return saved
