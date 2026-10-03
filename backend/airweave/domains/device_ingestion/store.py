"""Device admission in existing source/sync/job/scan transactions; no independent engine."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

from pydantic import ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.device_ingestion.models import (
    BindDevice,
    CommitDevicePage,
    DeviceBeginRequest,
    DeviceBinding,
    DeviceCaptureAuthority,
    DeviceCompletedScan,
    DeviceEnrollment,
    DeviceModel,
    DevicePageAck,
    DevicePrincipal,
    DeviceRunReceipt,
    DeviceRunState,
    DeviceSourceState,
    EnsureDeviceSource,
    RevokeDevice,
    device_run_id,
    device_source_id,
    device_sync_id,
)
from airweave.domains.device_ingestion.source_times import note_source_times
from airweave.domains.entities.canonical.actors import ACTOR_PIPELINE_VERSION
from airweave.domains.entities.canonical.apple_payloads import NativeNote, validate_device_original
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CompleteCycle,
    CycleConfiguration,
    RestartCycle,
)
from airweave.domains.entities.canonical.cycle_store import (
    begin_cycle,
    complete_cycle,
    cursor_row,
    cycle_state,
    restart_cycle,
)
from airweave.domains.entities.canonical.page_receipts import (
    PageAcknowledgement,
    ScanPageReceipts,
    page_digest,
    read_page_receipt,
)
from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore
from airweave.domains.entities.canonical.source_lifecycle import stop_source_writer
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob


class DeviceAdmissionError(CanonicalStoreError):
    """Enrollment or publisher intent is stale/conflicting; reload trusted binding state."""

    code = "device_admission_failed"


class DeviceSourceNotFound(DeviceAdmissionError):
    """Missing/foreign owner or organization exposes no source state."""

    code = "device_source_not_found"


class LockedDeviceSource(DeviceModel):
    """Internal ORM ownership under Sync then SourceConnection locks."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)
    sync: Sync
    source: SourceConnection
    enrollment: DeviceEnrollment

    def state(self) -> DeviceSourceState:
        """Enrollment/read authority are separate facts from publication readiness."""
        return DeviceSourceState(
            source_id=self.source.id,
            sync_id=self.sync.id,
            organization_id=self.sync.organization_id,
            collection=self.source.readable_collection_id,
            enrollment=self.enrollment,
            retained_read_authority=self.source.is_authenticated,
        )


def principal(request: DevicePrincipal) -> DevicePrincipal:
    """Page fields are not part of enrollment intent."""
    return DevicePrincipal(
        owner_id=request.owner_id,
        device_id=request.device_id,
        generation=request.generation,
        store_generation=request.store_generation,
    )


class DeviceIngestionStore:
    """Caller owns one UnitOfWork; enrollment and all admission share canonical Sync lock."""

    def __init__(self, canonical: CanonicalRecordStore):
        """The existing canonical writer/scans/receipts own all capture mechanics."""
        self.canonical = canonical
        self.scans = CanonicalScanStore(canonical)
        self.receipts = ScanPageReceipts(self.scans)

    async def ensure(
        self, db: AsyncSession, organization_id: UUID, request: EnsureDeviceSource
    ) -> DeviceSourceState:
        """Deterministic primary key serialization without credential fabrication."""
        collection = await db.scalar(
            select(Collection.id).where(
                Collection.organization_id == organization_id,
                Collection.readable_id == request.collection,
            )
        )
        if collection is None:
            raise DeviceSourceNotFound("Device collection not found")
        binding = request.binding()
        source_id = device_source_id(organization_id, binding)
        sync_id = device_sync_id(source_id)
        inserted = await db.scalar(
            insert(Sync)
            .values(
                id=sync_id,
                organization_id=organization_id,
                name=binding.source_kind,
                status="paused",
                sync_type="full",
                index_pipeline_version=ACTOR_PIPELINE_VERSION,
            )
            .on_conflict_do_nothing(index_elements=[Sync.id])
            .returning(Sync.id)
        )
        sync = await db.scalar(
            select(Sync)
            .where(Sync.id == sync_id, Sync.organization_id == organization_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if sync is None:
            raise DeviceAdmissionError("Device source identity conflicts with existing state")
        if inserted is not None:
            if await db.get(SourceConnection, source_id) is not None:
                raise DeviceAdmissionError("Device source identity conflicts with existing state")
            db.add(
                SourceConnection(
                    id=source_id,
                    organization_id=organization_id,
                    sync_id=sync_id,
                    name=binding.source_kind,
                    short_name=binding.source_kind,
                    config_fields=DeviceEnrollment(binding=binding).model_dump(mode="json"),
                    readable_collection_id=request.collection,
                    is_authenticated=False,
                )
            )
            await db.flush()
        bound = await self.require(db, organization_id, source_id, binding.owner_id)
        if (
            bound.enrollment.binding != binding
            or bound.source.readable_collection_id != request.collection
        ):
            raise DeviceAdmissionError("Device source binding and collection are immutable")
        return bound.state()

    async def require(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, owner_id: str
    ) -> LockedDeviceSource:
        """Lock ordering matches canonical admission and revocation; never query by labels."""
        sync = await db.scalar(
            select(Sync)
            .where(Sync.id == device_sync_id(source_id), Sync.organization_id == organization_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        source = await db.scalar(
            select(SourceConnection)
            .where(
                SourceConnection.id == source_id,
                SourceConnection.organization_id == organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if sync is None or source is None:
            raise DeviceSourceNotFound("Device source is unavailable")
        try:
            enrollment = DeviceEnrollment.model_validate(source.config_fields)
        except ValidationError as error:
            raise DeviceAdmissionError("Device source binding is malformed") from error
        if enrollment.binding.owner_id != owner_id:
            raise DeviceSourceNotFound("Device source is unavailable")
        if (
            source.sync_id != sync.id
            or device_source_id(organization_id, enrollment.binding) != source_id
            or source.short_name != enrollment.binding.source_kind
            or any(
                value is not None
                for value in (
                    source.connection_id,
                    source.readable_auth_provider_id,
                    source.auth_provider_config,
                    source.connection_init_session_id,
                )
            )
        ):
            raise DeviceAdmissionError("Device source identity conflicts with existing state")
        if (
            await db.scalar(
                select(Collection.id).where(
                    Collection.organization_id == organization_id,
                    Collection.readable_id == source.readable_collection_id,
                )
            )
            is None
        ):
            raise DeviceAdmissionError("Device collection is unavailable")
        return LockedDeviceSource(sync=sync, source=source, enrollment=enrollment)

    @staticmethod
    def attest(bound: LockedDeviceSource, request: DevicePrincipal) -> None:
        """The remotely stored publisher generation wins over late local bookmarks or payloads."""
        saved = bound.enrollment
        if (
            not saved.active
            or not bound.source.is_authenticated
            or bound.sync.status != "active"
            or saved.binding.owner_id != request.owner_id
            or saved.device_id != request.device_id
            or saved.generation != request.generation
            or saved.store_generation != request.store_generation
        ):
            raise DeviceAdmissionError("Device enrollment is unavailable or superseded")

    async def bind(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, request: BindDevice
    ) -> DeviceSourceState:
        """Explicit gateway-attested source reauthorization, not an ordinary bookmark reconnect."""
        bound = await self.require(db, organization_id, source_id, request.owner_id)
        old = bound.enrollment
        if (
            old.generation == request.expected_generation + 1
            and old.active
            and old.device_id == request.device_id
            and old.store_generation == request.store_generation
            and bound.source.is_authenticated
        ):
            return bound.state()  # Lost-response recovery must not rotate generation twice.
        if old.generation != request.expected_generation:
            raise DeviceAdmissionError("Device binding generation changed")
        await stop_source_writer(db, bound.sync, bound.source, retain_read_authority=False)
        updated = DeviceEnrollment(
            binding=old.binding,
            generation=old.generation + 1,
            device_id=request.device_id,
            store_generation=request.store_generation,
            active=True,
        )
        bound.source.config_fields = updated.model_dump(mode="json")
        bound.source.is_authenticated = True
        bound.sync.status = "active"
        await db.flush()
        return LockedDeviceSource(sync=bound.sync, source=bound.source, enrollment=updated).state()

    async def revoke(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, request: RevokeDevice
    ) -> DeviceSourceState:
        """Fencing, cancelled jobs and retained-read withdrawal commit under one Sync lock."""
        bound = await self.require(db, organization_id, source_id, request.owner_id)
        old = bound.enrollment
        if (
            old.generation == request.expected_generation + 1
            and not old.active
            and not bound.source.is_authenticated
        ):
            return bound.state()
        if old.generation != request.expected_generation:
            raise DeviceAdmissionError("Device binding generation changed")
        await stop_source_writer(db, bound.sync, bound.source, retain_read_authority=False)
        updated = old.model_copy(update={"generation": old.generation + 1, "active": False})
        bound.source.config_fields = updated.model_dump(mode="json")
        await db.flush()
        return LockedDeviceSource(sync=bound.sync, source=bound.source, enrollment=updated).state()

    async def load_run(
        self, db: AsyncSession, bound: LockedDeviceSource, request_key: str
    ) -> tuple[SyncJob, DeviceRunReceipt]:
        """Existing job metadata is the only run receipt; validate every authority identity."""
        job = await db.scalar(
            select(SyncJob)
            .where(
                SyncJob.id == device_run_id(bound.source.id, request_key),
                SyncJob.organization_id == bound.sync.organization_id,
                SyncJob.sync_id == bound.sync.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if job is None:
            raise DeviceSourceNotFound("Device run is unavailable")
        try:
            saved = DeviceRunReceipt.model_validate(job.sync_metadata)
        except ValidationError as error:
            raise DeviceAdmissionError("Device run receipt is malformed") from error
        if (
            saved.principal.owner_id != bound.enrollment.binding.owner_id
            or saved.source_id != bound.source.id
            or saved.request_key != request_key
            or saved.fence.job_id != job.id
            or saved.fence.sync_id != bound.sync.id
            or saved.fence.organization_id != bound.sync.organization_id
        ):
            raise DeviceAdmissionError("Device run receipt conflicts with its job")
        return job, saved

    async def run_state(
        self, db: AsyncSession, job: SyncJob, saved: DeviceRunReceipt, binding: DeviceBinding
    ) -> DeviceRunState:
        """A reused scope row never reports another run's progress."""
        if job.status == "completed" and saved.completed_scan is not None:
            terminal = saved.completed_scan
            return DeviceRunState(
                run_id=job.id,
                source_id=saved.source_id,
                principal=saved.principal,
                status=job.status,
                version=terminal.version,
                phase="complete",
                cursor=terminal.cursor,
                last_page=terminal.final_page,
                acquisition_mode=saved.acquisition_mode,
                final_page_version=terminal.final_page.version,
            )
        row = await self.scans._row(
            db, saved.fence, CompletedScope(record_type=binding.record_type)
        )
        state = (
            await self.scans._state(db, row)
            if row is not None and row.cycle_id == saved.cycle_id
            else None
        )
        receipt = read_page_receipt(state.continuation) if state else None
        return DeviceRunState(
            run_id=job.id,
            source_id=saved.source_id,
            principal=saved.principal,
            status=job.status,
            acquisition_mode=saved.acquisition_mode,
            final_page_version=receipt.acknowledgement.version
            if receipt and receipt.acknowledgement.phase == "reconciling"
            else None,
            version=state.version if state else None,
            phase=state.phase if state else None,
            cursor=state.continuation.value.get("cursor", {}) if state else {},
            last_page=receipt.acknowledgement if receipt else None,
        )

    @staticmethod
    def validate_acquisition(source_kind: str, request: DeviceBeginRequest) -> None:
        """Replacement policy is source-specific; Notes never infer snapshot absence."""
        if request.acquisition_mode == "contacts_snapshot" and source_kind != "apple_contacts":
            raise DeviceAdmissionError("Only Contacts can declare an authorized visible snapshot")
        if request.replaces_run_id is not None and not (
            (request.acquisition_mode == "contacts_snapshot" and source_kind == "apple_contacts")
            or (request.acquisition_mode == "delta" and source_kind == "apple_notes")
        ):
            raise DeviceAdmissionError(
                "Replacement requires Contacts snapshot or Notes delta acquisition"
            )

    async def start_run(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: DeviceBeginRequest,
    ) -> DeviceRunState:
        """Create the canonical writer, bounded cycle and fixed root scope atomically."""
        bound = await self.require(db, organization_id, source_id, request.owner_id)
        publisher = principal(request)
        self.attest(bound, publisher)
        self.validate_acquisition(bound.enrollment.binding.source_kind, request)
        run_id = device_run_id(source_id, request_key)
        if await db.get(SyncJob, run_id) is not None:
            job, saved = await self.load_run(db, bound, request_key)
            if (
                saved.principal != publisher
                or saved.acquisition_mode != request.acquisition_mode
                or saved.replaces_run_id != request.replaces_run_id
            ):
                raise DeviceAdmissionError("Device run key has conflicting intent")
            return await self.run_state(db, job, saved, bound.enrollment.binding)
        previous_job = bound.sync.writer_job_id
        if request.replaces_run_id is not None:
            if request.replaces_run_id == run_id or previous_job != request.replaces_run_id:
                raise DeviceAdmissionError("Device replacement does not name the current writer")
            old_job = await db.scalar(
                select(SyncJob)
                .where(
                    SyncJob.id == request.replaces_run_id,
                    SyncJob.organization_id == organization_id,
                    SyncJob.sync_id == bound.sync.id,
                )
                .with_for_update()
            )
            if old_job is None or old_job.status != "running":
                raise DeviceAdmissionError("Device replacement requires an interrupted active run")
            old_saved = DeviceRunReceipt.model_validate(old_job.sync_metadata)
            if (
                old_saved.principal != publisher
                or old_saved.source_id != source_id
                or old_saved.fence.job_id != old_job.id
                or old_saved.fence.sync_id != bound.sync.id
                or old_saved.fence.organization_id != organization_id
            ):
                raise DeviceAdmissionError("Device replacement belongs to another acquisition")
            # Sync/source locks serialize old pages with this exact-job cancellation.
            # Read authority and partial originals remain intact until final new reconciliation.
            old_job.status = "cancelled"
            old_job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            bound.sync.writer_epoch += 1
        job = SyncJob(
            id=run_id,
            organization_id=organization_id,
            sync_id=bound.sync.id,
            provisioning_generation=bound.sync.provisioning_generation,
            status="running",
            started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        db.add(job)
        await db.flush()
        fence = await self.canonical.activate_writer(
            db, organization_id, bound.sync.id, run_id, attempt_id=uuid4(), attempt_number=1
        )
        plan = {"device_run_id": str(run_id), "principal": request.model_dump(mode="json")}
        configuration = CycleConfiguration(
            fingerprint=page_digest(request),
            parents={bound.enrollment.binding.record_type: (None,)},
            completion_policies={
                bound.enrollment.binding.record_type: "exhaustive"
                if request.acquisition_mode == "contacts_snapshot"
                else "discovery_only"
            },
        )
        previous = cycle_state(await cursor_row(db, fence))
        if previous is not None and previous.source_plan.get("device_run_id") != str(previous_job):
            raise DeviceAdmissionError("Existing cycle belongs to another acquisition lifecycle")
        if previous is not None and previous.phase == "active":
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
        scope = CompletedScope(record_type=bound.enrollment.binding.record_type)
        old_scope = await self.scans.read(db, fence, scope)
        await self.scans.begin(
            db,
            BeginScan(
                fence=fence,
                scope=scope,
                cycle_id=cycle.version.cycle_id,
                fingerprint=configuration.fingerprint,
                expected=old_scope.version if old_scope else None,
            ),
        )
        saved = DeviceRunReceipt(
            source_id=source_id,
            request_key=request_key,
            principal=publisher,
            acquisition_mode=request.acquisition_mode,
            replaces_run_id=request.replaces_run_id,
            fence=fence,
            cycle_id=cycle.version.cycle_id,
        )
        job.sync_metadata = saved.model_dump(mode="json")
        await db.flush()
        return await self.run_state(db, job, saved, bound.enrollment.binding)

    async def page(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: CommitDevicePage,
        request_digest: str,
    ) -> DevicePageAck:
        """Generation check, capture and receipt commit serialize against remote revoke/rebind."""
        bound = await self.require(db, organization_id, source_id, request.owner_id)
        publisher = principal(request)
        self.attest(bound, publisher)
        job, saved = await self.load_run(db, bound, request_key)
        if saved.principal != publisher:
            raise DeviceAdmissionError("Device page belongs to another run generation")
        scope = CompletedScope(record_type=bound.enrollment.binding.record_type)
        digest = request_digest
        recovered = await self.receipts.recover(
            db,
            saved.fence,
            scope,
            saved.cycle_id,
            page_id=request.page_id,
            digest=digest,
            expected=request.expected,
        )
        if recovered is not None:
            return self.page_ack(source_id, publisher, digest, recovered)
        notes_times: dict[str, tuple[datetime | None, datetime | None]] = {}
        for item in request.observations:
            if not item.original and item.kind == "delete":
                continue  # Sparse explicit lifecycle evidence is attested by the backend gateway.
            try:
                original = validate_device_original(
                    bound.enrollment.binding.source_kind, item.original
                )
            except (ValueError, ValidationError) as error:
                raise DeviceAdmissionError(
                    "Device original does not match its supported source schema"
                ) from error
            if original.native_id != item.native_id:
                raise DeviceAdmissionError("Device original identity disagrees with its envelope")
            if isinstance(original, NativeNote):
                notes_times[item.native_id] = note_source_times(original)
                if original.marked_for_deletion and (
                    item.kind != "delete" or item.removal_reason != "scope_removed"
                ):
                    raise DeviceAdmissionError(
                        "Marked note requires explicit local scope withdrawal"
                    )
                if (
                    original.locked
                    and not original.marked_for_deletion
                    and (item.kind != "delete" or item.removal_reason != "access_revoked")
                ):
                    raise DeviceAdmissionError("Locked note requires explicit access withdrawal")
        from airweave.domains.device_ingestion.uploads import resolve_uploads

        now = datetime.now(timezone.utc)
        records = tuple(
            CaptureRecord(
                identity={
                    "record_type": bound.enrollment.binding.record_type,
                    "native_id": item.native_id,
                },
                payload={
                    "authority": "device",
                    "source_kind": bound.enrollment.binding.source_kind,
                    "account_id": bound.enrollment.binding.account_id,
                    "original": item.original,
                },
                blobs=resolve_uploads(item, saved.uploads),
                kind=item.kind,
                removal_reason=item.removal_reason,
                completeness=item.completeness,
                source_created_at=notes_times.get(
                    item.native_id, (item.source_created_at, item.source_updated_at)
                )[0],
                source_updated_at=notes_times.get(
                    item.native_id, (item.source_created_at, item.source_updated_at)
                )[1],
                observed_at=now,
            )
            for item in request.observations
        )
        result = await self.scans.page(
            db,
            CommitScanPage(
                fence=saved.fence,
                scope=scope,
                cycle_id=saved.cycle_id,
                expected=request.expected,
                records=records,
                continuation=ScanContinuation(value={"cursor": request.cursor}),
                final=request.final,
            ),
        )
        ack = PageAcknowledgement(
            page_id=request.page_id,
            version=result.state.version,
            phase=result.state.phase,
            sequence=result.capture.sequence,
            changed=len(result.capture.changes),
            unchanged=result.capture.unchanged,
        )
        await self.receipts.persist(
            db,
            saved.fence,
            scope,
            saved.cycle_id,
            digest=digest,
            acknowledgement=ack,
            cursor=request.cursor,
        )
        return self.page_ack(source_id, publisher, digest, ack)

    @staticmethod
    def page_ack(
        source_id: UUID, publisher: DevicePrincipal, digest: str, ack: PageAcknowledgement
    ) -> DevicePageAck:
        """Echo only validated immutable publisher authority and the accepted exact-byte digest."""
        return DevicePageAck(
            authority=DeviceCaptureAuthority(
                binding_id=source_id,
                generation=publisher.generation,
                local_store_generation=publisher.store_generation,
            ),
            page_id=ack.page_id,
            sha256=digest,
            acknowledgement=ack,
        )

    async def complete(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: DevicePrincipal,
    ) -> DeviceRunState:
        """Reconcile one bounded batch; only declared Contacts snapshots infer scope absence."""
        bound = await self.require(db, organization_id, source_id, request.owner_id)
        self.attest(bound, request)
        job, saved = await self.load_run(db, bound, request_key)
        if saved.principal != request:
            raise DeviceAdmissionError("Device completion belongs to another run generation")
        if job.status == "completed":
            return await self.run_state(db, job, saved, bound.enrollment.binding)
        state = await self.scans.read(
            db, saved.fence, CompletedScope(record_type=bound.enrollment.binding.record_type)
        )
        if state is None or state.cycle_id != saved.cycle_id or state.phase != "reconciling":
            raise DeviceAdmissionError("Device run has no final collected page")
        result = await self.scans.reconcile(
            db,
            ReconcileScan(
                fence=saved.fence,
                scope=state.scope,
                cycle_id=saved.cycle_id,
                expected=state.version,
                observed_at=datetime.now(timezone.utc),
                removal_reason="scope_removed",
            ),
        )
        if result.state.phase != "complete":
            return await self.run_state(db, job, saved, bound.enrollment.binding)
        receipt = read_page_receipt(result.state.continuation)
        if receipt is None or receipt.acknowledgement.phase != "reconciling":
            raise DeviceAdmissionError("Completed run lacks its final page receipt")
        terminal = DeviceCompletedScan(
            version=result.state.version,
            final_page=receipt.acknowledgement,
            cursor=result.state.continuation.value.get("cursor", {}),
        )
        saved = saved.model_copy(update={"completed_scan": terminal})
        job.sync_metadata = saved.model_dump(mode="json")
        cycle = cycle_state(await cursor_row(db, saved.fence))
        await complete_cycle(
            db, bound.sync, CompleteCycle(fence=saved.fence, expected=cycle.version)
        )
        job.status = "completed"
        job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        await db.flush()
        return await self.run_state(db, job, saved, bound.enrollment.binding)
