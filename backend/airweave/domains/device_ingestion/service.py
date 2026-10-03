"""Transaction and exact-byte admission boundary for the trusted device gateway."""

import hashlib
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.device_ingestion.models import (
    BindDevice,
    CommitDevicePage,
    DeviceBeginRequest,
    DevicePageAck,
    DevicePrincipal,
    DeviceRunState,
    DeviceSourceState,
    DeviceUploadHandle,
    DeviceUploadIntent,
    EnsureDeviceSource,
    RevokeDevice,
)
from airweave.domains.device_ingestion.store import DeviceAdmissionError, DeviceIngestionStore
from airweave.domains.storage.protocols import StorageBackend


class DeviceIngestion:
    """Gateway authority arrives at the HTTP edge; this service owns atomic operations."""

    def __init__(self, store: DeviceIngestionStore, storage: StorageBackend | None = None):
        """Inject the sole persistence boundary; it composes the existing canonical engine."""
        self.store = store
        self.storage = storage

    async def ensure(
        self, db: AsyncSession, organization_id: UUID, request: EnsureDeviceSource
    ) -> DeviceSourceState:
        """Create/recover immutable source identity without activating a publisher."""
        async with UnitOfWork(db):
            return await self.store.ensure(db, organization_id, request)

    async def get(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, owner_id: str
    ) -> DeviceSourceState:
        """Owner-bound enrollment read, never private source content."""
        async with UnitOfWork(db):
            return (await self.store.require(db, organization_id, source_id, owner_id)).state()

    async def bind(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, request: BindDevice
    ) -> DeviceSourceState:
        """Attested remote reauthorization replaces the previous publisher atomically."""
        async with UnitOfWork(db):
            return await self.store.bind(db, organization_id, source_id, request)

    async def revoke(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, request: RevokeDevice
    ) -> DeviceSourceState:
        """Invalidate generation and retained-read authority together."""
        async with UnitOfWork(db):
            return await self.store.revoke(db, organization_id, source_id, request)

    async def start(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: DeviceBeginRequest | DevicePrincipal,
    ) -> DeviceRunState:
        """Create/recover one active canonical job/cycle/scope for this generation."""
        async with UnitOfWork(db):
            return await self.store.start_run(
                db,
                organization_id,
                source_id,
                request_key,
                DeviceBeginRequest.model_validate(request.model_dump()),
            )

    async def get_run(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        owner_id: str,
    ) -> DeviceRunState:
        """Current publisher authority gates immutable terminal metadata too."""
        async with UnitOfWork(db):
            bound = await self.store.require(db, organization_id, source_id, owner_id)
            job, saved = await self.store.load_run(db, bound, request_key)
            self.store.attest(bound, saved.principal)
            return await self.store.run_state(db, job, saved, bound.enrollment.binding)

    async def page(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request_bytes: bytes,
    ) -> DevicePageAck:
        """SHA256 covers exact incoming UTF-8 JSON bytes, never reserialized client integers."""
        if len(request_bytes) > 2 * 1024 * 1024:
            raise DeviceAdmissionError("Device page exceeds 2MiB")
        request = CommitDevicePage.model_validate_json(request_bytes)
        digest = hashlib.sha256(request_bytes).hexdigest()
        async with UnitOfWork(db):
            return await self.store.page(
                db, organization_id, source_id, request_key, request, digest
            )

    async def complete(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: DevicePrincipal,
    ) -> DeviceRunState:
        """Completion certifies only bounded acquisition, with indexing explicitly unverified."""
        async with UnitOfWork(db):
            return await self.store.complete(db, organization_id, source_id, request_key, request)

    async def declare_upload(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        handle: UUID,
        request: DeviceUploadIntent,
    ) -> DeviceUploadHandle:
        """Persist exact immutable intent before accepting any attachment bytes."""
        from airweave.domains.device_ingestion.uploads import declare

        async with UnitOfWork(db):
            return await declare(
                self.store, db, organization_id, source_id, request_key, handle, request
            )

    async def upload(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        handle: UUID,
        publisher: DevicePrincipal,
        content: bytes,
    ) -> DeviceUploadHandle:
        """Two short transactions fence an external storage write without blocking revoke."""
        from airweave.domains.device_ingestion.uploads import active_run
        from airweave.domains.storage.file_service import FileService

        if self.storage is None:
            raise DeviceAdmissionError("Device attachment storage is unavailable")
        if len(content) > 8 * 1024 * 1024:
            raise DeviceAdmissionError("Attachment exceeds 8MiB")
        async with UnitOfWork(db):
            bound, job, saved = await active_run(
                self.store, db, organization_id, source_id, request_key, publisher
            )
            pending = next((item for item in saved.uploads if item.handle == handle), None)
            if pending is None:
                raise DeviceAdmissionError("Upload requires prior immutable intent")
            if (
                len(content) != pending.size_bytes
                or hashlib.sha256(content).hexdigest() != pending.sha256
            ):
                raise DeviceAdmissionError("Upload bytes disagree with declared hash or size")
            sync_id, job_id = bound.sync.id, job.id
        blob = await FileService(job_id, self.storage, sync_id=sync_id).store_canonical_blob(
            content, media_type=pending.media_type
        )
        async with UnitOfWork(db):
            _, job, saved = await active_run(
                self.store, db, organization_id, source_id, request_key, publisher
            )
            current = next((item for item in saved.uploads if item.handle == handle), None)
            if current is None or current.intent_digest != pending.intent_digest:
                raise DeviceAdmissionError("Upload intent changed during storage write")
            admitted = current.model_copy(update={"blob": blob})
            job.sync_metadata = saved.model_copy(
                update={
                    "uploads": tuple(
                        admitted if item.handle == handle else item for item in saved.uploads
                    )
                }
            ).model_dump(mode="json")
            await db.flush()
            return admitted.state()
