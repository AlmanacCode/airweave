"""Atomic native import start/read application boundary."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.native_ingestion.access_models import NativeAccessChange, NativeRecordAccess
from airweave.domains.native_ingestion.access_store import NativeAccessStore
from airweave.domains.native_ingestion.import_models import NativeImportState, StartNativeImport
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.page_models import CommitNativePage, NativePageAck
from airweave.domains.native_ingestion.page_store import NativePageStore
from airweave.domains.native_ingestion.scope_models import (
    BeginNativeScope,
    NativeScopeRef,
    NativeScopeState,
    ReconcileNativeScope,
)
from airweave.domains.native_ingestion.scope_store import NativeScopeStore
from airweave.domains.native_ingestion.store import NativeIngestionStore


class NativeImports:
    """Own the transaction; never enqueue provider execution or expose writer fences."""

    def __init__(self, store: NativeImportStore):
        """Inject the native import persistence boundary."""
        self.store = store
        self.access = NativeAccessStore(store)
        self.scopes = NativeScopeStore(store)
        self.pages = NativePageStore(store, NativeIngestionStore(store.canonical))

    async def start(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: StartNativeImport,
    ) -> NativeImportState:
        """Create or recover a durable import and commit its writer/cycle atomically."""
        async with UnitOfWork(db):
            return await self.store.start(db, organization_id, source_id, request_key, request)

    async def read(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, request_key: str
    ) -> NativeImportState:
        """Read durable progress under source authorization without reactivation."""
        async with UnitOfWork(db):
            return await self.store.read(db, organization_id, source_id, request_key)

    async def begin_scope(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: BeginNativeScope,
    ) -> NativeScopeState:
        """Authorize and execute begin scope in one transaction."""
        async with UnitOfWork(db):
            return await self.scopes.begin(db, organization_id, source_id, request_key, request)

    async def read_scope(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: NativeScopeRef,
    ) -> NativeScopeState:
        """Authorize and execute read scope in one transaction."""
        async with UnitOfWork(db):
            return await self.scopes.read(db, organization_id, source_id, request_key, request)

    async def reconcile_scope(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: ReconcileNativeScope,
    ) -> NativeScopeState:
        """Authorize and execute reconcile scope in one transaction."""
        async with UnitOfWork(db):
            return await self.scopes.reconcile(db, organization_id, source_id, request_key, request)

    async def page(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: CommitNativePage,
    ) -> NativePageAck:
        """Authorize and execute page in one transaction."""
        async with UnitOfWork(db):
            return await self.pages.commit(db, organization_id, source_id, request_key, request)

    async def finish(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        *,
        cancel: bool = False,
    ) -> NativeImportState:
        """Finalize capture and its durable outcome in one transaction."""
        async with UnitOfWork(db):
            return await self.store.finish(
                db, organization_id, source_id, request_key, cancel=cancel
            )

    async def read_access(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        record_id: UUID,
    ) -> NativeRecordAccess:
        """Read retained revision and current availability without disclosing original content."""
        async with UnitOfWork(db):
            return await self.access.read(db, organization_id, source_id, request_key, record_id)

    async def change_access(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        record_id: UUID,
        request: NativeAccessChange,
    ) -> NativeRecordAccess:
        """Commit source access evidence and publication invalidation atomically."""
        async with UnitOfWork(db):
            return await self.access.change(
                db, organization_id, source_id, request_key, record_id, request
            )
