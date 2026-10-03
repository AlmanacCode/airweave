"""Native scope lifecycle composed with canonical scope admission and reconciliation."""

from datetime import datetime, timezone
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_store import attest_cycle, scope_owner
from airweave.domains.entities.canonical.page_receipts import read_page_receipt
from airweave.domains.entities.canonical.scan_models import BeginScan, ReconcileScan, ScanState
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.scope_models import (
    BeginNativeScope,
    NativeScopeRef,
    NativeScopeState,
    ReconcileNativeScope,
)


def scope_state(state: ScanState) -> NativeScopeState:
    """Hide the internal receipt envelope, retaining its bounded last-page acknowledgement."""
    try:
        saved = read_page_receipt(state.continuation)
        ack = saved.acknowledgement if saved is not None else None
        return NativeScopeState(
            scope=state.scope,
            cycle_id=state.cycle_id,
            version=state.version,
            phase=state.phase,
            coverage="complete" if state.completion_policy == "exhaustive" else "bounded",
            cursor=state.continuation.value.get("cursor", {}),
            last_page=ack,
        )
    except ValidationError as error:
        raise NativeAdmissionError("Native scope progress is malformed") from error


class NativeScopeStore:
    """No commits; import authorization and scope mutation share the outer transaction."""

    def __init__(self, imports: NativeImportStore):
        """Use one canonical scan implementation for native and provider correctness."""
        self.imports = imports
        self.scans = CanonicalScanStore(imports.canonical)

    async def begin(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: BeginNativeScope,
    ) -> NativeScopeState:
        """Derive current parent authority server-side; do not reset same-import scans silently."""
        imported = await self.imports.active(db, organization_id, source_id, request_key)
        _, cycle = await attest_cycle(db, imported.fence, imported.cycle_id)
        parent = await scope_owner(db, imported.fence, cycle, request.scope)
        previous = await self.scans.read(db, imported.fence, request.scope)
        expected = request.expected
        if expected is None and previous is not None and previous.cycle_id != imported.cycle_id:
            # A new import explicitly replaces a preceding cycle, under the same writer lock.
            expected = previous.version
        state = await self.scans.begin(
            db,
            BeginScan(
                fence=imported.fence,
                scope=request.scope,
                cycle_id=imported.cycle_id,
                fingerprint=cycle.configuration.fingerprint,
                expected=expected,
                restart=request.restart,
                expected_parent_epoch=parent.visibility_epoch if parent else None,
            ),
        )
        return scope_state(state)

    async def read(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: NativeScopeRef,
    ) -> NativeScopeState:
        """Read retained scope progress only while its row still belongs to this import."""
        _, _, imported = await self.imports.load(db, organization_id, source_id, request_key)
        row = await self.scans._row(db, imported.fence, request.scope)
        if row is None or row.cycle_id != imported.cycle_id:
            raise NativeAdmissionError("Scope is unavailable or superseded for this import")
        state = (await self.scans._state(db, row)).model_copy(
            update={
                "completion_policy": imported.request.completion_policy(request.scope.record_type)
            }
        )
        return scope_state(state)

    async def reconcile(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: ReconcileNativeScope,
    ) -> NativeScopeState:
        """The canonical completion policy controls whether absence can withdraw records."""
        imported = await self.imports.active(db, organization_id, source_id, request_key)
        result = await self.scans.reconcile(
            db,
            ReconcileScan(
                fence=imported.fence,
                scope=request.scope,
                cycle_id=imported.cycle_id,
                expected=request.expected,
                observed_at=datetime.now(timezone.utc),
                limit=request.limit,
            ),
        )
        return scope_state(result.state)
