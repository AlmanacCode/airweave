"""Sequential page driver inside the existing capture job, without provider prefetch."""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CaptureCycle
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.models import CaptureResult
from airweave.domains.entities.canonical.page_source import (
    CanonicalPageSource,
    InvalidScanContinuation,
    ScopeAccessLost,
)
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    RemovedScope,
    WriterFence,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanState,
)
from airweave.domains.entities.canonical.service import CanonicalCaptureService


class CanonicalScanDriver:
    """Provider calls occur between existing fenced service transactions, never within one."""

    def __init__(
        self,
        service: CanonicalCaptureService,
        sessions: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        fence: WriterFence,
        source: CanonicalPageSource,
        progress: Callable[[CaptureResult, tuple[CaptureRecord, ...]], Awaitable[None]],
        with_parent: Callable[[CaptureRecord], CaptureRecord],
        check_limits: Callable[[], Awaitable[None]],
    ):
        """Reuse the capture service and pipeline progress/guard callbacks."""
        self.service, self.sessions, self.fence, self.source = service, sessions, fence, source
        self.progress, self.with_parent, self.check_limits = progress, with_parent, check_limits

    async def run(self) -> CaptureCycle:
        """Refresh membership first, then resume its currently visible child scopes."""
        configuration = self.source.capture_cycle_configuration
        async with self.sessions() as db:
            current = await self.service.read_cycle(db, self.fence)
        if current is not None and current.phase == "complete":
            if current.completed_job_id == self.fence.job_id:
                return current
            expected = current.version
        else:
            expected = None
        async with self.sessions() as db:
            cycle = await self.service.begin_cycle(
                db, BeginCycle(fence=self.fence, configuration=configuration, expected=expected)
            )
        root = CompletedScope(record_type=configuration.root_record_type)
        await self.scan(cycle, root, refresh_membership=True)
        after: UUID | None = None
        while True:
            async with self.sessions() as db:
                roots = await self.service.list_cycle_roots(
                    db, self.fence, cycle.version.cycle_id, after=after
                )
            if not roots:
                break
            for item in roots:
                for child_type in configuration.child_record_types:
                    scope = CompletedScope(record_type=child_type, container_id=item.native_id)
                    try:
                        await self.scan(cycle, scope)
                    except ScopeAccessLost:
                        await self.withdraw_root(
                            configuration.root_record_type,
                            item.native_id,
                            configuration.child_record_types,
                        )
                        break
            after = roots[-1].id
        async with self.sessions() as db:
            current = await self.service.read_cycle(db, self.fence)
        if current is None:
            raise CycleConflict("Capture cycle disappeared before finalization")
        return current

    async def scan(
        self,
        cycle: CaptureCycle,
        scope: CompletedScope,
        *,
        refresh_membership: bool = False,
    ) -> None:
        """Only a provider invalid-cursor response permits one explicit sweep restart."""
        async with self.sessions() as db:
            previous = await self.service.read_scan(db, self.fence, scope)
        restart = bool(
            previous
            and previous.cycle_id == cycle.version.cycle_id
            and refresh_membership
            and cycle.root_writer_attempt_id != self.fence.attempt_id
        )
        async with self.sessions() as db:
            state = await self.service.begin_scan(
                db,
                BeginScan(
                    fence=self.fence,
                    scope=scope,
                    cycle_id=cycle.version.cycle_id,
                    fingerprint=cycle.configuration.fingerprint,
                    expected=previous.version if previous else None,
                    restart=restart,
                ),
            )
        restarts = 0
        while state.phase == "collecting":
            await self.check_limits()
            try:
                page = await self.source.capture_page(scope, state.continuation)
            except InvalidScanContinuation:
                if restarts >= 1:
                    raise
                restarts += 1
                async with self.sessions() as db:
                    state = await self.service.begin_scan(
                        db,
                        BeginScan(
                            fence=self.fence,
                            scope=scope,
                            cycle_id=state.cycle_id,
                            fingerprint=state.fingerprint,
                            expected=state.version,
                            restart=True,
                        ),
                    )
                continue
            # Never split a page or retry an uncertain commit with this old version.
            async with self.sessions() as db:
                result = await self.service.commit_scan_page(
                    db,
                    CommitScanPage(
                        fence=self.fence,
                        scope=scope,
                        cycle_id=state.cycle_id,
                        expected=state.version,
                        records=tuple(self.with_parent(r) for r in page.records),
                        continuation=page.continuation,
                        final=page.final,
                    ),
                )
            state = result.state
            await self.progress(result.capture, page.records)
        if state.phase == "reconciling" and refresh_membership:
            await self.confirm_omissions(cycle)
        await self.reconcile(state)

    async def confirm_omissions(self, cycle: CaptureCycle) -> None:
        """A provider error or accessible omission fails before any absence removal."""
        after: UUID | None = None
        while True:
            async with self.sessions() as db:
                roots = await self.service.list_cycle_roots(
                    db, self.fence, cycle.version.cycle_id, after=after, missing=True
                )
            if not roots:
                return
            for item in roots:
                await self.check_limits()
                await self.source.confirm_root_absent(item.native_id)
            after = roots[-1].id

    async def reconcile(self, state: ScanState) -> None:
        """Each bounded removal batch resumes from its committed scan version."""
        while state.phase == "reconciling":
            await self.check_limits()
            async with self.sessions() as db:
                result = await self.service.reconcile_scan(
                    db,
                    ReconcileScan(
                        fence=self.fence,
                        scope=state.scope,
                        cycle_id=state.cycle_id,
                        expected=state.version,
                        observed_at=datetime.now(timezone.utc),
                    ),
                )
            state = result.state
            await self.progress(result.capture, ())

    async def withdraw_root(
        self, root_type: str, native_id: str, child_types: tuple[str, ...]
    ) -> None:
        """Explicit provider access loss hides the parent before bounded child cleanup."""
        observed = datetime.now(timezone.utc)
        async with self.sessions() as db:
            result = await self.service.capture(
                db,
                CaptureBatch(
                    fence=self.fence,
                    records=(
                        CaptureRecord(
                            identity=RecordIdentity(record_type=root_type, native_id=native_id),
                            payload={"id": native_id},
                            kind="delete",
                            removal_reason="access_revoked",
                            observed_at=observed,
                        ),
                    ),
                ),
            )
        await self.progress(result, ())
        for child_type in child_types:
            while True:
                await self.check_limits()
                async with self.sessions() as db:
                    result = await self.service.remove_scope(
                        db,
                        self.fence,
                        RemovedScope(
                            record_type=child_type,
                            container_id=native_id,
                            removal_reason="access_revoked",
                            observed_at=observed,
                        ),
                    )
                await self.progress(result.capture, ())
                if not result.has_more:
                    break
