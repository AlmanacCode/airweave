"""Sequential page driver inside the existing capture job, without provider prefetch."""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CaptureCycle, RestartCycle
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.models import CaptureResult, SourceRecord
from airweave.domains.entities.canonical.page_source import (
    CanonicalPageSource,
    CapturePlan,
    CheckpointedPageSource,
    InvalidCaptureCheckpoint,
    InvalidScanContinuation,
    KnownObjectSource,
    ScopeAccessLost,
    ScopeRemovalReason,
)
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    WriterFence,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitOmission,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
    ScanState,
)
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.storage.file_service import FileService


class CanonicalScanDriver:
    """Provider calls occur between existing fenced service transactions, never within one."""

    def __init__(
        self,
        service: CanonicalCaptureService,
        sessions: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        fence: WriterFence,
        source: CanonicalPageSource,
        progress: Callable[[CaptureResult, tuple[CaptureRecord, ...]], Awaitable[None]],
        check_limits: Callable[[], Awaitable[None]],
        files: FileService,
    ):
        """Reuse the capture service and pipeline progress/guard callbacks."""
        self.service, self.sessions, self.fence, self.source = service, sessions, fence, source
        self.files = files
        self.progress, self.check_limits = progress, check_limits

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
        plan = (
            CapturePlan(mode=current.mode, starting_checkpoint=current.starting_checkpoint)
            if current is not None and current.phase == "active"
            else (
                await self.source.prepare_cycle(current)
                if isinstance(self.source, CheckpointedPageSource)
                else CapturePlan()
            )
        )
        async with self.sessions() as db:
            cycle = await self.service.begin_cycle(
                db,
                BeginCycle(
                    fence=self.fence,
                    configuration=configuration,
                    expected=expected,
                    mode=plan.mode,
                    starting_checkpoint=plan.starting_checkpoint,
                ),
            )
        for restart in range(2):
            try:
                return await self.run_cycle(cycle)
            except InvalidCaptureCheckpoint:
                if restart or not isinstance(self.source, CheckpointedPageSource):
                    raise
                await self.check_limits()
                plan = await self.source.prepare_cycle(None)
                if plan.mode != "full":
                    raise CycleConflict("An invalid checkpoint requires a fresh full capture")
                async with self.sessions() as db:
                    current = await self.service.read_cycle(db, self.fence)
                    if current is None or current.version.cycle_id != cycle.version.cycle_id:
                        raise CycleConflict("Cycle changed during checkpoint recovery")
                    cycle = await self.service.restart_cycle(
                        db,
                        RestartCycle(
                            fence=self.fence,
                            expected=current.version,
                            configuration=configuration,
                            mode=plan.mode,
                            starting_checkpoint=plan.starting_checkpoint,
                        ),
                    )
        raise AssertionError("Unreachable checkpoint recovery")

    async def run_cycle(self, cycle: CaptureCycle) -> CaptureCycle:
        """Process the current frontier; every acknowledgement remains in the existing SQL store."""
        configuration = cycle.configuration
        while True:
            async with self.sessions() as db:
                work = await self.service.next_scope_work(db, self.fence, cycle.version.cycle_id)
            if work is None:
                break
            scope = (
                CompletedScope(record_type=work.record_type)
                if work.parent is None
                else self.source.child_scope(work.parent, work.record_type)
            )
            if scope.record_type != work.record_type or scope.parent != (
                work.parent.identity if work.parent else None
            ):
                raise CycleConflict("Source returned a scope with the wrong exact owner")
            try:
                await self.scan(
                    cycle,
                    scope,
                    parent=work.parent,
                    parent_epoch=work.parent_visibility_epoch,
                    refresh_membership=bool(configuration.children_of(work.record_type)),
                )
            except ScopeAccessLost as error:
                if work.parent is None:
                    raise
                if work.parent_visibility_epoch is None:
                    raise CycleConflict("Scope owner epoch is missing") from error
                await self.withdraw_parent(
                    work.parent, work.parent_visibility_epoch, error.removal_reason
                )
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
        parent: SourceRecord | None = None,
        parent_epoch: int | None = None,
    ) -> None:
        """Only a provider invalid-cursor response permits one explicit sweep restart."""
        async with self.sessions() as db:
            previous = await self.service.read_scan(db, self.fence, scope)
        restart = bool(
            previous
            and previous.cycle_id == cycle.version.cycle_id
            and (
                (refresh_membership and previous.membership_attempt_id != self.fence.attempt_id)
                or previous.parent_visibility_epoch != parent_epoch
            )
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
                    continuation=self.initial_continuation(cycle),
                ),
            )
        restarts = 0
        while state.phase == "collecting":
            await self.check_limits()
            try:
                page = await self.source.capture_page(
                    scope, state.continuation, files=self.files, parent=parent
                )
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
                            continuation=self.initial_continuation(cycle),
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
                        records=tuple(self._parented(r, scope) for r in page.records),
                        discovered_records=page.discovered_records,
                        continuation=page.continuation,
                        final=page.final,
                        provider_checkpoint=page.provider_checkpoint,
                    ),
                )
            state = result.state
            await self.progress(result.capture, (*page.records, *page.discovered_records))
        if state.phase == "reconciling":
            if state.completion_policy == "discovery_with_validation" or (
                state.scope.record_type in cycle.configuration.known_object_validation
            ):
                state = await self.refresh_known(state)
            elif state.completion_policy == "exhaustive" and refresh_membership:
                await self.confirm_omissions(state)
        await self.reconcile(state)

    def initial_continuation(self, cycle: CaptureCycle) -> ScanContinuation:
        """Sources without a native changes contract retain the empty initial cursor."""
        return (
            self.source.initial_continuation(cycle)
            if isinstance(self.source, CheckpointedPageSource)
            else ScanContinuation()
        )

    @staticmethod
    def _parented(record: CaptureRecord, scope: CompletedScope) -> CaptureRecord:
        """Legacy flat adapters may omit the parent; never overwrite a conflicting declaration."""
        if record.parent is not None and record.parent != scope.parent:
            raise CycleConflict("Captured record declares a different scope parent")
        return record.model_copy(update={"parent": scope.parent})

    async def refresh_known(self, state: ScanState) -> ScanState:
        """SQL sightings are the durable frontier; uncertain commits end this attempt."""
        if not isinstance(self.source, KnownObjectSource):
            raise CycleConflict("Source does not implement exact known-object validation")
        while True:
            async with self.sessions() as db:
                records = await self.service.scan_missing(db, self.fence, state)
            if not records:
                return state
            for record in records:
                await self.check_limits()
                observation = await self.source.refresh_known(record, files=self.files)
                async with self.sessions() as db:
                    result = await self.service.commit_omission(
                        db,
                        CommitOmission(
                            fence=self.fence,
                            state=state,
                            expected_record=record,
                            observation=observation,
                        ),
                    )
                state = result.state
                await self.progress(result.capture, (observation,))

    async def confirm_omissions(self, state: ScanState) -> None:
        """Accessible omissions or provider errors fail before any absence removal."""
        after: UUID | None = None
        while True:
            async with self.sessions() as db:
                records = await self.service.scan_missing(db, self.fence, state, after=after)
            if not records:
                return
            for record in records:
                await self.check_limits()
                await self.source.confirm_absent(record)
            after = records[-1].id

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

    async def withdraw_parent(
        self, parent: SourceRecord, expected_epoch: int, removal_reason: ScopeRemovalReason
    ) -> None:
        """Withdraw exact owner first; recursive visibility closes before bounded cleanup."""
        async with self.sessions() as db:
            result = await self.service.withdraw_scan_parent(
                db,
                self.fence,
                parent,
                expected_epoch=expected_epoch,
                removal_reason=removal_reason,
            )
        await self.progress(result, ())
        while True:
            await self.check_limits()
            async with self.sessions() as db:
                result = await self.service.reconcile_parents(db, self.fence)
            await self.progress(result.capture, ())
            if not result.has_more:
                return
