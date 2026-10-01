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
    InvalidScopeCheckpoint,
    KnownObjectSource,
    RequiredScopeAccessLost,
    ScopeAccessLost,
    ScopedPageSource,
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
    ScanAdmission,
    ScanContinuation,
    ScanState,
)
from airweave.domains.entities.canonical.scope_execution import ScopePlan
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
        *,
        force_full: bool = False,
    ):
        """Reuse the capture service and pipeline progress/guard callbacks."""
        self.service, self.sessions, self.fence, self.source = service, sessions, fence, source
        self.files = files
        self.force_full = force_full
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
        if (
            self.force_full
            and isinstance(self.source, (CheckpointedPageSource, ScopedPageSource))
            and current is not None
            and current.phase == "active"
            and (
                current.mode == "changes"
                or (current.mode == "mixed" and not current.force_full_scopes)
                or current.configuration != configuration
            )
        ):
            cycle = await self.fresh_full(current)
        else:
            plan = (
                CapturePlan(
                    mode=current.mode,
                    starting_checkpoint=current.starting_checkpoint,
                    source_plan=current.source_plan,
                )
                if current is not None and current.phase == "active"
                else (
                    await self.source.prepare_cycle(None if self.force_full else current)
                    if isinstance(self.source, (CheckpointedPageSource, ScopedPageSource))
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
                        source_plan=plan.source_plan,
                        force_full_scopes=(
                            current.force_full_scopes
                            if current and current.phase == "active"
                            else self.force_full
                        )
                        if plan.mode == "mixed"
                        else False,
                    ),
                )
        for restart in range(2):
            try:
                return await self.run_cycle(cycle)
            except InvalidCaptureCheckpoint:
                if restart or not isinstance(
                    self.source, (CheckpointedPageSource, ScopedPageSource)
                ):
                    raise
                await self.check_limits()
                cycle = await self.fresh_full(cycle)
        raise AssertionError("Unreachable checkpoint recovery")

    async def fresh_full(self, previous: CaptureCycle) -> CaptureCycle:
        """Explicit full requests and native expiry share the same exact-cycle restart."""
        if not isinstance(self.source, (CheckpointedPageSource, ScopedPageSource)):
            raise CycleConflict("Source has no checkpoint recovery contract")
        plan = await self.source.prepare_cycle(None)
        if plan.mode != "full" and not (
            plan.mode == "mixed" and isinstance(self.source, ScopedPageSource)
        ):
            raise CycleConflict("Checkpoint recovery requires a fresh full capture")
        async with self.sessions() as db:
            current = await self.service.read_cycle(db, self.fence)
            if current is None or current.version.cycle_id != previous.version.cycle_id:
                raise CycleConflict("Cycle changed during checkpoint recovery")
            return await self.service.restart_cycle(
                db,
                RestartCycle(
                    fence=self.fence,
                    expected=current.version,
                    configuration=self.source.capture_cycle_configuration,
                    mode=plan.mode,
                    starting_checkpoint=plan.starting_checkpoint,
                    source_plan=plan.source_plan,
                    force_full_scopes=plan.mode == "mixed",
                ),
            )

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
                if isinstance(error, RequiredScopeAccessLost):
                    raise
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
        admission = await self.begin_scope(cycle, scope, parent, parent_epoch)
        if admission.state is None:
            return
        state = admission.state
        parent = admission.parent
        restarts = 0
        while state.phase == "collecting":
            await self.check_limits()
            try:
                page = await self.source.capture_page(
                    scope, state.continuation, files=self.files, parent=parent
                )
            except (InvalidScanContinuation, InvalidScopeCheckpoint) as error:
                if restarts >= 1:
                    raise
                restarts += 1
                state = await self.restart_scope(cycle, scope, state, parent, error)
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

    async def begin_scope(
        self,
        cycle: CaptureCycle,
        scope: CompletedScope,
        parent: SourceRecord | None,
        parent_epoch: int | None,
    ) -> ScanAdmission:
        """Select a plan outside SQL, then attest prior version and owner under the fence."""
        async with self.sessions() as db:
            previous = await self.service.read_scan(db, self.fence, scope)
        restart = bool(
            previous
            and previous.cycle_id == cycle.version.cycle_id
            and (
                (
                    cycle.configuration.fresh_inventory(scope.record_type)
                    and previous.membership_attempt_id != self.fence.attempt_id
                )
                or previous.parent_visibility_epoch != parent_epoch
            )
        )
        scoped = isinstance(self.source, ScopedPageSource)
        if cycle.mode == "mixed" and cycle.configuration.exact_parent_validation:
            raise CycleConflict("Mixed planning does not support exact parent validation")
        if cycle.mode == "mixed" and not scoped:
            raise CycleConflict("Mixed capture requires source scope planning")
        if scoped and cycle.force_full_scopes and previous and previous.mode == "changes":
            restart = True
        plan = None
        if scoped:
            if previous and previous.cycle_id == cycle.version.cycle_id and not restart:
                if previous.execution is None:
                    raise CycleConflict("Mixed scope lost its persisted plan")
                plan = previous.execution.plan
            else:
                plan = await self.source.prepare_scope(
                    scope,
                    cycle,
                    previous,
                    parent=parent,
                    force_full=cycle.force_full_scopes
                    or bool(previous and previous.parent_visibility_epoch != parent_epoch),
                )
        observation = None
        if parent and parent.identity.record_type in cycle.configuration.exact_parent_validation:
            verified = bool(
                previous
                and previous.cycle_id == cycle.version.cycle_id
                and previous.parent_verified_attempt_id == self.fence.attempt_id
                and previous.parent_verified_revision == parent.revision
                and previous.parent_visibility_epoch == parent_epoch
            )
            if not verified:
                if not isinstance(self.source, KnownObjectSource):
                    raise CycleConflict("Source requires exact owner refresh support")
                await self.check_limits()
                observation = await self.source.refresh_known(parent, files=self.files)
        initial = self.scope_initial(scope, cycle, plan)
        async with self.sessions() as db:
            admission = await self.service.admit_scan(
                db,
                BeginScan(
                    fence=self.fence,
                    scope=scope,
                    cycle_id=cycle.version.cycle_id,
                    fingerprint=cycle.configuration.fingerprint,
                    expected=previous.version if previous else None,
                    restart=restart,
                    continuation=initial,
                    plan=plan,
                    expected_parent_epoch=parent_epoch,
                    expected_parent_revision=parent.revision if parent else None,
                    exact_parent_observation=observation,
                ),
            )

        await self.progress(admission.capture, (observation,) if observation else ())
        return admission

    async def restart_scope(
        self,
        cycle: CaptureCycle,
        scope: CompletedScope,
        state: ScanState,
        parent: SourceRecord | None,
        error: Exception,
    ) -> ScanState:
        """Native cursor expiry can reset one sweep; native sync expiry forces full scope mode."""
        plan = state.execution.plan if state.execution else None
        if isinstance(error, InvalidScopeCheckpoint):
            if not isinstance(self.source, ScopedPageSource):
                raise error
            plan = await self.source.prepare_scope(
                scope, cycle, state, parent=parent, force_full=True
            )
            if plan.mode != "full":
                raise CycleConflict("Invalid scope checkpoint requires a full scope restart")
        initial = self.scope_initial(scope, cycle, plan)
        async with self.sessions() as db:
            return await self.service.begin_scan(
                db,
                BeginScan(
                    fence=self.fence,
                    scope=scope,
                    cycle_id=state.cycle_id,
                    fingerprint=state.fingerprint,
                    expected=state.version,
                    restart=True,
                    continuation=initial,
                    plan=plan,
                    expected_parent_epoch=state.parent_visibility_epoch,
                    expected_parent_revision=parent.revision if parent else None,
                ),
            )

    def scope_initial(
        self, scope: CompletedScope, cycle: CaptureCycle, plan: ScopePlan | None
    ) -> ScanContinuation:
        """Sources receive exactly the plan that will be committed with the new sweep."""
        if isinstance(self.source, ScopedPageSource):
            if plan is None:
                raise CycleConflict("Mixed source requires a scope plan")
            return self.source.initial_scope_continuation(scope, cycle, plan)
        return self.initial_continuation(cycle)

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
