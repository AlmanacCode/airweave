"""Capture pipeline: native records commit before independent search projection."""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.events.sync import EntityBatchProcessedEvent, TypeActionCounts
from airweave.core.protocols.event_bus import EventBus
from airweave.domains.entities.canonical.cycle_models import (
    CaptureCycle,
    CompleteCycle,
    CycleConfiguration,
)
from airweave.domains.entities.canonical.models import CaptureResult
from airweave.domains.entities.canonical.page_source import CanonicalPageSource
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CaptureRecord,
    CompletedScope,
    ReconcileScope,
    RecordIdentity,
    RemovedScope,
    StartedScope,
    WriterFence,
)
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.source import SourceObservation
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.domains.sync_pipeline.contexts import SyncContext
from airweave.domains.sync_pipeline.contexts.runtime import SyncRuntime
from airweave.domains.sync_pipeline.exceptions import SyncFailureError
from airweave.domains.sync_pipeline.pipeline.cleanup_service import cleanup_service


class CanonicalCapturePipeline:
    """One source writer, one record authority; no legacy sink dispatcher participates."""

    def __init__(
        self,
        service: CanonicalCaptureService,
        sessions: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        event_bus: EventBus,
        record_types: tuple[str, ...],
        attempt: CaptureAttempt,
        container_parents: dict[str, str | tuple[str | None, ...]] | None = None,
        page_source: CanonicalPageSource | None = None,
        files: FileService | None = None,
    ):
        """Inject transactional persistence and existing progress-event infrastructure."""
        if not record_types:
            raise ValueError("Canonical source must declare at least one audited record type")
        self._service = service
        self._sessions = sessions
        self._event_bus = event_bus
        self._record_types = frozenset(record_types)
        self._container_parents = dict(container_parents or {})
        self.page_source = page_source
        self.files = files
        self._cycle: CaptureCycle | None = None
        if page_source is not None:
            config = page_source.capture_cycle_configuration
            declared = CycleConfiguration.from_source(
                fingerprint=config.fingerprint,
                record_types=record_types,
                container_parents=self._container_parents,
            )
            if config != declared:
                raise ValueError("Page source cycle must match its declared container topology")
        else:
            for child, parent in self._container_parents.items():
                if (
                    child not in self._record_types
                    or not isinstance(parent, str)
                    or parent not in self._record_types
                ):
                    raise ValueError(
                        "Container relationships must name declared canonical record types"
                    )
                if parent in self._container_parents:
                    raise ValueError("Nested sources require the durable page capability")
        self._attempt = attempt
        self._fence: WriterFence | None = None
        self._completed_scopes: set[CompletedScope] = set()
        self._batch_sequence = 0
        self._removed_scopes: set[CompletedScope] = set()

    async def start(self, sync_context: SyncContext) -> None:
        """Fence the activity after RUNNING transition and before fetching provider data."""
        async with self._sessions() as db:
            self._fence = await self._service.activate_writer(
                db,
                sync_context.organization_id,
                sync_context.sync_id,
                sync_context.sync_job_id,
                attempt_id=self._attempt.id,
                attempt_number=self._attempt.number,
            )

    async def run_scans(
        self,
        sync_context: SyncContext,
        runtime: SyncRuntime,
        check_limits: Callable[[], Awaitable[None]],
    ) -> None:
        """Run opted-in pages with a commit barrier before every next provider call."""
        if self.files is None:
            raise SyncFailureError("Page capture requires the source file service")
        if self.page_source is None:
            raise SyncFailureError("Source has no durable page capability")
        if sync_context.execution_config.cursor.skip_updates:
            raise SyncFailureError("Durable page capture cannot disable checkpoint updates")

        async def progress(result: CaptureResult, records: tuple[CaptureRecord, ...]) -> None:
            for record in records:
                await runtime.entity_tracker.track_entity(
                    record.identity.record_type, record.identity.entity_key
                )
            await self._record_progress(result, sync_context, runtime)

        self._cycle = await CanonicalScanDriver(
            self._service,
            self._sessions,
            self._writer(),
            self.page_source,
            progress,
            check_limits,
            self.files,
        ).run()

    def _writer(self) -> WriterFence:
        if self._fence is None:
            raise SyncFailureError("Canonical capture was not activated")
        return self._fence

    async def process(
        self, entities: list[SourceObservation], sync_context: SyncContext, runtime: SyncRuntime
    ) -> None:
        """Persist original source observations in stream order; never infer raw data."""
        records: list[CaptureRecord] = []
        for observation in entities:
            if isinstance(observation, CaptureRecord):
                record_type = observation.identity.record_type
                scope = CompletedScope(
                    record_type=record_type, container_id=observation.identity.container_id
                )
                if scope in self._removed_scopes:
                    raise SyncFailureError("Source yielded a record after confirmed scope loss")
                records.append(self._with_parent(observation))
            elif isinstance(observation, (StartedScope, RemovedScope, CompletedScope)):
                record_type = observation.record_type
            else:
                raise SyncFailureError("Canonical source yielded a non-observation value")
            if record_type not in self._record_types:
                raise SyncFailureError(f"Undeclared canonical record type: {record_type}")
            if isinstance(observation, CaptureRecord):
                continue
            # A lifecycle marker takes effect after all prior records and before following ones.
            await self._capture_records(records, sync_context, runtime)
            records = []
            await self._apply_scope(observation, sync_context, runtime)
        await self._capture_records(records, sync_context, runtime)

    def _with_parent(self, record: CaptureRecord) -> CaptureRecord:
        parent_type = self._container_parents.get(record.identity.record_type)
        if parent_type is None:
            return record
        if record.identity.container_id is None:
            raise SyncFailureError("Container-scoped canonical record is missing its container ID")
        parent = RecordIdentity(record_type=parent_type, native_id=record.identity.container_id)
        if record.parent is not None and record.parent != parent:
            raise SyncFailureError("Record parent conflicts with its declared container identity")
        return record.model_copy(update={"parent": parent})

    async def _apply_scope(
        self,
        observation: StartedScope | RemovedScope | CompletedScope,
        sync_context: SyncContext,
        runtime: SyncRuntime,
    ) -> None:
        """Apply a scope boundary after earlier records have committed."""
        scope = CompletedScope(
            record_type=observation.record_type, container_id=observation.container_id
        )
        if isinstance(observation, StartedScope):
            self._completed_scopes.discard(scope)
            self._removed_scopes.discard(scope)
            async with self._sessions() as db:
                await self._service.start_scope(db, self._writer(), observation)
        elif isinstance(observation, RemovedScope):
            self._completed_scopes.discard(scope)
            self._removed_scopes.add(scope)
            while True:
                async with self._sessions() as db:
                    result = await self._service.remove_scope(db, self._writer(), observation)
                if result.capture.changes:
                    await self._record_progress(result.capture, sync_context, runtime)
                if not result.has_more:
                    break
        else:
            if scope in self._removed_scopes:
                raise SyncFailureError("Removed scope cannot be marked complete without a new scan")
            self._completed_scopes.add(scope)

    async def _capture_records(
        self, records: list[CaptureRecord], sync_context: SyncContext, runtime: SyncRuntime
    ) -> None:
        # Keep bounded DB transactions independently of stream micro-batch tuning.
        for offset in range(0, len(records), 500):
            chunk = tuple(records[offset : offset + 500])
            async with self._sessions() as db:
                result = await self._service.capture(
                    db, CaptureBatch(fence=self._writer(), records=chunk)
                )
            for record in chunk:
                await runtime.entity_tracker.track_entity(
                    record.identity.record_type, record.identity.entity_key
                )
            await self._record_progress(result, sync_context, runtime)

    async def _record_progress(
        self, result: CaptureResult, ctx: SyncContext, runtime: SyncRuntime
    ) -> None:
        counts: dict[str, TypeActionCounts] = {}
        inserted = updated = deleted = 0
        for change in result.changes:
            record_type = change.record.identity.record_type
            old = counts.get(record_type, TypeActionCounts())
            if change.kind == "delete":
                deleted += 1
                counts[record_type] = old.model_copy(update={"deleted": old.deleted + 1})
                await runtime.entity_tracker.record_deletes(record_type)
            elif change.record.revision == 1:
                inserted += 1
                counts[record_type] = old.model_copy(update={"inserted": old.inserted + 1})
                await runtime.entity_tracker.record_inserts(record_type)
            else:
                updated += 1
                counts[record_type] = old.model_copy(update={"updated": old.updated + 1})
                await runtime.entity_tracker.record_updates(record_type)
        await runtime.entity_tracker.record_kept(result.unchanged)
        self._batch_sequence += 1
        await self._event_bus.publish(
            EntityBatchProcessedEvent(
                organization_id=ctx.organization_id,
                sync_id=ctx.sync_id,
                sync_job_id=ctx.sync_job_id,
                collection_id=ctx.collection_id,
                source_connection_id=ctx.source_connection_id,
                source_type=ctx.source_short_name,
                inserted=inserted,
                updated=updated,
                deleted=deleted,
                kept=result.unchanged,
                type_breakdown=counts,
                batch_seq=self._batch_sequence,
            )
        )

    async def cleanup_orphaned_entities(
        self, sync_context: SyncContext, runtime: SyncRuntime
    ) -> None:
        """Reconcile only explicit successful scopes after the entire capture barrier."""
        for scope in sorted(
            self._completed_scopes, key=lambda s: (s.record_type, s.container_id or "")
        ):
            while True:
                async with self._sessions() as db:
                    result = await self._service.reconcile_scope(
                        db,
                        ReconcileScope(
                            fence=self._writer(),
                            scope=scope,
                            # Missing containers withdraw content scope; absence does
                            # not prove provider deletion or credential revocation.
                            removal_reason=(
                                "scope_removed"
                                if scope.record_type in self._container_parents.values()
                                else "absent"
                            ),
                            observed_at=datetime.now(timezone.utc),
                        ),
                    )
                if result.capture.changes:
                    await self._record_progress(result.capture, sync_context, runtime)
                if not result.has_more:
                    break
        await self._reconcile_parents(sync_context, runtime)

    async def _reconcile_parents(self, sync_context: SyncContext, runtime: SyncRuntime) -> None:
        while True:
            async with self._sessions() as db:
                result = await self._service.reconcile_parents(db, self._writer())
            if result.capture.changes:
                await self._record_progress(result.capture, sync_context, runtime)
            if not result.has_more:
                break

    async def save_checkpoint(self, sync_context: SyncContext, runtime: SyncRuntime) -> None:
        """Commit cursor only after source success, durable captures and scoped reconciliation."""
        if self.page_source is not None:
            if self._cycle is None:
                raise SyncFailureError("Page capture did not reach its completion barrier")
            async with self._sessions() as db:
                current = await self._service.read_cycle(db, self._writer())
            if current != self._cycle:
                raise SyncFailureError("Cycle changed before source finalization")
            if current.phase == "complete":
                if current.completed_job_id != self._writer().job_id:
                    raise SyncFailureError("Completed cycle belongs to another job")
                return
            async with self._sessions() as db:
                self._cycle = await self._service.complete_cycle(
                    db, CompleteCycle(fence=self._writer(), expected=current.version)
                )
            return
        if runtime.cursor is None or not runtime.cursor.cursor_data:
            return
        if sync_context.execution_config and sync_context.execution_config.cursor.skip_updates:
            return
        async with self._sessions() as db:
            await self._service.save_checkpoint(db, self._writer(), runtime.cursor.cursor_data)

    async def cleanup_temp_files(self, sync_context: SyncContext, runtime: SyncRuntime) -> None:
        """Reuse source-job temporary-file cleanup; immutable blobs are not temporary."""
        await cleanup_service.cleanup_temp_files(sync_context, runtime)
