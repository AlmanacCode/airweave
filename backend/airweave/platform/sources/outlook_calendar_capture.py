"""Calendar-owned Graph originals using the shared resumable full-scan engine.

Unbounded singles and masters precede bounded native expansion in the SAME event
scope. Previously captured occurrences remain retained while exact reads succeed;
the completed window describes expansion coverage, not a deletion boundary.
"""

import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlsplit

from pydantic import AwareDatetime, TypeAdapter, ValidationError

from airweave.domains.entities.canonical.cycle_models import CaptureCycle, CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import CapturePage, CapturePlan
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation, ScanState
from airweave.domains.entities.canonical.scope_execution import ScopePlan
from airweave.domains.storage.file_service import FileService
from airweave.platform.configs.config import CalendarOccurrenceWindow, OutlookCalendarConfig
from airweave.platform.sources.outlook_calendar_models import (
    OutlookCalendar,
    OutlookCalendarContinuation,
    OutlookCalendarEvent,
    OutlookCalendarPage,
    OutlookCalendarScopeContext,
)
from airweave.platform.sources.outlook_graph import OutlookBoundaryError, OutlookGraphClient

BASE = "https://graph.microsoft.com/v1.0/me"


class OutlookCalendarCapture:
    """Provider interpretation only; SQL owns progress, parent visibility and publication."""

    canonical_record_types = ("calendar", "event")
    canonical_container_parents = {"event": "calendar"}

    def __init__(self, graph: OutlookGraphClient, config: OutlookCalendarConfig) -> None:
        """Use create to attest the principal before obtaining this composition."""
        self.graph = graph
        self.config = config.model_copy(deep=True)
        self.fingerprint = hashlib.sha256(
            json.dumps(
                {"version": 1, "id_type": "ImmutableId", "config": config.model_dump(mode="json")},
                sort_keys=True,
            ).encode()
        ).hexdigest()

    @classmethod
    async def create(
        cls, *, graph: OutlookGraphClient, config: OutlookCalendarConfig
    ) -> "OutlookCalendarCapture":
        """Require trusted mailbox identity; credential lifecycle remains in GraphClient."""
        instance = cls(graph, config)
        if not config.expected_principal_id or (
            graph.expected_principal_id != config.expected_principal_id
        ):
            raise instance._error("Original Outlook calendars require a bound principal")
        await graph.verify_principal()
        instance._require_principal()
        return instance

    @staticmethod
    def _error(message: str) -> OutlookBoundaryError:
        return OutlookBoundaryError(message, source_short_name="outlook_calendar")

    def _require_principal(self) -> None:
        if not self.config.expected_principal_id or (
            self.graph.expected_principal_id != self.config.expected_principal_id
            or self.graph.verified_principal_id != self.config.expected_principal_id
        ):
            raise self._error("Original Outlook calendar principal is not attested")

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Neither listing omissions nor ambiguous404 imply provider deletion."""
        self._require_principal()
        return CycleConfiguration.from_source(
            fingerprint=self.fingerprint,
            record_types=self.canonical_record_types,
            container_parents=self.canonical_container_parents,
            completion_policies={
                "calendar": "discovery_with_validation",
                "event": "discovery_with_validation",
            },
        )

    async def prepare_cycle(self, previous: CaptureCycle | None) -> CapturePlan:
        """Resolve rolling bounds once; this first version does not claim incremental capture."""
        await self.graph.verify_principal()
        self._require_principal()
        return CapturePlan(
            mode="full",
            source_plan={"window": self.config.resolved_window().model_dump(mode="json")},
        )

    def _scope(self, scope: CompletedScope, parent: SourceRecord | None) -> None:
        root = scope.record_type == "calendar"
        if root and scope.container_id is None and scope.parent is None and parent is None:
            return
        if (
            scope.record_type == "event"
            and scope.parent is not None
            and scope.parent.record_type == "calendar"
            and scope.parent.container_id is None
            and scope.container_id == scope.parent.native_id
            and parent is not None
            and parent.identity == scope.parent
        ):
            return
        raise self._error("Outlook calendar scope does not match its native owner")

    async def prepare_scope(
        self,
        scope: CompletedScope,
        cycle: CaptureCycle,
        previous: ScanState | None,
        *,
        parent: SourceRecord | None,
        force_full: bool,
    ) -> ScopePlan:
        """Both phases share one full sweep and one immutable expansion window."""
        self._scope(scope, parent)
        if cycle.configuration != self.capture_cycle_configuration or cycle.mode != "full":
            raise self._error("Outlook calendar cycle belongs to another configuration")
        try:
            window = CalendarOccurrenceWindow.model_validate(cycle.source_plan["window"])
            context = OutlookCalendarScopeContext(
                fingerprint=self.fingerprint, calendar_id=scope.container_id, window=window
            )
            return ScopePlan(request_context=context.model_dump(mode="json"))
        except (ValidationError, KeyError):
            raise self._error("Invalid Outlook calendar scope plan") from None

    def initial_scope_continuation(
        self, scope: CompletedScope, cycle: CaptureCycle, plan: ScopePlan
    ) -> ScanContinuation:
        """Resume only the persisted scope request, never newly resolved wall-clock bounds."""
        self._require_principal()
        try:
            context = OutlookCalendarScopeContext.model_validate(plan.request_context)
        except ValidationError:
            raise self._error("Invalid Outlook calendar scope context") from None
        if (
            plan.mode != "full"
            or cycle.mode != "full"
            or plan.starting_checkpoint is not None
            or cycle.configuration != self.capture_cycle_configuration
            or context.fingerprint != self.fingerprint
            or context.calendar_id != scope.container_id
            or context.window.model_dump(mode="json") != cycle.source_plan.get("window")
        ):
            raise self._error("Outlook calendar scope context changed")
        return self._continuation(
            OutlookCalendarContinuation(
                context=context, phase="calendars" if scope.record_type == "calendar" else "events"
            )
        )

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """One native calendar owns all event kinds and their eventual attachment children."""
        if (
            parent.identity.record_type != "calendar"
            or parent.parent is not None
            or record_type != "event"
        ):
            raise self._error("Invalid Outlook calendar child scope")
        return CompletedScope(
            record_type="event", container_id=parent.identity.native_id, parent=parent.identity
        )

    def _continuation(self, state: OutlookCalendarContinuation) -> ScanContinuation:
        try:
            return ScanContinuation(value=state.model_dump(mode="json"))
        except ValidationError:
            raise self._error(
                "Outlook calendar continuation exceeds its bounded capacity"
            ) from None

    def _collection_url(self, url: str, collection: str) -> None:
        try:
            actual, expected = urlsplit(url), urlsplit(collection)
            if (
                actual.scheme == "https"
                and actual.netloc == "graph.microsoft.com"
                and unquote(actual.path) == unquote(expected.path)
                and not actual.fragment
                and not any(ord(char) < 32 for char in url)
            ):
                return
        except ValueError:
            pass
        raise self._error("Outlook calendar continuation changed its native collection")

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Commit a complete bounded page with its next phase; late failures do not advance it."""
        self._require_principal()
        self._scope(scope, parent)
        try:
            state = OutlookCalendarContinuation.model_validate(continuation.value)
        except ValidationError:
            raise self._error("Invalid Outlook calendar continuation") from None
        root = scope.record_type == "calendar"
        if (
            state.context.fingerprint != self.fingerprint
            or state.context.calendar_id != scope.container_id
            or (state.phase not in (("calendars",) if root else ("events", "calendarView")))
        ):
            raise self._error("Outlook calendar continuation belongs to another scope")
        collection = f"{BASE}/calendars"
        params: dict[str, str | int] | None = {"$top": 100, "$select": "id"}
        if not root:
            collection += f"/{quote(scope.container_id or '', safe='')}/{state.phase}"
            if state.phase == "calendarView":
                # Graph's default response representation is UTC. Preserve that native JSON.
                params = {
                    "$top": 100,
                    "startDateTime": state.context.window.start.isoformat(),
                    "endDateTime": state.context.window.end.isoformat(),
                }
        url = state.next_link or collection
        self._collection_url(url, collection)
        raw = await self.graph.get(
            url, params=None if state.next_link else params, immutable_ids=True
        )
        try:
            page = OutlookCalendarPage.model_validate(raw)
        except ValidationError:
            raise self._error("Invalid Outlook calendar collection response") from None
        ids = tuple(item.id for item in page.value)
        if len(set(ids)) != len(ids):
            raise self._error("Outlook calendar page repeated a native identity")
        progress = self._advance(state, page, collection, url)
        observations = []
        for native_id in ids:
            if root:
                observations.append(await self._calendar(native_id))
            else:
                observations.append(await self._event(scope.parent, native_id))
        return CapturePage(
            records=tuple(observations),
            continuation=progress,
            final=progress.value["phase"] == "done",
        )

    def _advance(
        self,
        state: OutlookCalendarContinuation,
        page: OutlookCalendarPage,
        collection: str,
        url: str,
    ) -> ScanContinuation:
        """Validate bounded progress before fetching exact originals."""
        links = state.recent_links
        if page.next_link:
            self._collection_url(page.next_link, collection)
            digest = hashlib.sha256(page.next_link.encode()).hexdigest()
            if page.next_link == url or digest in links:
                raise self._error("Outlook calendar pagination repeated a recent link")
            links = (*links, digest)[-128:]
        phase = (
            state.phase
            if page.next_link
            else ("calendarView" if state.phase == "events" else "done")
        )
        return self._continuation(
            state.model_copy(
                update={
                    "next_link": page.next_link,
                    "recent_links": links if page.next_link else (),
                    "phase": phase,
                }
            )
        )

    async def _calendar(self, native_id: str) -> CaptureRecord:
        raw = await self.graph.get(
            f"{BASE}/calendars/{quote(native_id, safe='')}", immutable_ids=True
        )
        try:
            calendar = OutlookCalendar.model_validate(raw)
        except ValidationError:
            raise self._error("Invalid Outlook calendar metadata") from None
        if calendar.id != native_id:
            raise self._error("Outlook calendar identity changed during capture")
        return CaptureRecord(
            identity=RecordIdentity(record_type="calendar", native_id=native_id),
            payload=raw,
            observed_at=datetime.now(timezone.utc),
            descendant_visibility_fields=("canViewPrivateItems",),
        )

    async def _event(self, parent: RecordIdentity | None, native_id: str) -> CaptureRecord:
        if parent is None:
            raise self._error("Outlook event original requires its calendar owner")
        calendar_url = f"{BASE}/calendars/{quote(parent.native_id, safe='')}"
        url = f"{calendar_url}/events/{quote(native_id, safe='')}"
        raw = await self.graph.get(url, immutable_ids=True)
        try:
            event = OutlookCalendarEvent.model_validate(raw)
            created = (
                TypeAdapter(AwareDatetime).validate_python(event.createdDateTime)
                if event.createdDateTime
                else None
            )
            updated = (
                TypeAdapter(AwareDatetime).validate_python(event.lastModifiedDateTime)
                if event.lastModifiedDateTime
                else None
            )
        except ValidationError:
            raise self._error("Invalid Outlook event metadata") from None
        if event.id != native_id:
            raise self._error("Outlook event identity changed during capture")
        return CaptureRecord(
            identity=RecordIdentity(
                record_type="event", native_id=native_id, container_id=parent.native_id
            ),
            parent=parent,
            payload=raw,
            observed_at=datetime.now(timezone.utc),
            source_created_at=created,
            source_updated_at=updated,
            # Native attachments and selected-only master cancellation metadata are not
            # retained yet. hasAttachments=false must not certify full content capture.
            completeness="partial",
        )

    async def refresh_known(self, record: SourceRecord, *, files: FileService) -> CaptureRecord:
        """Historical instances remain originals; an ambiguous404 fails before reconciliation."""
        self._require_principal()
        if (
            record.identity.record_type == "calendar"
            and record.parent is None
            and record.identity.container_id is None
        ):
            return await self._calendar(record.identity.native_id)
        if (
            record.identity.record_type == "event"
            and record.parent is not None
            and record.parent.record_type == "calendar"
            and record.parent.container_id is None
            and record.identity.container_id == record.parent.native_id
        ):
            return await self._event(record.parent, record.identity.native_id)
        raise self._error("Invalid Outlook exact-read identity")

    async def confirm_absent(self, record: SourceRecord) -> None:
        """No arbitrary Graph failure establishes authoritative native absence."""
        raise self._error("Outlook calendar omissions require exact current observation")
