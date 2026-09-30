"""Native Calendar pages under the existing fenced mixed-scope capture engine."""

import hashlib
import json
from typing import Literal
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airweave.domains.entities.canonical.calendar import CalendarScopeContext
from airweave.domains.entities.canonical.cycle_models import (
    CaptureCycle,
    CycleConfiguration,
    ProviderCheckpoint,
)
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    CapturePlan,
    InvalidScopeCheckpoint,
    RequiredScopeAccessLost,
    ScopeAccessLost,
)
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation, ScanState
from airweave.domains.entities.canonical.scope_execution import ScopePlan
from airweave.domains.sources.exceptions import SourceEntityNotFoundError, SourceGoneError
from airweave.platform.configs.config import CalendarOccurrenceWindow, GoogleCalendarConfig
from airweave.platform.sources.records.google_calendar import BASE, CalendarPage, GetJSON, record


class CalendarProgress(BaseModel):
    """Bounded per-scope continuation; original payloads remain only in the store."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    mode: Literal["full", "changes"]
    context: CalendarScopeContext
    sync_token: str | None = None
    page_token: str | None = None
    recent_tokens: tuple[str, ...] = Field(default=(), max_length=128)
    selected_seen: tuple[str, ...] = Field(default=(), max_length=250)
    selection_unavailable: bool = False
    pages: int = Field(default=0, ge=0)
    records: int = Field(default=0, ge=0)
    bytes: int = Field(default=0, ge=0)

    def continuation(self) -> ScanContinuation:
        """Validate the complete continuation against the engine byte budget."""
        return ScanContinuation(value=self.model_dump(mode="json"))


class CalendarPages:
    """Acquisition only; SQL owns every page, scope publication and cycle transition."""

    def __init__(self, get: GetJSON, config: GoogleCalendarConfig):
        """Reuse the source authenticated native HTTP boundary."""
        self.get, self.config = get, config

    @property
    def configuration(self) -> CycleConfiguration:
        """Fingerprint native selection and capture semantics, not moving wall-clock bounds."""
        raw = {"version": 3, "config": self.config.model_dump(mode="json")}
        return CycleConfiguration(
            fingerprint=hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest(),
            parents={
                "calendar": (None,),
                "event": ("calendar",),
                "event_occurrence": ("calendar",),
            },
            scope_changes=("event",),
        )

    def prepare(self) -> CapturePlan:
        """Resolve a rolling window once, before the durable cycle is created."""
        return CapturePlan(
            mode="mixed",
            source_plan={"window": self.config.resolved_window().model_dump(mode="json")},
        )

    def scope_plan(
        self,
        scope: CompletedScope,
        cycle: CaptureCycle,
        previous: ScanState | None,
        *,
        parent: SourceRecord | None,
        force_full: bool,
    ) -> ScopePlan:
        """Reuse native tokens only with compatible completed full and current publication."""
        params: dict[str, str | int] = {"maxResults": 250, "showHidden": "true"}
        timezone = None
        if scope.record_type != "calendar":
            if (
                parent is None
                or parent.identity != scope.parent
                or scope.container_id != parent.identity.native_id
            ):
                raise ValueError("Calendar child requires its exact native owner")
            params = {"maxResults": 250, "showDeleted": "true", "singleEvents": "false"}
            if scope.record_type == "event_occurrence":
                window = CalendarOccurrenceWindow.model_validate(cycle.source_plan["window"])
                timezone = parent.payload.get("timeZone")
                if not isinstance(timezone, str) or not timezone:
                    raise ValueError("Calendar has no timezone for occurrence capture")
                params.update(
                    singleEvents="true",
                    timeMin=window.start.isoformat(),
                    timeMax=window.end.isoformat(),
                    timeZone=timezone,
                )
            elif scope.record_type != "event":
                raise ValueError("Unknown Calendar scope")
        context = CalendarScopeContext(parameters=params, timezone=timezone).model_dump(mode="json")
        proposed = ScopePlan(request_context=context)
        execution = previous.execution if previous else None
        if (
            scope.record_type == "event"
            and not force_full
            and previous
            and previous.fingerprint == cycle.configuration.fingerprint
            and execution
            and execution.last_full
            and execution.published
            and execution.last_full.policy == "exhaustive"
            and execution.published.policy == "exhaustive"
            and execution.last_full.matches(proposed, previous.parent_visibility_epoch)
            and execution.published.matches(proposed, previous.parent_visibility_epoch)
            and execution.published.checkpoint
        ):
            return proposed.model_copy(
                update={"mode": "changes", "starting_checkpoint": execution.published.checkpoint}
            )
        return proposed

    @staticmethod
    def initial(plan: ScopePlan) -> ScanContinuation:
        """Only the persisted scope plan determines native page parameters."""
        token = None
        if plan.starting_checkpoint:
            token = plan.starting_checkpoint.value.get("sync_token")
            if not isinstance(token, str) or not token:
                raise ValueError("Calendar scope checkpoint has no native sync token")
        return CalendarProgress(
            mode=plan.mode,
            context=CalendarScopeContext.model_validate(plan.request_context),
            sync_token=token,
        ).continuation()

    async def page(self, scope: CompletedScope, continuation: ScanContinuation) -> CapturePage:
        """Fetch bounded native data; no cursor or coverage writes happen here."""
        state = CalendarProgress.model_validate(continuation.value)
        if state.selection_unavailable:
            raise ValueError("A requested calendar is not accessible in current membership")
        native = await self._fetch(scope, state)
        root = scope.record_type == "calendar"
        if len(native.items) > 250 or (native.nextPageToken and native.nextSyncToken):
            raise ValueError("Calendar returned an invalid bounded page")
        observations = tuple(
            record(scope.record_type, item, scope.container_id) for item in native.items
        )
        if len({item.identity for item in observations}) != len(observations):
            raise ValueError("Calendar page repeated a native identity")
        seen, unavailable = set(state.selected_seen), False
        if root:
            observations, seen, unavailable = await self._selected(native, state, observations)
        token_history = state.recent_tokens
        if native.nextPageToken:
            digest = hashlib.sha256(native.nextPageToken.encode()).hexdigest()
            if digest in token_history:
                raise ValueError("Calendar returned a recent repeated page token")
            token_history = (*token_history, digest)[-128:]
        count = state.records + len(observations)
        size = state.bytes + sum(len(json.dumps(item.payload).encode()) for item in observations)
        pages = state.pages + 1
        if scope.record_type == "event_occurrence" and (
            count > 10000 or size > 20 * 1024 * 1024 or pages > 100
        ):
            raise ValueError("Expanded Calendar capture exceeds bounded horizon budget")
        checkpoint = None
        if scope.record_type == "event" and not native.nextPageToken:
            checkpoint = ProviderCheckpoint(value={"sync_token": native.completed_token()})
        progress = state.model_copy(
            update={
                "page_token": native.nextPageToken,
                "recent_tokens": token_history,
                "selected_seen": tuple(sorted(seen)),
                "selection_unavailable": unavailable,
                "records": count,
                "bytes": size,
                "pages": pages,
            }
        )
        return CapturePage(
            records=observations,
            continuation=progress.continuation(),
            final=not native.nextPageToken and not unavailable,
            provider_checkpoint=checkpoint,
        )

    async def _fetch(self, scope: CompletedScope, state: CalendarProgress) -> CalendarPage:
        params = dict(state.context.parameters)
        if state.page_token:
            params["pageToken"] = state.page_token
        if state.sync_token:
            params["syncToken"] = state.sync_token
        root = scope.record_type == "calendar"
        url = (
            f"{BASE}/users/me/calendarList"
            if root
            else f"{BASE}/calendars/{quote(scope.container_id or '', safe='')}/events"
        )
        try:
            native = (
                CalendarPage()
                if root and self.config.calendar_ids == ()
                else CalendarPage.model_validate(await self.get(url, params=params))
            )
        except SourceGoneError as error:
            if scope.record_type == "event" and state.mode == "changes":
                raise InvalidScopeCheckpoint("Calendar incremental token expired") from error
            raise
        except SourceEntityNotFoundError as error:
            if root:
                raise
            error_type = (
                RequiredScopeAccessLost if self.config.calendar_ids is not None else ScopeAccessLost
            )
            raise error_type(
                "Calendar is no longer accessible", removal_reason="scope_removed"
            ) from error
        return native

    async def _selected(
        self, native: CalendarPage, state: CalendarProgress, observations: tuple[CaptureRecord, ...]
    ):
        seen = set(state.selected_seen)
        if self.config.calendar_ids is not None:
            selected = set(self.config.calendar_ids)
            observations = tuple(
                item for item in observations if item.identity.native_id in selected
            )
            seen.update(item.identity.native_id for item in observations)
        unavailable = (
            any(item.kind == "delete" for item in observations)
            and self.config.calendar_ids is not None
        )
        if not native.nextPageToken and self.config.calendar_ids is not None:
            missing = set(self.config.calendar_ids) - seen
            # Commit verified withdrawals before surfacing the configured-selection failure.
            extra = []
            for native_id in sorted(missing):
                await self.confirm_member_absent(native_id)
                extra.append(
                    record("calendar", {"id": native_id, "deleted": True}).model_copy(
                        update={"removal_reason": "scope_removed"}
                    )
                )
            present = {item.identity.native_id for item in observations}
            observations += tuple(item for item in extra if item.identity.native_id not in present)
            unavailable = unavailable or bool(missing)
        return observations, seen, unavailable

    async def confirm_member_absent(self, native_id: str) -> None:
        """Only exact missing/deleted membership confirms a list omission."""
        if self.config.calendar_ids is not None and native_id not in self.config.calendar_ids:
            return
        try:
            payload = await self.get(f"{BASE}/users/me/calendarList/{quote(native_id, safe='')}")
        except SourceEntityNotFoundError:
            return
        if payload.get("id") != native_id:
            raise ValueError("Calendar returned a different membership identity")
        if payload.get("deleted") is not True and payload.get("accessRole") not in {
            "none",
            "freeBusyReader",
        }:
            raise ValueError("Calendar listing omitted accessible membership; retry capture")


class _NativeError(BaseModel):
    model_config = ConfigDict(extra="ignore")
    reason: str
    location: str | None = None
    locationType: str | None = None


class _NativeErrorBody(BaseModel):
    model_config = ConfigDict(extra="ignore")
    errors: tuple[_NativeError, ...] = ()


class _NativeErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore")
    error: _NativeErrorBody


def invalid_page_token(raw: dict) -> bool:
    """Only explicit native pageToken parameter evidence permits resetting pagination."""
    try:
        error = _NativeErrorEnvelope.model_validate(raw)
    except ValidationError:
        return False
    return any(
        item.location == "pageToken"
        and item.locationType == "parameter"
        and item.reason in {"badRequest", "invalidArgument", "invalid"}
        for item in error.error.errors
    )
