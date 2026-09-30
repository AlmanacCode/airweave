"""Gmail native page acquisition; the canonical engine owns every durable acknowledgement."""

import hashlib
from typing import Annotated, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from airweave.domains.entities.canonical.cycle_models import (
    CaptureCycle,
    CycleConfiguration,
    ProviderCheckpoint,
)
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    CapturePlan,
    InvalidCaptureCheckpoint,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceEntityNotFoundError
from airweave.platform.sources.gmail_capture import BASE, GmailCapture

PAGE_SIZE = 50
MessageID = Annotated[str, Field(min_length=1, max_length=128)]


class _MessageID(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    id: MessageID


class _ListPage(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    messages: list[_MessageID] = Field(default_factory=list, max_length=PAGE_SIZE)
    nextPageToken: str | None = Field(default=None, min_length=1, max_length=8192)


class _ParameterError(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    location: str | None = None
    locationType: str | None = None
    reason: str | None = None


class _ErrorBody(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    errors: list[_ParameterError] = Field(default_factory=list)


class _ErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    error: _ErrorBody


def invalid_page_token(raw: dict) -> bool:
    """Only a native parameter-specific error authorizes page-token recovery."""
    try:
        body = _ErrorEnvelope.model_validate(raw)
    except ValidationError:
        return False
    return any(
        item.location == "pageToken"
        and item.locationType == "parameter"
        and item.reason in {"badRequest", "invalidArgument"}
        for item in body.error.errors
    )


class _Boundary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    history_id: str = Field(min_length=1, max_length=1024)


class _Progress(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    mode: Literal["full", "changes"]
    phase: Literal["list", "history", "done"]
    history_boundary: str | None = Field(default=None, max_length=1024)
    list_token: str | None = Field(default=None, max_length=8192)
    listing_complete: bool = False
    pending_ids: tuple[MessageID, ...] = Field(default=(), max_length=PAGE_SIZE)
    history_token: str | None = Field(default=None, max_length=8192)
    history_fingerprint: str | None = None
    history_offset: int = Field(default=0, ge=0)
    list_tokens: tuple[str, ...] = Field(default=(), max_length=128)
    history_tokens: tuple[str, ...] = Field(default=(), max_length=128)

    @model_validator(mode="after")
    def valid_phase(self):
        if self.phase == "history" and not self.history_boundary:
            raise ValueError("History traversal requires its immutable start boundary")
        if self.history_offset and not self.history_fingerprint:
            raise ValueError("History offset requires an exact replay page fingerprint")
        if self.pending_ids and self.phase != "list":
            raise ValueError("Only a baseline list page may retain pending native IDs")
        if len(set(self.pending_ids)) != len(self.pending_ids):
            raise ValueError("Pending Gmail identities must be unique")
        if self.listing_complete and self.list_token is not None:
            raise ValueError("Completed listing cannot retain a next-page token")
        if self.mode == "changes" and (not self.listing_complete or self.phase == "list"):
            raise ValueError("Changes cannot enumerate a mailbox inventory")
        return self

    def continuation(self) -> ScanContinuation:
        validated = type(self).model_validate(self.model_dump())
        return ScanContinuation(value=validated.model_dump(mode="json"))


def _next_token(token: str | None, recent: tuple[str, ...]) -> tuple[str, ...]:
    """Detect recent cursor loops without imposing a total page ceiling."""
    if token is None:
        return recent
    digest = hashlib.sha256(token.encode()).hexdigest()
    if digest in recent:
        raise ValueError("Gmail pagination repeated a recent cursor")
    return (*recent[-127:], digest)


class GmailPages:
    """Small source-local acquisition functions; no cursor writes, SQL, or background workers."""

    def __init__(self, capture: GmailCapture):
        """Reuse native message/MIME acquisition and the source transport."""
        self.capture = capture

    async def prepare(
        self, previous: CaptureCycle | None, configuration: CycleConfiguration
    ) -> CapturePlan:
        """A legacy cursor never substitutes for compatible canonical full-capture evidence."""
        if self.capture.query:
            return CapturePlan()
        if (
            previous is not None
            and previous.configuration == configuration
            and (
                previous.last_full_capture is not None and previous.promoted_checkpoint is not None
            )
        ):
            return CapturePlan(
                mode="changes", starting_checkpoint=previous.promoted_checkpoint.checkpoint
            )
        raw = await self.capture.get(f"{BASE}/profile")
        boundary = _Boundary(history_id=raw["historyId"])
        return CapturePlan(starting_checkpoint=ProviderCheckpoint(value=boundary.model_dump()))

    @staticmethod
    def initial(cycle: CaptureCycle) -> ScanContinuation:
        """Only the persisted cycle decides the first page and history boundary."""
        boundary = (
            _Boundary.model_validate(cycle.starting_checkpoint.value).history_id
            if cycle.starting_checkpoint
            else None
        )
        return _Progress(
            mode=cycle.mode,
            phase="history" if cycle.mode == "changes" else "list",
            history_boundary=boundary,
            listing_complete=cycle.mode == "changes",
        ).continuation()

    async def page(self, continuation: ScanContinuation) -> CapturePage:
        """Return one bounded fully hydrated batch, without acknowledging it."""
        state = _Progress.model_validate(continuation.value)
        if state.phase == "done":
            raise ValueError("A completed Gmail page must not be fetched again")
        if bool(self.capture.query) != (state.history_boundary is None):
            raise ValueError("Gmail continuation belongs to a different query mode")
        return await self._list(state) if state.phase == "list" else await self._history(state)

    async def _list(self, state: _Progress) -> CapturePage:
        if state.pending_ids:
            return await self._hydrate_list(state)
        params = {"maxResults": PAGE_SIZE, "includeSpamTrash": "true"}
        if self.capture.query:
            params["q"] = self.capture.query
        if state.list_token is not None:
            params["pageToken"] = state.list_token
        raw = await self.capture.get(f"{BASE}/messages", params=params)
        listing = _ListPage.model_validate(raw)
        tokens = _next_token(listing.nextPageToken, state.list_tokens)
        identities = tuple(item.id for item in listing.messages)
        if len(set(identities)) != len(identities):
            raise ValueError("Gmail list response duplicated a native message identity")
        state = state.model_copy(
            update={
                "list_token": listing.nextPageToken,
                "listing_complete": listing.nextPageToken is None,
                "list_tokens": tokens,
                "pending_ids": identities,
            }
        )
        return await self._hydrate_list(state)

    async def _hydrate_list(self, state: _Progress) -> CapturePage:
        batch = await self.capture.hydrate_messages(state.pending_ids)
        pending = state.pending_ids[batch.next_offset :]
        final = bool(self.capture.query) and state.listing_complete and not pending
        following = state.model_copy(
            update={
                "pending_ids": pending,
                "phase": "list"
                if pending
                else "done"
                if final
                else "list"
                if self.capture.query
                else "history",
            }
        )
        return CapturePage(
            records=batch.records, continuation=following.continuation(), final=final
        )

    async def _history(self, state: _Progress) -> CapturePage:
        try:
            page = await self.capture.history_page(
                state.history_boundary, state.history_token, max_results=100
            )
        except (SourceEntityNotFoundError, httpx.HTTPStatusError) as error:
            if isinstance(error, httpx.HTTPStatusError) and error.response.status_code != 404:
                raise
            raise InvalidCaptureCheckpoint(
                "Gmail history boundary expired; full capture required"
            ) from error
        batch = await self.capture.hydrate_history_page(
            page,
            offset=state.history_offset,
            limit=PAGE_SIZE,
            expected_fingerprint=state.history_fingerprint,
        )
        if not batch.complete:
            following = state.model_copy(
                update={
                    "history_offset": batch.next_offset,
                    "history_fingerprint": page.fingerprint,
                }
            )
            return CapturePage(records=batch.records, continuation=following.continuation())
        if page.next_page_token is not None:
            following = state.model_copy(
                update={
                    "history_offset": 0,
                    "history_fingerprint": None,
                    "history_token": page.next_page_token,
                    "history_tokens": _next_token(page.next_page_token, state.history_tokens),
                }
            )
            return CapturePage(records=batch.records, continuation=following.continuation())
        final = state.listing_complete
        following = state.model_copy(
            update={
                "phase": "done" if final else "list",
                "history_boundary": page.history_id,
                "history_token": None,
                "history_fingerprint": None,
                "history_offset": 0,
                "history_tokens": (),
            }
        )
        return CapturePage(
            records=batch.records,
            continuation=following.continuation(),
            final=final,
            provider_checkpoint=ProviderCheckpoint(value={"history_id": page.history_id})
            if final
            else None,
        )
