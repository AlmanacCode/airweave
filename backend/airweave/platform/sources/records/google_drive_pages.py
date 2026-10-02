"""Drive pages retain native identities; SQL owns acknowledgements and checkpoint promotion."""

import hashlib
from collections.abc import Awaitable, Callable
from typing import Annotated, Literal
from urllib.parse import quote

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
from airweave.domains.entities.canonical.requests import CaptureRecord
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceEntityNotFoundError
from airweave.platform.sources.records.google_drive import BASE, GetJSON, file_record

NativeID = Annotated[str, Field(min_length=1, max_length=256)]
Token = Annotated[str, Field(min_length=1, max_length=8192)]


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


def rejected_listing_token(raw: dict) -> bool:
    """Only parameter-specific native evidence permits discarding an inventory token."""
    try:
        body = _ErrorEnvelope.model_validate(raw)
    except ValidationError:
        return False
    return any(
        item.location == "pageToken"
        and item.locationType == "parameter"
        and item.reason in {"invalid", "badRequest", "invalidArgument"}
        for item in body.error.errors
    )


class _File(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    id: NativeID


class _Change(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    changeType: Literal["file", "drive"]
    fileId: NativeID | None = None

    @model_validator(mode="after")
    def identity(self):
        if self.changeType == "file" and self.fileId is None:
            raise ValueError("Drive file change omitted its identity")
        return self


class _Page(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    files: list[_File] = Field(default_factory=list, max_length=100)
    changes: list[_Change] = Field(default_factory=list, max_length=100)
    nextPageToken: Token | None = None
    newStartPageToken: Token | None = None
    incompleteSearch: bool = False


class _Boundary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    page_token: Token


class _Progress(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    phase: Literal["list", "changes", "done"]
    boundary: Token
    token: Token | None = None
    pending: tuple[NativeID, ...] = Field(default=(), max_length=100)
    loaded: bool = False
    terminal: Token | None = None
    recent: tuple[str, ...] = Field(default=(), max_length=128)

    @model_validator(mode="after")
    def valid(self):
        if len(set(self.pending)) != len(self.pending):
            raise ValueError("Drive pending identities must be unique")
        if self.terminal and (self.phase != "changes" or self.token is not None):
            raise ValueError("Drive terminal boundary conflicts with page continuation")
        return self

    def continuation(self):
        return ScanContinuation(
            value=type(self).model_validate(self.model_dump()).model_dump(mode="json")
        )


class DrivePages:
    """Acquire bounded native pages through the existing authenticated transport."""

    def __init__(self, get: GetJSON):
        """Retain only the authenticated read function, never durable state."""
        self.get = get

    async def prepare(
        self, previous: CaptureCycle | None, configuration: CycleConfiguration
    ) -> CapturePlan:
        """Choose changes only from matching completed capture evidence."""
        if (
            previous
            and previous.configuration == configuration
            and previous.last_full_capture
            and previous.promoted_checkpoint
        ):
            return CapturePlan(
                mode="changes", starting_checkpoint=previous.promoted_checkpoint.checkpoint
            )
        raw = await self.get(f"{BASE}/changes/startPageToken", params={"supportsAllDrives": "true"})
        return CapturePlan(
            starting_checkpoint=ProviderCheckpoint(
                value=_Boundary(page_token=raw["startPageToken"]).model_dump()
            )
        )

    @staticmethod
    def initial(cycle: CaptureCycle) -> ScanContinuation:
        """Resume the original persisted boundary, including after a crash."""
        if cycle.starting_checkpoint is None:
            raise ValueError("Drive requires a persisted native boundary")
        boundary = _Boundary.model_validate(cycle.starting_checkpoint.value).page_token
        return _Progress(
            phase="changes" if cycle.mode == "changes" else "list",
            boundary=boundary,
            token=boundary if cycle.mode == "changes" else None,
        ).continuation()

    async def current(self, native_id: str) -> CaptureRecord:
        """Only exact current404 proves the file unavailable to this connection."""
        try:
            raw = await self.get(
                f"{BASE}/files/{quote(native_id, safe='')}",
                params={"fields": "*", "supportsAllDrives": "true"},
            )
        except SourceEntityNotFoundError:
            return file_record({"id": native_id}, removed=True)
        if raw.get("id") != native_id:
            raise ValueError("Drive exact read returned a different file identity")
        return file_record(raw)

    async def page(
        self,
        continuation: ScanContinuation,
        hydrate: Callable[[CaptureRecord], Awaitable[CaptureRecord]],
    ) -> CapturePage:
        """Hydrate one file while retaining at most100 remaining native identities."""
        state = _Progress.model_validate(continuation.value)
        if state.phase == "done":
            raise ValueError("Completed Drive scan cannot fetch another page")
        if not state.pending and not state.loaded:
            state = await self._load(state)
        records = ()
        if state.pending:
            record = await hydrate(await self.current(state.pending[0]))
            records = (record,)
            state = state.model_copy(update={"pending": state.pending[1:]})
        final = False
        checkpoint = None
        if not state.pending:
            if state.token is not None:
                state = state.model_copy(update={"loaded": False})
            elif state.phase == "list":
                state = state.model_copy(
                    update={
                        "phase": "changes",
                        "token": state.boundary,
                        "loaded": False,
                        "recent": (),
                    }
                )
            else:
                if state.terminal is None:
                    raise ValueError("Drive final changes page omitted newStartPageToken")
                checkpoint = ProviderCheckpoint(value={"page_token": state.terminal})
                state = state.model_copy(update={"phase": "done", "terminal": None})
                final = True
        return CapturePage(
            records=records,
            continuation=state.continuation(),
            final=final,
            provider_checkpoint=checkpoint,
        )

    async def _load(self, state: _Progress):
        params = {
            "pageSize": 100,
            "fields": "files(id),nextPageToken,incompleteSearch"
            if state.phase == "list"
            else "changes(fileId,changeType),nextPageToken,newStartPageToken",
            "spaces": "drive",
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
        }
        if state.token:
            params["pageToken"] = state.token
        if state.phase == "list":
            params["corpora"] = "allDrives"
        else:
            params.update(includeRemoved="true", includeCorpusRemovals="true")
        page = _Page.model_validate(
            await self.get(
                f"{BASE}/{'files' if state.phase == 'list' else 'changes'}", params=params
            )
        )
        if page.incompleteSearch:
            raise ValueError("Drive reported incomplete enumeration; checkpoint was not advanced")
        if any(item.changeType == "drive" for item in page.changes):
            raise InvalidCaptureCheckpoint("Drive membership changed; fresh full capture required")
        if (
            state.phase == "changes"
            and page.nextPageToken is None
            and page.newStartPageToken is None
        ):
            raise ValueError("Drive changes omitted terminal boundary")
        recent = state.recent
        if page.nextPageToken:
            digest = hashlib.sha256(page.nextPageToken.encode()).hexdigest()
            if digest in recent:
                raise ValueError("Drive pagination repeated a recent cursor")
            recent = (*recent[-127:], digest)
        identities = (
            [item.id for item in page.files]
            if state.phase == "list"
            else [item.fileId for item in page.changes]
        )
        return state.model_copy(
            update={
                "pending": tuple(dict.fromkeys(identities)),
                "token": page.nextPageToken,
                "terminal": page.newStartPageToken if state.phase == "changes" else None,
                "recent": recent,
                "loaded": True,
            }
        )
