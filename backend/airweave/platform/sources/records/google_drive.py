"""Drive metadata capture with boundary-before-scan and complete changes replay."""

from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    StartedScope,
)
from airweave.domains.entities.canonical.source import SourceObservation
from airweave.domains.sources.exceptions import SourceGoneError
from airweave.domains.syncs.cursors.cursor import SyncCursor

BASE = "https://www.googleapis.com/drive/v3"
GetJSON = Callable[..., Awaitable[dict]]


class DrivePage(BaseModel):
    """Original item dictionaries survive validation intact."""

    model_config = ConfigDict(extra="ignore")
    files: list[dict[str, JsonValue]] = Field(default_factory=list)
    changes: list[dict[str, JsonValue]] = Field(default_factory=list)
    nextPageToken: str | None = None
    newStartPageToken: str | None = None
    incompleteSearch: bool = False


class DriveScopeChanged(Exception):
    """A shared-drive membership change requires fresh corpus enumeration."""


class DriveCheckpoint(BaseModel):
    """In-memory result of draining one change stream, never persisted prematurely."""

    token: str | None = None


def file_record(payload: dict[str, JsonValue], *, removed: bool = False) -> CaptureRecord:
    """A Drive removal may mean lost access; do not invent permanent provider deletion."""
    native_id = payload.get("id")
    if not isinstance(native_id, str) or not native_id:
        raise ValueError("Drive resource has no file identity")
    folder = payload.get("mimeType") in {
        "application/vnd.google-apps.folder",
        "application/vnd.google-apps.shortcut",
    }
    return CaptureRecord(
        identity=RecordIdentity(record_type="file", native_id=native_id),
        payload=payload,
        kind="delete" if removed else "upsert",
        removal_reason="scope_removed" if removed else None,
        completeness="complete" if folder or removed else "metadata_only",
        observed_at=datetime.now(timezone.utc),
    )


async def enumerate_files(get: GetJSON) -> AsyncGenerator[CaptureRecord, None]:
    """Fail incomplete all-drive listings rather than deleting unenumerated records."""
    params: dict[str, str | int] = {
        "pageSize": 1000,
        "fields": "*",
        "spaces": "drive",
        "corpora": "allDrives",
        "includeItemsFromAllDrives": "true",
        "supportsAllDrives": "true",
    }
    seen: set[str] = set()
    while True:
        page = DrivePage.model_validate(await get(f"{BASE}/files", params=params))
        if page.incompleteSearch:
            raise ValueError("Drive reported incomplete enumeration; checkpoint was not advanced")
        for item in page.files:
            yield file_record(item)
        if not page.nextPageToken:
            return
        if page.nextPageToken in seen:
            raise ValueError("Drive returned a repeated listing page token")
        seen.add(page.nextPageToken)
        params["pageToken"] = page.nextPageToken


async def replay_changes(
    get: GetJSON, token: str, checkpoint: DriveCheckpoint
) -> AsyncGenerator[CaptureRecord, None]:
    """Read every change page; metadata edits and trash state are real updates."""
    params: dict[str, str | int] = {
        "pageToken": token,
        "pageSize": 1000,
        "fields": "*",
        "spaces": "drive",
        "includeRemoved": "true",
        "includeCorpusRemovals": "true",
        "includeItemsFromAllDrives": "true",
        "supportsAllDrives": "true",
    }
    seen: set[str] = {token}
    while True:
        page = DrivePage.model_validate(await get(f"{BASE}/changes", params=params))
        for change in page.changes:
            if change.get("changeType") == "drive":
                # Drive membership changes require a new exhaustive file scan.
                raise DriveScopeChanged("Shared-drive membership changed")
            if change.get("removed") is True:
                yield file_record({"id": change.get("fileId"), "change": change}, removed=True)
            else:
                item = change.get("file")
                if not isinstance(item, dict):
                    raise ValueError("Drive file change omitted its current file resource")
                yield file_record(item)
        if not page.nextPageToken:
            if not page.newStartPageToken:
                raise ValueError("Drive change enumeration ended without newStartPageToken")
            checkpoint.token = page.newStartPageToken
            return
        if page.nextPageToken in seen:
            raise ValueError("Drive returned a repeated changes page token")
        seen.add(page.nextPageToken)
        params["pageToken"] = page.nextPageToken


async def generate_drive_observations(
    get: GetJSON, cursor: SyncCursor | None
) -> AsyncGenerator[SourceObservation, None]:
    """Only canonical checkpoints can skip bootstrap; legacy index cursors cannot."""
    state = cursor.data if cursor else {}
    token = state.get("canonical_page_token")
    for reset_attempt in range(2):
        full = not token
        if full:
            boundary = await get(
                f"{BASE}/changes/startPageToken", params={"supportsAllDrives": "true"}
            )
            token = boundary.get("startPageToken")
            if not isinstance(token, str) or not token:
                raise ValueError("Drive omitted its starting change boundary")
            yield StartedScope(record_type="file")
            async for item in enumerate_files(get):
                yield item
        checkpoint = DriveCheckpoint()
        try:
            async for item in replay_changes(get, token, checkpoint):
                yield item
        except (DriveScopeChanged, SourceGoneError):
            if reset_attempt:
                raise
            token = None
            continue
        if full:
            yield CompletedScope(record_type="file")
        if cursor is not None:
            cursor.update(canonical_page_token=checkpoint.token)
        return
