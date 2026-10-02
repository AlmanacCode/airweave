"""Native Drive identity and original record mapping shared with body capture."""

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from pydantic import JsonValue

from airweave.domains.entities.canonical.requests import CaptureRecord, RecordIdentity
from airweave.platform.entities.google_drive import _parse_drive_dt

BASE = "https://www.googleapis.com/drive/v3"
GetJSON = Callable[..., Awaitable[dict]]


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
        source_created_at=_parse_drive_dt(payload.get("createdTime")),
        source_updated_at=_parse_drive_dt(payload.get("modifiedTime")),
    )
