"""Native Calendar resource validation; page progress belongs to the canonical engine."""

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from airweave.domains.entities.canonical.calendar import is_cancelled_recurring_event
from airweave.domains.entities.canonical.requests import CaptureRecord, RecordIdentity

GetJSON = Callable[..., Awaitable[dict]]
BASE = "https://www.googleapis.com/calendar/v3"


class CalendarPage(BaseModel):
    """Validate pagination shape without stripping fields from original resources."""

    model_config = ConfigDict(extra="ignore")
    items: list[dict[str, JsonValue]] = Field(default_factory=list)
    nextPageToken: str | None = None
    nextSyncToken: str | None = None

    def completed_token(self) -> str:
        """An exhausted event listing must supply its durable continuation boundary."""
        if not self.nextSyncToken:
            raise ValueError("Calendar event enumeration ended without nextSyncToken")
        return self.nextSyncToken


def record(
    kind: str, payload: dict[str, JsonValue], calendar_id: str | None = None
) -> CaptureRecord:
    """Preserve sparse cancellation identity as well as full live provider JSON."""
    native_id = payload.get("id")
    if not isinstance(native_id, str) or not native_id:
        raise ValueError("Calendar returned a resource without an ID")
    recurring_cancellation = kind == "event" and is_cancelled_recurring_event(payload)
    if (
        kind == "event"
        and payload.get("status") == "cancelled"
        and payload.get("recurringEventId")
        and not recurring_cancellation
    ):
        raise ValueError("Cancelled recurring event lacks its original occurrence identity")
    # Google requires clients to retain this exclusion for the lifetime of its series.
    # Keep its native ID/container so cancellation and reinstatement update the same row.
    unavailable_role = kind == "calendar" and payload.get("accessRole") in {
        "none",
        "freeBusyReader",
    }
    deleted = (
        unavailable_role
        or (payload.get("status") == "cancelled" and not recurring_cancellation)
        or payload.get("deleted") is True
    )
    return CaptureRecord(
        identity=RecordIdentity(record_type=kind, native_id=native_id, container_id=calendar_id),
        payload=payload,
        descendant_visibility_fields=("accessRole",) if kind == "calendar" else (),
        kind="delete" if deleted else "upsert",
        removal_reason=(
            "access_revoked"
            if unavailable_role
            else "scope_removed"
            if kind == "calendar"
            else "provider_deleted"
        )
        if deleted
        else None,
        source_created_at=TypeAdapter(AwareDatetime).validate_python(payload["created"])
        # Observed on expanded Google occurrences. Python cannot represent year zero;
        # keep the exact native value in payload without inventing a creation date.
        if payload.get("created") not in (None, "", "0000-12-31T00:00:00.000Z")
        else None,
        source_updated_at=TypeAdapter(AwareDatetime).validate_python(payload["updated"])
        if payload.get("updated")
        else None,
        observed_at=datetime.now(timezone.utc),
    )
