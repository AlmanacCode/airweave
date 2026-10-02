"""Google Calendar recurrence exclusions are scheduling state, not live meetings."""

from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict


def is_cancelled_recurring_event(payload: Mapping[str, object]) -> bool:
    """Recognize the provider-guaranteed identity of a cancelled series occurrence."""
    if payload.get("status") != "cancelled":
        return False
    recurring_id = payload.get("recurringEventId")
    original = payload.get("originalStartTime")
    return (
        isinstance(recurring_id, str)
        and bool(recurring_id)
        and isinstance(original, dict)
        and any(
            isinstance(original.get(key), str) and original[key] for key in ("date", "dateTime")
        )
    )


class CalendarScopeContext(BaseModel):
    """Effective native parameters and timezone used by the same range publication."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    parameters: dict[str, str | int]
    timezone: str | None = None
