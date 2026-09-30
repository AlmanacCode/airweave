"""Calendar delta tokens and successfully observed expanded-window coverage."""

from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from airweave.platform.cursors._base import BaseCursor


class CalendarWindowCoverage(BaseModel):
    """A completed observed scan, never a frozen provider snapshot."""

    model_config = ConfigDict(extra="forbid")
    start: AwareDatetime
    end: AwareDatetime
    timezone: str
    completed_at: AwareDatetime
    scan_id: UUID


class GoogleCalendarCursor(BaseCursor):
    """Master deltas and expanded rows have independent source semantics."""

    calendar_tokens: dict[str, str] = Field(default_factory=dict)
    occurrence_coverage: dict[str, CalendarWindowCoverage] = Field(default_factory=dict)
