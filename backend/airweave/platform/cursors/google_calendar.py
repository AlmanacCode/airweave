"""Independent continuation tokens for each calendar's unexpanded event stream."""

from pydantic import Field

from airweave.platform.cursors._base import BaseCursor


class GoogleCalendarCursor(BaseCursor):
    """Token parameters are fixed by the canonical generator, not user date ranges."""

    calendar_tokens: dict[str, str] = Field(default_factory=dict)
