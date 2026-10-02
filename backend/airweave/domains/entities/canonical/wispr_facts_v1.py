"""Version-one Wispr meeting preview validation, frozen for migration0015."""

from datetime import datetime

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictStr,
    TypeAdapter,
    ValidationError,
    field_validator,
)


class _Meeting(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str | None = Field(default=None, min_length=1)
    start: AwareDatetime | None = None
    title: str = "Meeting"
    has_transcript: StrictBool | None = None
    modified_at: AwareDatetime | None = None

    @field_validator("start", "modified_at", mode="before")
    @classmethod
    def native_iso_date(cls, value):
        """Native dates are ISO strings, never inferred numeric epoch values."""
        if value is None:
            return None
        return datetime.fromisoformat(TypeAdapter(StrictStr).validate_python(value))


class _Response(BaseModel):
    model_config = ConfigDict(extra="ignore")
    response: _Meeting


class _RetainedMeeting(BaseModel):
    model_config = ConfigDict(extra="ignore")
    listing: _Meeting
    responses: tuple[_Response, ...] = Field(min_length=1)


def meeting_started_at_v1(payload: dict, native_id: str) -> AwareDatetime | None:
    """Unknown or conflicting native facts remain gaps; originals are never rewritten."""
    try:
        meeting = _RetainedMeeting.model_validate(payload)
        if meeting.listing.id != native_id:
            return None
        starts = []
        flags = []
        for item in (meeting.listing, *(part.response for part in meeting.responses)):
            if item.id is not None and item.id != native_id:
                return None
            if item.start is not None:
                starts.append(item.start)
            if item.has_transcript is not None:
                flags.append(item.has_transcript)
        if (
            not starts
            or any(value != starts[0] for value in starts)
            or any(value != flags[0] for value in flags)
        ):
            return None
        return starts[0]
    except ValidationError:
        return None
