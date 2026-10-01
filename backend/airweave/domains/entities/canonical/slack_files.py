"""Retained acquisition evidence for files owned by a Slack message."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

SlackFileReason = Literal[
    "access_denied", "unsupported_external", "oversized", "missing_metadata", "not_found"
]


class SlackFileOutcome(BaseModel):
    """One native file position, without modifying the message's native JSON."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    index: int = Field(ge=0, lt=100)
    native_id: str = Field(pattern=r"^F[A-Z0-9]+$", max_length=128)
    outcome: Literal["captured", "unavailable"]
    reason: SlackFileReason | None = None
    file: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def consistent(self):
        """Retained enrichment must attest the same native file."""
        if (self.outcome == "captured") != (self.reason is None):
            raise ValueError("Slack file outcome and reason disagree")
        if self.file is not None and self.file.get("id") != self.native_id:
            raise ValueError("Slack file enrichment identity disagrees")
        return self


class SlackFileManifest(BaseModel):
    """Bounded, ordered per-message acquisition results."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    files: tuple[SlackFileOutcome, ...] = Field(max_length=100)

    @model_validator(mode="after")
    def positions(self):
        """No duplicate identities or omitted native positions."""
        if [entry.index for entry in self.files] != list(range(len(self.files))):
            raise ValueError("Slack file positions are not contiguous")
        if len({entry.native_id for entry in self.files}) != len(self.files):
            raise ValueError("Slack file identities are duplicated")
        return self
