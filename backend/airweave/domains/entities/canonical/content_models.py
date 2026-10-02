"""Immutable presentation facts from a known retained-content construction boundary."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class MatchedPart(BaseModel):
    """Source-local part identity; the containing record remains the read authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    part_index: int = Field(ge=0)
    key: str = Field(min_length=1, max_length=2048)
    kind: Literal["body", "file", "record"]
    title: str = Field(max_length=512)


class ContentProvenance(BaseModel):
    """Offsets address the full prepared string; preview uses Unicode characters."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    part: MatchedPart
    content_start: int | None = Field(default=None, ge=0)
    content_end: int = Field(ge=0)
    preview: str | None = Field(default=None, max_length=600)

    @model_validator(mode="after")
    def valid_boundary(self) -> "ContentProvenance":
        """Generated-only text cannot claim an original-content preview."""
        if self.content_start is None and self.preview is not None:
            raise ValueError("Generated text has no original-content preview")
        if self.content_start is not None and self.content_start > self.content_end:
            raise ValueError("Content boundary exceeds its prepared text")
        return self

    def chunk_preview(self, full_text: str, text: str, start: int, end: int) -> "ContentProvenance":
        """Exact source-slice equality is required before excluding generated metadata."""
        if (
            self.content_end != len(full_text)
            or not 0 <= start <= end <= len(full_text)
            or full_text[start:end] != text
        ):
            raise ValueError("Chunk lost its exact prepared-text offsets")
        preview = None
        if self.content_start is not None:
            left = max(start, self.content_start)
            preview = full_text[left:end][:600].strip() if left < end else ""
        return self.model_copy(update={"preview": preview})
