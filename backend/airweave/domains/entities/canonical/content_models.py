"""Immutable presentation facts from a known retained-content construction boundary."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class MatchedPart(BaseModel):
    """Source-local part identity; the containing record remains the read authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    part_index: int = Field(ge=0)
    key: str = Field(min_length=1, max_length=2048)
    kind: Literal["body", "file", "record", "metadata"]
    title: str = Field(max_length=512)


class ContentProvenance(BaseModel):
    """Unicode ranges address prepared text; subtract content_start for content-view reads.

    Original chunk ranges include surrounding source whitespace; preview ranges
    address the displayed, trimmed slice. Legacy previews have no range metadata.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    part: MatchedPart
    content_start: int | None = Field(default=None, ge=0)
    content_end: int = Field(ge=0)
    preview: str | None = Field(default=None, max_length=2400)
    original_chunk_start: int | None = Field(default=None, ge=0)
    original_chunk_end: int | None = Field(default=None, ge=0)
    preview_start: int | None = Field(default=None, ge=0)
    preview_end: int | None = Field(default=None, ge=0)
    preview_truncated: bool | None = None

    @model_validator(mode="after")
    def valid_boundary(self) -> "ContentProvenance":
        """Generated-only text cannot claim an original-content preview."""
        if self.content_start is None and self.preview is not None:
            raise ValueError("Generated text has no original-content preview")
        if self.content_start is not None and self.content_start > self.content_end:
            raise ValueError("Content boundary exceeds its prepared text")
        ranges = (
            self.original_chunk_start,
            self.original_chunk_end,
            self.preview_start,
            self.preview_end,
            self.preview_truncated,
        )
        if any(value is not None for value in ranges):
            if (
                any(value is None for value in ranges)
                or self.content_start is None
                or self.preview is None
            ):
                raise ValueError(
                    "Original chunk preview requires complete range metadata"
                )
            if not (
                self.content_start
                <= self.original_chunk_start
                <= self.preview_start
                <= self.preview_end
                <= self.original_chunk_end
                <= self.content_end
            ):
                raise ValueError("Preview range exceeds its original chunk")
            if self.preview_end - self.preview_start != len(self.preview):
                raise ValueError("Preview range must use Unicode character offsets")
        return self

    def chunk_preview(
        self, full_text: str, text: str, start: int, end: int
    ) -> "ContentProvenance":
        """Exact source-slice equality is required before excluding generated metadata."""
        if (
            self.content_end != len(full_text)
            or not 0 <= start <= end <= len(full_text)
            or full_text[start:end] != text
        ):
            raise ValueError("Chunk lost its exact prepared-text offsets")
        preview = None
        original_chunk_start = original_chunk_end = None
        preview_start = preview_end = None
        preview_truncated = None
        if self.content_start is not None:
            left = max(start, self.content_start)
            preview = ""
            if left < end:
                bounded = full_text[left : min(end, left + 2400)]
                preview = bounded.strip()
                original_chunk_start, original_chunk_end = left, end
                preview_start = left + len(bounded) - len(bounded.lstrip())
                preview_end = preview_start + len(preview)
                preview_truncated = left + 2400 < end
        return ContentProvenance(
            part=self.part,
            content_start=self.content_start,
            content_end=self.content_end,
            preview=preview,
            original_chunk_start=original_chunk_start,
            original_chunk_end=original_chunk_end,
            preview_start=preview_start,
            preview_end=preview_end,
            preview_truncated=preview_truncated,
        )
