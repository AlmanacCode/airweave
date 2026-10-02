"""One conversion result, shared by indexing and optional retained representation reads."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from airweave.platform.entities._base import BaseEntity


class NativeTextBody(BaseModel):
    """Source-selected body; an empty string is present content, not missing content."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    text: str
    metadata_fields: tuple[str, ...] = ()


class BuiltText(BaseModel):
    """Content starts at a known construction boundary, never a parsed delimiter."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    entity_id: str
    text: str
    content_start: int | None = Field(default=None, ge=0)
    kind: Literal["native_text", "extracted_text", "generated_text"] = "generated_text"


class BuiltTextBatch(BaseModel):
    """Entities retain the same strings that were passed to the chunker."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    entities: list[BaseEntity]
    representations: tuple[BuiltText, ...]
    failed_entity_ids: tuple[str, ...] = ()
    conversion_gaps: dict[str, Literal["ocr_unavailable"]] = Field(default_factory=dict)
