"""One conversion result, shared by indexing and optional retained representation reads."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.text_models import TextPreparation
from airweave.platform.entities._base import BaseEntity


class NativeTextBody(BaseModel):
    """Source-selected body with explicit conversion provenance; empty is present content."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    text: str
    kind: Literal["native_text", "extracted_text"] = "native_text"
    preparation: TextPreparation | None = None
    metadata_fields: tuple[str, ...] = ()


class BuiltText(BaseModel):
    """Content starts at a known construction boundary, never a parsed delimiter."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    entity_id: str
    text: str
    content_start: int | None = Field(default=None, ge=0)
    kind: Literal["native_text", "extracted_text", "generated_text"] = "generated_text"
    preparation: TextPreparation | None = None


class BuiltTextBatch(BaseModel):
    """Entities retain the same strings that were passed to the chunker."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    entities: list[BaseEntity]
    representations: tuple[BuiltText, ...]
    failed_entity_ids: tuple[str, ...] = ()
    conversion_gaps: dict[str, Literal["ocr_unavailable", "embedded_content_unprocessed"]] = Field(
        default_factory=dict
    )
    conversion_failures: dict[str, Literal["preparation_limit"]] = Field(default_factory=dict)
