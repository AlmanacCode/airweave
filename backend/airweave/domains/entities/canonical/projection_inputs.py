"""Typed mapper outputs; source descriptors survive filtering and entity-ID stamping."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
from airweave.platform.entities._base import BaseEntity


class ProjectionInput(BaseModel):
    """An expected part, with None only for explicitly uncaptured original bytes."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    part: ExtractionPart
    entity: BaseEntity | None
    native_body: NativeTextBody | None = None
    omission: Literal["unsupported_format"] | None = None

    @model_validator(mode="after")
    def valid_omission(self) -> "ProjectionInput":
        """Explicit unsupported originals cannot also supply indexable content."""
        if self.omission is not None and (self.entity is not None or self.native_body is not None):
            raise ValueError("Unsupported projection parts cannot supply an entity or text")
        return self


class ProjectionInputs(BaseModel):
    """All expected parts, including deliberate original-capture omissions."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    parts: tuple[ProjectionInput, ...]

    @property
    def entities(self) -> tuple[BaseEntity, ...]:
        """Materialized entities for mapper callers; coverage lives in parts."""
        return tuple(p.entity for p in self.parts if p.entity is not None)
