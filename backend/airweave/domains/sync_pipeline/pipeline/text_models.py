"""One conversion result, shared by indexing and optional retained representation reads."""

from pydantic import BaseModel, ConfigDict, Field

from airweave.platform.entities._base import BaseEntity


class BuiltText(BaseModel):
    """Content starts at a known construction boundary, never a parsed delimiter."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    entity_id: str
    text: str
    content_start: int | None = Field(default=None, ge=0)


class BuiltTextBatch(BaseModel):
    """Entities retain the same strings that were passed to the chunker."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    entities: list[BaseEntity]
    representations: tuple[BuiltText, ...]
