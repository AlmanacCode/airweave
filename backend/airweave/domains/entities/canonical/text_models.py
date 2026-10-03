"""Derived text descriptors; source originals remain the canonical authority."""

from typing import Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TextPreparationDependency(BaseModel):
    """An actual parser/extractor version used to produce this retained derived view."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(min_length=1, max_length=64)
    version: str = Field(min_length=1, max_length=64)


class TextPreparation(BaseModel):
    """Bounded provenance descriptor; values are populated by the producing mapper."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    processor: str = Field(min_length=1, max_length=64)
    version: str = Field(min_length=1, max_length=64)
    source_path: str = Field(min_length=1, max_length=2048)
    policy: str = Field(min_length=1, max_length=128)
    dependencies: tuple[TextPreparationDependency, ...] = Field(default=(), max_length=8)


class TextArtifact(BaseModel):
    """Body-free immutable descriptor, owned by one projection generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    part_index: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(ge=0)
    characters: int = Field(ge=0)
    content_start: int | None = Field(default=None, ge=0)
    kind: Literal["native_text", "extracted_text", "generated_text"]
    preparation: TextPreparation | None = None

    @model_validator(mode="before")
    @classmethod
    def legacy_kind(cls, value):
        """Older manifests encoded only converter-vs-generated provenance."""
        if isinstance(value, dict) and "kind" not in value:
            return {
                **value,
                "kind": (
                    "extracted_text" if value.get("content_start") is not None else "generated_text"
                ),
            }
        return value

    @model_validator(mode="after")
    def boundary(self):
        """The content boundary must lie inside the retained indexing text."""
        if (self.kind == "generated_text") != (self.content_start is None):
            raise ValueError("Text provenance disagrees with content boundary")
        if self.content_start is not None and self.content_start > self.characters:
            raise ValueError("Content boundary exceeds representation length")
        return self

    def storage_key(self, sync_id: UUID, generation: UUID) -> str:
        """No caller/provider-supplied storage path is accepted."""
        if self.id != representation_id(generation, self.part_index):
            raise ValueError("Representation identity does not belong to this generation")
        return f"canonical/{sync_id}/projections/{generation}/{self.id}/{self.sha256}"


def representation_id(generation: UUID, part_index: int) -> UUID:
    """Opaque stable reference within this immutable publication generation."""
    return uuid5(generation, f"text:{part_index}")


class TextRepresentation(BaseModel):
    """Public reference with explicit derived provenance, never a storage key."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    record_id: UUID
    revision: int
    generation: UUID
    pipeline_version: int
    part_key: str
    kind: Literal["native_text", "extracted_text", "generated_text"]
    preparation: TextPreparation | None = None
    media_type: Literal["text/markdown"] = "text/markdown"
    content_characters: int | None
    index_characters: int
    source_anchors: Literal["unavailable"] = "unavailable"


class TextRepresentationList(BaseModel):
    """Unknown covers generations built before text retention, not an empty document."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_id: UUID
    revision: int
    status: Literal["available", "unavailable"]
    representations: tuple[TextRepresentation, ...]


class TextRead(BaseModel):
    """Unicode character range in the selected derived representation view."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    representation: TextRepresentation
    view: Literal["content", "index"]
    offset: int
    total_characters: int
    text: str
    next_offset: int | None
