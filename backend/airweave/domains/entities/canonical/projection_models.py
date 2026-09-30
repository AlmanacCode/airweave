"""Typed immutable search publication identities, independent of provider identity."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.models import SourceRecord


class ProjectionLocator(BaseModel):
    """Stored in the existing original_entity_id field, never a native provider ID."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_id: UUID
    revision: int = Field(ge=1)
    pipeline_version: int = Field(ge=1)
    generation: UUID
    part_index: int = Field(ge=0)

    def encode(self) -> str:
        """Encode a versioned, unambiguous publication locator."""
        return (
            f"canonical:v1:{self.record_id}:{self.revision}:{self.pipeline_version}:"
            f"{self.generation}:{self.part_index}"
        )

    @classmethod
    def parse(cls, value: str) -> "ProjectionLocator | None":
        """None identifies legacy values; malformed canonical values fail closed."""
        if not value.startswith("canonical:"):
            return None
        parts = value.split(":")
        if len(parts) != 7 or parts[:2] != ["canonical", "v1"]:
            raise ValueError("Invalid canonical projection locator")
        return cls(
            record_id=parts[2],
            revision=parts[3],
            pipeline_version=parts[4],
            generation=parts[5],
            part_index=parts[6],
        )


class ProjectionWork(BaseModel):
    """Snapshot plus publication pointer used for compare-and-swap."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    organization_id: UUID
    record: SourceRecord
    pipeline_version: int
    previous_generation: UUID | None


class ProjectionBatchResult(BaseModel):
    """One bounded scan page; failures remain pending for a later retry."""

    after_id: UUID | None = None
    has_more: bool = False
    published: int = 0
    superseded: int = 0
    failed: int = 0
