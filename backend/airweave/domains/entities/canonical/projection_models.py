"""Typed immutable search publication identities, independent of provider identity."""

import re
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.models import SourceRecord


def canonical_projection_workflow_id(organization_id: UUID | str, sync_id: UUID | str) -> str:
    """One execution identity shared by scheduled, capture and operator projection."""
    return f"canonical-projection:{organization_id}:{sync_id}"


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


class ProjectionBinding(BaseModel):
    """Authenticated tenant source and exact destination captured with pending work."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_connection_id: UUID
    source_name: str
    collection_id: UUID


class ProjectionWork(BaseModel):
    """Snapshot plus publication pointer used for compare-and-swap."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    organization_id: UUID
    binding: ProjectionBinding
    record: SourceRecord
    pipeline_version: int
    previous_generation: UUID | None


class ProjectionSourceRef(BaseModel):
    """Stored tenant/source identity eligible for recovery, without original payloads."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    organization_id: UUID
    sync_id: UUID


class ProjectionSourcePage(BaseModel):
    """Bounded source keyset page; a cursor exists only when another row was observed."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sources: tuple[ProjectionSourceRef, ...]
    next_cursor: UUID | None = None


class ProjectionBatchResult(BaseModel):
    """One bounded scan page; failures remain pending for a later retry."""

    after_id: UUID | None = None
    has_more: bool = False
    published: int = 0
    superseded: int = 0
    failed: int = 0


class ProjectionDocument(BaseModel):
    """Exact remote identifier, without searchable text or embeddings."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_name: str = Field(min_length=1)
    document_id: str = Field(min_length=1)


class ProjectionCleanupPage(BaseModel):
    """One idempotent deletion page of an irrevocably retired generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    generation: UUID
    attempt: UUID
    cursor: int
    documents: tuple[ProjectionDocument, ...]
    artifact_keys: tuple[str, ...] = ()


def scope_projection_document_id(sync_id: UUID, collection_id: UUID, document_id: str) -> str:
    """Bind canonical remote IDs to the destination without changing search locators."""
    scoped = f"{sync_id}_{collection_id}_{document_id}"
    projection_document_locator(scoped, sync_id, collection_id)
    return scoped


def projection_document_locator(
    document_id: str, sync_id: UUID, collection_id: UUID
) -> ProjectionLocator:
    """Reject foreign prefixes and ambiguous substring matches before manifest commit."""
    prefix = f"{sync_id}_{collection_id}_"
    if not document_id.startswith(prefix):
        raise ValueError("Projection document scope does not match destination")
    match = re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_]*_(canonical:v1:[^_]+)__chunk_(0|[1-9][0-9]*)",
        document_id[len(prefix) :],
    )
    if match is None:
        raise ValueError("Invalid canonical remote document ID")
    locator = ProjectionLocator.parse(match[1])
    if locator is None or locator.encode() != match[1]:
        raise ValueError("Noncanonical projection locator encoding")
    return locator
