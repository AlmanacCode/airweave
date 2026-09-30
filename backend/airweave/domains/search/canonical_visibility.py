"""Reject stale index publications before any reranking or model consumption."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import publications_match
from airweave.domains.search.types.results import SearchResult
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


async def visible_results(
    db: AsyncSession,
    organization_id: UUID,
    collection: str,
    results: list[SearchResult],
    registry: SourceRegistryProtocol,
) -> list[SearchResult]:
    """Validate request scope and exact publication; never trust index ownership fields."""
    if not results:
        return []
    connections = await db.scalars(
        select(SourceConnection).where(
            SourceConnection.organization_id == organization_id,
            SourceConnection.is_authenticated.is_(True),
            SourceConnection.readable_collection_id == collection,
        )
    )
    scopes = {
        str(connection.sync_id): connection
        for connection in connections
        if connection.sync_id is not None
    }
    legacy: set[int] = set()
    candidates: dict[int, tuple[UUID, ProjectionLocator]] = {}
    for index, result in enumerate(results):
        metadata = result.airweave_system_metadata
        scope = scopes.get(metadata.sync_id)
        if scope is None or metadata.source_name != scope.short_name:
            continue
        canonical = bool(
            getattr(registry.get(scope.short_name).source_class_ref, "canonical_record_types", ())
        )
        try:
            locator = ProjectionLocator.parse(metadata.original_entity_id)
        except ValueError:
            continue
        if locator is None:
            if not canonical:
                legacy.add(index)
        else:
            candidates[index] = (scope.sync_id, locator)
    if not candidates:
        return [result for index, result in enumerate(results) if index in legacy]
    # Bound predicate size independently from a caller's requested search page.
    eligible: dict[tuple[UUID, UUID, int, int, UUID], ExtractionCoverage | None] = {}
    candidate_values = list(set(candidates.values()))
    for start in range(0, len(candidate_values), 100):
        batch = candidate_values[start : start + 100]
        rows = await db.execute(
            select(
                Entity.sync_id,
                Entity.id,
                Entity.record_revision,
                Entity.indexed_pipeline_version,
                Entity.indexed_generation,
                ProjectionGeneration.extraction_coverage,
            )
            .join(Sync, Sync.id == Entity.sync_id)
            .join(ProjectionGeneration, ProjectionGeneration.id == Entity.indexed_generation)
            .where(
                Entity.organization_id == organization_id,
                Sync.organization_id == organization_id,
                Entity.sync_id.in_({sync_id for sync_id, _ in batch}),
                publications_match(locator for _, locator in batch),
            )
        )
        for row in rows:
            eligible[tuple(row[:-1])] = (
                ExtractionCoverage.model_validate(row[-1]) if row[-1] is not None else None
            )
    return [
        result
        for index, result in enumerate(results)
        if index in legacy or _eligible_part(candidates.get(index), eligible)
    ]


def _eligible_part(
    candidate: tuple[UUID, ProjectionLocator] | None,
    eligible: dict[tuple[UUID, UUID, int, int, UUID], ExtractionCoverage | None],
) -> bool:
    if candidate is None:
        return False
    sync_id, locator = candidate
    identity = (
        sync_id,
        locator.record_id,
        locator.revision,
        locator.pipeline_version,
        locator.generation,
    )
    if identity not in eligible:
        return False
    coverage = eligible[identity]
    return coverage is None or any(
        part.part_index == locator.part_index and part.outcome == "indexed"
        for part in coverage.parts
    )
