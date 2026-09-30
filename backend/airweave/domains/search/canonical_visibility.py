"""Reject stale index publications before any reranking or model consumption."""

from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import publication_matches
from airweave.domains.search.types.results import SearchResult
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.models.entity import Entity
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
    eligible: set[tuple[UUID, UUID, int, int, UUID]] = set()
    candidate_values = list(candidates.values())
    for start in range(0, len(candidate_values), 100):
        predicates = [
            and_(Entity.sync_id == sync_id, publication_matches(locator))
            for sync_id, locator in candidate_values[start : start + 100]
        ]
        rows = await db.execute(
            select(
                Entity.sync_id,
                Entity.id,
                Entity.record_revision,
                Entity.indexed_pipeline_version,
                Entity.indexed_generation,
            )
            .join(Sync, Sync.id == Entity.sync_id)
            .where(
                Entity.organization_id == organization_id,
                Sync.organization_id == organization_id,
                or_(*predicates),
            )
        )
        eligible.update(tuple(row) for row in rows)
    return [
        result
        for index, result in enumerate(results)
        if index in legacy
        or (
            index in candidates
            and (
                candidates[index][0],
                candidates[index][1].record_id,
                candidates[index][1].revision,
                candidates[index][1].pipeline_version,
                candidates[index][1].generation,
            )
            in eligible
        )
    ]
