"""Aggregate current metadata without loading originals or calling the index."""

from uuid import UUID

from sqlalchemy import and_, cast, func, select
from sqlalchemy.dialects.postgresql import JSONPATH
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.status_models import (
    ExtractionStatus,
    PreparationStatus,
    SourceStatus,
)
from airweave.domains.entities.canonical.store import SourceNotFound, content_is_available
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


async def source_status(db: AsyncSession, organization_id: UUID, sync_id: UUID) -> SourceStatus:
    """Scope every count to visible originals and exact current generation metadata."""
    if not await db.scalar(select(source_is_readable(organization_id, sync_id))):
        raise SourceNotFound("Source is not available")
    generation = ProjectionGeneration
    current = func.coalesce(
        and_(
            Entity.indexed_revision == Entity.record_revision,
            Entity.indexed_pipeline_version == Sync.index_pipeline_version,
            generation.record_id == Entity.id,
            generation.organization_id == organization_id,
            generation.sync_id == sync_id,
            generation.revision == Entity.record_revision,
            generation.pipeline_version == Sync.index_pipeline_version,
            generation.retired_at.is_(None),
            generation.documents.is_not(None),
            select(SourceConnection.id)
            .join(
                Collection,
                (Collection.readable_id == SourceConnection.readable_collection_id)
                & (Collection.organization_id == SourceConnection.organization_id),
            )
            .where(
                SourceConnection.organization_id == organization_id,
                SourceConnection.sync_id == sync_id,
                SourceConnection.is_authenticated.is_(True),
                Collection.id == generation.collection_id,
            )
            .exists(),
        ),
        False,
    )
    coverage = generation.extraction_coverage
    # Exclusion is attested by a current zero-part publication, never guessed
    # from provider payloads or stale policy decisions awaiting preparation.
    excluded = func.coalesce(
        coverage.contains({"parts": []})
        & (func.jsonb_array_length(coverage["parts"]) == 0)
        & (func.jsonb_array_length(generation.documents) == 0),
        False,
    )
    indexed = func.coalesce(
        func.jsonb_path_exists(
            coverage,
            cast('$.parts[*] ? (@.kind != "metadata" && @.outcome == "indexed")', JSONPATH),
        ),
        False,
    )
    omitted = func.coalesce(
        func.jsonb_path_exists(
            coverage,
            cast('$.parts[*] ? (@.kind != "metadata" && @.outcome != "indexed")', JSONPATH),
        ),
        False,
    )
    gaps = func.coalesce(
        func.jsonb_path_exists(
            coverage, cast('$.parts[*] ? (@.kind != "metadata" && exists(@.gaps[0]))', JSONPATH)
        ),
        False,
    )
    # PostgreSQL otherwise repeats the correlated current-publication check for
    # each aggregate. Materialize only narrow facts, never original JSON/text.
    facts = (
        select(
            Entity.observed_at.label("observed"),
            Entity.projection_error.is_not(None).label("failed"),
            current.label("current"),
            excluded.label("excluded"),
            indexed.label("indexed"),
            omitted.label("omitted"),
            gaps.label("gaps"),
        )
        .select_from(Entity)
        .join(Sync, Sync.id == Entity.sync_id)
        .outerjoin(generation, generation.id == Entity.indexed_generation)
        .where(
            Entity.organization_id == organization_id,
            Sync.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
            source_is_readable(organization_id, sync_id),
        )
        .cte("status_facts")
        .prefix_with("MATERIALIZED")
    )
    current = facts.c.current
    excluded = current & facts.c.excluded
    indexed, omitted, gaps = facts.c.indexed, facts.c.omitted, facts.c.gaps
    counts = (
        await db.execute(
            select(
                func.count().label("retained"),
                func.max(facts.c.observed).label("observed"),
                func.count().filter(excluded).label("excluded"),
                func.count().filter(current & ~excluded).label("current"),
                func.count().filter(~current & facts.c.failed).label("failed"),
                func.count()
                .filter(current & ~excluded & indexed & (omitted | gaps))
                .label("partial"),
                func.count().filter(current & ~excluded & ~indexed & omitted).label("unavailable"),
                func.count().filter(current & ~excluded & ~indexed & ~omitted).label("unknown"),
            ).select_from(facts)
        )
    ).one()
    captures = await capture_coverage(db, organization_id, (sync_id,))
    # Repeat after all reads: a withdrawal during the request cannot authorize
    # publishing previously computed counts.
    if not await db.scalar(select(source_is_readable(organization_id, sync_id))):
        raise SourceNotFound("Source is not available")
    candidates = counts.retained - counts.excluded
    return SourceStatus(
        sync_id=sync_id,
        retained_records=counts.retained,
        last_observed_at=counts.observed,
        capture=captures.get(sync_id),
        preparation=PreparationStatus(
            candidate_records=candidates,
            current_records=counts.current,
            pending_records=candidates - counts.current,
            failed_records=counts.failed,
            excluded_records=counts.excluded,
        ),
        extraction=ExtractionStatus(
            partial_records=counts.partial,
            unavailable_records=counts.unavailable,
            unknown_records=counts.unknown,
        ),
    )
