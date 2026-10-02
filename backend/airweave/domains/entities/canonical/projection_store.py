"""Canonical pending work and atomic publication. No network I/O under row locks."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import and_, exists, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import (
    ProjectionBinding,
    ProjectionDocument,
    ProjectionLocator,
    ProjectionSourcePage,
    ProjectionSourceRef,
    ProjectionWork,
    projection_document_locator,
)
from airweave.domains.entities.canonical.projection_policy import excluded_from_search
from airweave.domains.entities.canonical.store import content_is_available, source_record
from airweave.domains.entities.canonical.text_artifacts import text_manifest
from airweave.domains.entities.canonical.text_models import TextArtifact
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


def publication_matches(locator: ProjectionLocator):
    """Add request organization/source scope separately; gate before text leaves retrieval."""
    return publications_match((locator,))


def publications_match(locators: Iterable[ProjectionLocator]):
    """Batch publication checks; callers must still validate each returned chunk's part.

    One record can match one requested part while another part is unavailable.
    Organization/source scope is supplied by the caller.
    """
    identities = set()
    parts: dict[int, set[UUID]] = defaultdict(set)
    for locator in locators:
        identities.add(
            (locator.record_id, locator.revision, locator.pipeline_version, locator.generation)
        )
        parts[locator.part_index].add(locator.generation)
    return and_(
        tuple_(
            Entity.id,
            Entity.record_revision,
            Entity.indexed_pipeline_version,
            Entity.indexed_generation,
        ).in_(list(identities)),
        Entity.indexed_revision == Entity.record_revision,
        Entity.indexed_pipeline_version == Sync.index_pipeline_version,
        Entity.deleted_at.is_(None),
        content_is_available(),
        exists(
            select(ProjectionGeneration.id)
            .correlate(Entity)
            .where(
                ProjectionGeneration.id == Entity.indexed_generation,
                ProjectionGeneration.retired_at.is_(None),
                or_(
                    ProjectionGeneration.extraction_coverage.is_(None),
                    *(
                        and_(
                            ProjectionGeneration.id.in_(list(generations)),
                            ProjectionGeneration.extraction_coverage.contains(
                                {"parts": [{"part_index": part, "outcome": "indexed"}]}
                            ),
                        )
                        for part, generations in parts.items()
                    ),
                ),
            )
        ),
    )


def _pending_record():
    """Shared exact publication/visibility eligibility for discovery and execution."""
    return and_(
        Entity.record_revision > 0,
        or_(
            Entity.indexed_revision.is_distinct_from(Entity.record_revision),
            Entity.indexed_pipeline_version.is_distinct_from(Sync.index_pipeline_version),
            Entity.indexed_generation.is_(None),
        ),
        or_(Entity.deleted_at.is_not(None), content_is_available()),
    )


class CanonicalProjectionStore:
    """Optimistic projection computation, serialized CAS against source capture."""

    async def binding(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID
    ) -> ProjectionBinding | None:
        """Resolve one authenticated source and collection; ambiguity fails closed."""
        rows = (
            await db.execute(
                select(SourceConnection.id, SourceConnection.short_name, Collection.id)
                .join(
                    Collection,
                    and_(
                        Collection.readable_id == SourceConnection.readable_collection_id,
                        Collection.organization_id == SourceConnection.organization_id,
                    ),
                )
                .join(
                    Sync,
                    and_(
                        Sync.id == SourceConnection.sync_id,
                        Sync.organization_id == SourceConnection.organization_id,
                    ),
                )
                .where(
                    SourceConnection.organization_id == organization_id,
                    SourceConnection.sync_id == sync_id,
                    SourceConnection.is_authenticated.is_(True),
                )
                .limit(2)
            )
        ).all()
        if len(rows) != 1:
            return None
        source, name, collection = rows[0]
        return ProjectionBinding(
            source_connection_id=source, source_name=name, collection_id=collection
        )

    async def admit(self, db: AsyncSession, work: ProjectionWork) -> bool:
        """Fence current authorization before any converter, storage or embedding call."""
        async with UnitOfWork(db):
            return await self._current(db, work) is not None

    async def pending(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        *,
        after_id: UUID | None = None,
        limit: int = 25,
        skip_failed: bool = False,
    ) -> tuple[ProjectionWork, ...]:
        """One bounded UUID page; deleted records publish an empty generation."""
        if not 1 <= limit <= 100:
            raise ValueError("Projection batch size must be between 1 and 100")
        binding = await self.binding(db, organization_id, sync_id)
        if binding is None:
            return ()
        statement = (
            select(Entity, Sync.index_pipeline_version)
            .join(Sync, Sync.id == Entity.sync_id)
            .where(
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Sync.organization_id == organization_id,
                _pending_record(),
            )
        )
        if skip_failed:
            statement = statement.where(Entity.projection_error.is_(None))
        if after_id is not None:
            statement = statement.where(Entity.id > after_id)
        rows = await db.execute(statement.order_by(Entity.id).limit(limit + 1))
        return tuple(
            ProjectionWork(
                organization_id=organization_id,
                binding=binding,
                record=source_record(row),
                pipeline_version=version,
                previous_generation=row.indexed_generation,
            )
            for row, version in rows
        )

    async def pending_sources(
        self,
        db: AsyncSession,
        source_names: tuple[str, ...],
        *,
        after_id: UUID | None = None,
        limit: int = 20,
    ) -> ProjectionSourcePage:
        """Discover fresh pending work fairly; failures require an explicit retry.

        This system-worker query crosses tenants but returns their exact stored
        scope. Execution must revalidate that scope before reading any originals.
        """
        if not 1 <= limit <= 100:
            raise ValueError("Projection source page size must be between 1 and 100")
        owned_source = exists(
            select(SourceConnection.id)
            .join(
                Collection,
                and_(
                    Collection.readable_id == SourceConnection.readable_collection_id,
                    Collection.organization_id == SourceConnection.organization_id,
                ),
            )
            .where(
                SourceConnection.is_authenticated.is_(True),
                SourceConnection.sync_id == Sync.id,
                SourceConnection.organization_id == Sync.organization_id,
                SourceConnection.short_name.in_(source_names),
            )
        )
        fresh_work = exists(
            select(Entity.id).where(
                Entity.sync_id == Sync.id,
                Entity.organization_id == Sync.organization_id,
                _pending_record(),
                Entity.projection_error.is_(None),
            )
        )
        statement = select(Sync.organization_id, Sync.id).where(owned_source, fresh_work)
        if after_id is not None:
            statement = statement.where(Sync.id > after_id)
        rows = (await db.execute(statement.order_by(Sync.id).limit(limit + 1))).all()
        sources = tuple(
            ProjectionSourceRef(organization_id=organization, sync_id=sync)
            for organization, sync in rows[:limit]
        )
        return ProjectionSourcePage(
            sources=sources,
            next_cursor=sources[-1].sync_id if len(rows) > limit else None,
        )

    async def _current(self, db: AsyncSession, work: ProjectionWork) -> Entity | None:
        # Capture locks Sync first too: parent visibility and revision cannot change
        # between validation and publication. This transaction contains no provider I/O.
        sync = await db.scalar(
            select(Sync)
            .where(
                Sync.id == work.record.sync_id,
                Sync.organization_id == work.organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if sync is None or sync.index_pipeline_version != work.pipeline_version:
            return None
        if await self.binding(db, work.organization_id, work.record.sync_id) != work.binding:
            return None
        row = (
            await db.execute(
                select(Entity, content_is_available())
                .where(
                    Entity.id == work.record.id,
                    Entity.sync_id == work.record.sync_id,
                    Entity.organization_id == work.organization_id,
                )
                .with_for_update(of=Entity)
                .execution_options(populate_existing=True)
            )
        ).one_or_none()
        if row is None:
            return None
        entity, visible = row
        if (
            entity.record_revision != work.record.revision
            or entity.indexed_generation != work.previous_generation
        ):
            return None
        if entity.deleted_at is None and not visible:
            return None
        return entity

    async def prepare(
        self,
        db: AsyncSession,
        work: ProjectionWork,
        generation: UUID,
        collection_id: UUID,
        documents: tuple[ProjectionDocument, ...],
        *,
        coverage: ExtractionCoverage | None = None,
        text_representations: tuple[TextArtifact, ...] | None = None,
    ) -> bool:
        """Commit exact immutable deletion identities before the first remote feed."""
        if collection_id != work.binding.collection_id:
            raise ValueError("Projection destination does not match authenticated binding")
        identities = set()
        indexed_parts = set()
        for document in documents:
            try:
                locator = projection_document_locator(
                    document.document_id, work.record.sync_id, collection_id
                )
                valid = (
                    locator.record_id == work.record.id
                    and locator.revision == work.record.revision
                    and locator.pipeline_version == work.pipeline_version
                    and locator.generation == generation
                )
            except (ValueError, IndexError, AttributeError):
                valid = False
            if not valid or (document.schema_name, document.document_id) in identities:
                raise ValueError("Projection manifest must contain unique IDs of this generation")
            identities.add((document.schema_name, document.document_id))
            indexed_parts.add(locator.part_index)
        if coverage is not None and indexed_parts != {
            p.part_index for p in coverage.parts if p.outcome == "indexed"
        }:
            raise ValueError("Projection documents must cover exactly the indexed parts")
        text_descriptors = text_manifest(
            text_representations, indexed_parts, work.record.sync_id, generation
        )
        extraction = coverage.persisted() if coverage is not None else None
        manifest = [document.model_dump() for document in documents]
        async with UnitOfWork(db):
            if await self._current(db, work) is None:
                return False
            prior = await db.get(ProjectionGeneration, generation)
            if prior is not None:
                if (
                    prior.documents != manifest
                    or prior.extraction_coverage != extraction
                    or prior.text_representations != text_descriptors
                    or prior.record_id != work.record.id
                    or prior.revision != work.record.revision
                    or prior.pipeline_version != work.pipeline_version
                    or prior.organization_id != work.organization_id
                    or prior.collection_id != collection_id
                ):
                    raise ValueError("Projection manifest is immutable")
                return prior.retired_at is None
            db.add(
                ProjectionGeneration(
                    id=generation,
                    organization_id=work.organization_id,
                    sync_id=work.record.sync_id,
                    collection_id=collection_id,
                    record_id=work.record.id,
                    revision=work.record.revision,
                    pipeline_version=work.pipeline_version,
                    documents=manifest,
                    extraction_coverage=extraction,
                    text_representations=text_descriptors,
                    next_gc_at=datetime.now(timezone.utc) + timedelta(hours=1),
                )
            )
            await db.flush()
            return True

    async def publish(
        self,
        db: AsyncSession,
        work: ProjectionWork,
        generation: UUID,
        chunk_count: int,
    ) -> bool:
        """Publish only a complete current generation; late writers cannot replace it."""
        no_content = work.record.deleted_at is not None or excluded_from_search(
            work.record, work.binding.source_name
        )
        if chunk_count < 0:
            raise ValueError("Active records require a nonempty complete projection")
        if no_content and chunk_count != 0:
            raise ValueError("Deleted records and search exclusions must publish no content")
        async with UnitOfWork(db):
            entity = await self._current(db, work)
            if entity is None:
                return False
            attempt = await db.scalar(
                select(ProjectionGeneration)
                .where(
                    ProjectionGeneration.id == generation,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if (
                attempt is None
                or attempt.retired_at is not None
                or attempt.collection_id != work.binding.collection_id
                or attempt.sync_id != work.record.sync_id
                or attempt.organization_id != work.organization_id
                or attempt.record_id != work.record.id
                or attempt.revision != work.record.revision
                or attempt.pipeline_version != work.pipeline_version
                or len(attempt.documents) != chunk_count
            ):
                return False
            coverage = (
                ExtractionCoverage.model_validate(attempt.extraction_coverage)
                if attempt.extraction_coverage is not None
                else None
            )
            if (
                not no_content
                and chunk_count == 0
                and (coverage is None or not coverage.parts or coverage.status != "unavailable")
            ):
                raise ValueError(
                    "Active records require indexed content or explicit unavailable extraction"
                )
            if no_content and coverage is not None and coverage.parts:
                raise ValueError("Excluded records must have empty extraction coverage")
            if work.previous_generation is not None:
                previous = await db.get(ProjectionGeneration, work.previous_generation)
                if previous is not None:
                    previous.retired_at = datetime.now(timezone.utc)
                    previous.next_gc_at = datetime.now(timezone.utc)
            entity.indexed_revision = work.record.revision
            entity.indexed_pipeline_version = work.pipeline_version
            entity.indexed_generation = generation
            entity.indexed_chunk_count = chunk_count
            entity.projection_error = None
            await db.flush()
            return True

    async def fail(self, db: AsyncSession, work: ProjectionWork, message: str) -> None:
        """Record bounded diagnostics only while the same input remains current."""
        async with UnitOfWork(db):
            entity = await self._current(db, work)
            if entity is not None:
                entity.projection_error = message[:1000]
                await db.flush()


async def current_extraction(
    db: AsyncSession, organization_id: UUID, sync_id: UUID, record_id: UUID, revision: int
) -> ExtractionCoverage | None:
    """Only the visible current revision/pipeline/publication exposes extraction evidence."""
    raw = await db.scalar(
        select(ProjectionGeneration.extraction_coverage)
        .join(Entity, Entity.indexed_generation == ProjectionGeneration.id)
        .join(Sync, Sync.id == Entity.sync_id)
        .where(
            Entity.id == record_id,
            Entity.record_revision == revision,
            Entity.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Sync.organization_id == organization_id,
            ProjectionGeneration.organization_id == organization_id,
            ProjectionGeneration.record_id == Entity.id,
            ProjectionGeneration.revision == Entity.record_revision,
            ProjectionGeneration.pipeline_version == Sync.index_pipeline_version,
            ProjectionGeneration.retired_at.is_(None),
            Entity.indexed_revision == Entity.record_revision,
            Entity.indexed_pipeline_version == Sync.index_pipeline_version,
            Entity.deleted_at.is_(None),
            content_is_available(),
        )
    )
    return ExtractionCoverage.model_validate(raw) if raw is not None else None
