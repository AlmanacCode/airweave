"""Bounded original-record projection using existing conversion/embed/feed primitives."""

from collections.abc import Callable, Iterable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.logging import ContextualLogger
from airweave.domains.entities.canonical.extraction_models import (
    ExtractionCoverage,
    ExtractionOutcome,
)
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInputs
from airweave.domains.entities.canonical.projection_models import (
    ProjectionBatchResult,
    ProjectionDocument,
    ProjectionLocator,
    ProjectionWork,
    scope_projection_document_id,
)
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.search_metadata import stamp_search_metadata
from airweave.domains.storage.protocols import StorageBackend
from airweave.domains.sync_pipeline.file_types import SUPPORTED_FILE_EXTENSIONS
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.destinations.vespa.destination import VespaDestination
from airweave.platform.entities._base import AirweaveSystemMetadata, BaseEntity


@dataclass
class ProjectionContext:
    """Projection processing has no source credentials or artificial capture job."""

    logger: ContextualLogger
    source_short_name: str


class StrictProjectionTracker:
    """A replay publication cannot silently omit failed conversion inputs."""

    async def record_skipped(self, count: int) -> None:
        """Fail publication when an existing converter tries to skip required input."""
        if count:
            raise ValueError("Required projection input could not be converted")


@dataclass
class ProjectionRuntime:
    """Only processing diagnostics are required; source runtime is not recreated."""

    entity_tracker: StrictProjectionTracker


def _stamp_chunks(chunks: Iterable[BaseEntity], record: SourceRecord) -> None:
    """Conversion cannot replace the source-owned searchable metadata."""
    for chunk in chunks:
        if chunk.airweave_system_metadata is None:
            raise ValueError("Projection chunk lost canonical metadata")
        stamp_search_metadata(chunk.airweave_system_metadata, record)


class CanonicalProjector:
    """Read snapshot, compute outside transaction, then atomically publish generation."""

    def __init__(
        self,
        store: CanonicalProjectionStore,
        sessions: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        processor: ChunkEmbedProcessor,
        storage: StorageBackend,
    ):
        """Inject existing processor, storage, and transaction session ownership."""
        self._store = store
        self._sessions = sessions
        self._processor = processor
        self._storage = storage

    async def project_one(
        self,
        work: ProjectionWork,
        source_name: str,
        destination: VespaDestination,
        logger: ContextualLogger,
    ) -> bool:
        """Never feed native capture JSON; mapper emits explicit safe projection entities."""
        from airweave.domains.entities.canonical.projection_mappers import (
            excluded_from_search,
            map_record,
        )

        generation = uuid4()
        chunks = []
        coverage = ExtractionCoverage(parts=())
        no_documents = work.record.deleted_at is not None or excluded_from_search(
            work.record, source_name
        )
        if not no_documents:
            async with map_record(work.record, source_name, self._storage) as mapped:
                if not mapped.parts:
                    raise ValueError("Projection mapper returned no required content")
                selected, coverage = _select_inputs(mapped, work, source_name, generation)
                chunks = await self._processor.process(
                    selected,
                    ProjectionContext(logger, source_name),
                    ProjectionRuntime(StrictProjectionTracker()),
                    strict=True,
                )
                _stamp_chunks(chunks, work.record)
                prepared = destination.prepare_documents(chunks)
                for group in prepared.values():
                    for document in group:
                        document.id = scope_projection_document_id(
                            work.record.sync_id, destination.collection_id, document.id
                        )
                manifest = tuple(
                    ProjectionDocument(schema_name=doc.schema_name, document_id=doc.id)
                    for group in prepared.values()
                    for doc in group
                )
                async with self._sessions() as db:
                    if not await self._store.prepare(
                        db, work, generation, destination.collection_id, manifest, coverage=coverage
                    ):
                        return False
                await destination.feed_prepared(prepared)
        if no_documents:
            async with self._sessions() as db:
                if not await self._store.prepare(
                    db, work, generation, destination.collection_id, (), coverage=coverage
                ):
                    return False
        async with self._sessions() as db:
            return await self._store.publish(db, work, generation, len(chunks))

    async def batch(
        self,
        organization_id: UUID,
        sync_id: UUID,
        source_name: str,
        destination: VespaDestination,
        logger: ContextualLogger,
        *,
        after_id: UUID | None = None,
        limit: int = 25,
    ) -> ProjectionBatchResult:
        """Failed rows stay pending; other rows in the page continue to make progress."""
        async with self._sessions() as db:
            pending = await self._store.pending(
                db,
                organization_id,
                sync_id,
                after_id=after_id,
                limit=limit,
            )
        published = superseded = failed = 0
        for work in pending[:limit]:
            try:
                if await self.project_one(work, source_name, destination, logger):
                    published += 1
                else:
                    superseded += 1
            except Exception as error:
                failed += 1
                async with self._sessions() as db:
                    await self._store.fail(db, work, type(error).__name__)
                logger.warning(
                    "Canonical projection failed for record %s (%s)",
                    work.record.id,
                    type(error).__name__,
                )
        return ProjectionBatchResult(
            after_id=pending[min(limit, len(pending)) - 1].record.id if pending else after_id,
            has_more=len(pending) > limit,
            published=published,
            superseded=superseded,
            failed=failed,
        )


def _select_inputs(
    mapped: ProjectionInputs, work: ProjectionWork, source_name: str, generation: UUID
) -> tuple[list[BaseEntity], ExtractionCoverage]:
    """Classify only deterministic omissions, preserving stable pre-filter part ordinals."""
    selected = []
    outcomes = []
    for item in mapped.parts:
        part, entity = item.part, item.entity
        if entity is None:
            outcome, reason = "unavailable_original", "original_not_captured"
        elif (
            part.kind == "file"
            and part.extension is not None
            and part.extension not in SUPPORTED_FILE_EXTENSIONS
        ):
            outcome, reason = "unsupported", "unsupported_format"
        else:
            outcome, reason = "indexed", None
            selected.append(entity)
        outcomes.append(ExtractionOutcome(**part.model_dump(), outcome=outcome, reason=reason))
        if outcome != "indexed":
            continue
        part_index = part.part_index
        populate_base_fields(entity)
        if entity.airweave_system_metadata is None:
            entity.airweave_system_metadata = AirweaveSystemMetadata()
        locator = ProjectionLocator(
            record_id=work.record.id,
            revision=work.record.revision,
            pipeline_version=work.pipeline_version,
            generation=generation,
            part_index=part_index,
        )
        entity.entity_id = locator.encode()
        meta = entity.airweave_system_metadata
        meta.source_name = source_name
        meta.entity_type = type(entity).__name__
        meta.sync_id = work.record.sync_id
        meta.sync_job_id = None
        meta.db_entity_id = work.record.id
        stamp_search_metadata(meta, work.record)
    return selected, ExtractionCoverage(parts=tuple(outcomes))
