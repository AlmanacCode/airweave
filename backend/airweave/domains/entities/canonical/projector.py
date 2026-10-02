"""Bounded original-record projection using existing conversion/embed/feed primitives."""

from collections.abc import Callable, Iterable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.logging import ContextualLogger
from airweave.domains.entities.canonical.content_models import ContentProvenance, MatchedPart
from airweave.domains.entities.canonical.extraction_models import (
    ExtractionCoverage,
    ExtractionOutcome,
)
from airweave.domains.entities.canonical.mail_body import prepared_mail_body
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInputs
from airweave.domains.entities.canonical.projection_models import (
    ProjectionBatchResult,
    ProjectionDocument,
    ProjectionLocator,
    ProjectionResult,
    ProjectionWork,
    scope_projection_document_id,
)
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.search_metadata import stamp_search_metadata
from airweave.domains.entities.canonical.text_artifacts import prepare_text
from airweave.domains.storage.protocols import StorageBackend
from airweave.domains.sync_pipeline.pipeline.text_models import BuiltText, BuiltTextBatch
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.destinations.vespa.destination import VespaDestination
from airweave.platform.destinations.vespa.types import VespaDocument
from airweave.platform.entities._base import AirweaveSystemMetadata, BaseEntity


@dataclass
class ProjectionContext:
    """Projection processing has no source credentials or artificial capture job."""

    logger: ContextualLogger
    source_short_name: str


@dataclass
class ProjectionConversionTracker:
    """Conversion failures must be accounted for, never silently dropped."""

    allow_failures: bool = False
    failed_count: int = 0

    async def record_skipped(self, count: int) -> None:
        """Fail publication when an existing converter tries to skip required input."""
        self.failed_count += count
        if count and not self.allow_failures:
            raise ValueError("Required projection input could not be converted")


@dataclass
class ProjectionRuntime:
    """Only processing diagnostics are required; source runtime is not recreated."""

    entity_tracker: ProjectionConversionTracker


def _stamp_chunks(chunks: Iterable[BaseEntity], record: SourceRecord) -> None:
    """Conversion cannot replace the source-owned searchable metadata."""
    for chunk in chunks:
        if chunk.airweave_system_metadata is None:
            raise ValueError("Projection chunk lost canonical metadata")
        stamp_search_metadata(chunk.airweave_system_metadata, record)


def _scope_manifest(
    prepared: dict[str, list[VespaDocument]], sync_id: UUID, collection_id: UUID
) -> tuple[ProjectionDocument, ...]:
    """Make the durable manifest match exact scoped remote document identities."""
    for group in prepared.values():
        for document in group:
            document.id = scope_projection_document_id(sync_id, collection_id, document.id)
    return tuple(
        ProjectionDocument(schema_name=document.schema_name, document_id=document.id)
        for group in prepared.values()
        for document in group
    )


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

    async def _admit(
        self, work: ProjectionWork, source_name: str, destination: VespaDestination
    ) -> bool:
        """Close authorization transaction before any external processing begins."""
        if (
            source_name != work.binding.source_name
            or destination.collection_id != work.binding.collection_id
        ):
            return False
        async with self._sessions() as db:
            return await self._store.admit(db, work)

    async def _prepare_mail_text(
        self, work: ProjectionWork, source_name: str, generation: UUID, built: tuple[BuiltText, ...]
    ) -> bool:
        """Prepare body facts without making embedding or remote feed a read prerequisite."""
        if source_name != "gmail" or work.record.identity.record_type != "message":
            return True
        body = prepared_mail_body(built, generation, work.record.completeness)
        if body is None:
            raise ValueError("Gmail projection did not convert its required body")
        async with self._sessions() as db:
            return await self._store.prepare_mail_body(db, work, generation, body)

    async def project_one(
        self,
        work: ProjectionWork,
        source_name: str,
        destination: VespaDestination,
        logger: ContextualLogger,
    ) -> ProjectionResult:
        """Never feed native capture JSON; mapper emits explicit safe projection entities."""
        from airweave.domains.entities.canonical.projection_mappers import map_record
        from airweave.domains.entities.canonical.projection_policy import excluded_from_search

        if not await self._admit(work, source_name, destination):
            return ProjectionResult()

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
                selected, coverage = _select_inputs(
                    mapped, work, source_name, generation, self._processor.supports_file_extension
                )
                context = ProjectionContext(logger, source_name)
                runtime = ProjectionRuntime(ProjectionConversionTracker())
                selected_ids = {entity.entity_id for entity in selected}
                native_bodies = {
                    item.entity.entity_id: item.native_body
                    for item in mapped.parts
                    if item.native_body is not None
                    and item.entity is not None
                    and item.entity.entity_id in selected_ids
                }
                if source_name == "gmail" and work.record.identity.record_type == "message":
                    outcome = await self._build_gmail_text(
                        selected, coverage, context, work, generation
                    )
                    if outcome is None:
                        return ProjectionResult()
                    built, coverage = outcome
                else:
                    built = await self._processor.build_text(
                        selected, context, runtime, native_bodies=native_bodies
                    )
                _stamp_content(built, coverage)
                artifacts = prepare_text(built.representations, generation)
                chunks = await self._processor.process_built_text(
                    built.entities,
                    context,
                    runtime,
                    strict=True,
                    expected_ids=(
                        {entity.entity_id for entity in built.entities}
                        if source_name == "gmail" and work.record.identity.record_type == "message"
                        else selected_ids
                    ),
                )
                _stamp_chunks(chunks, work.record)
                prepared = destination.prepare_documents(chunks)
                manifest = _scope_manifest(prepared, work.record.sync_id, destination.collection_id)
                async with self._sessions() as db:
                    if not await self._store.prepare(
                        db,
                        work,
                        generation,
                        destination.collection_id,
                        manifest,
                        coverage=coverage,
                        text_representations=tuple(item for item, _ in artifacts),
                    ):
                        return ProjectionResult()
                for artifact, content in artifacts:
                    await self._storage.write_file(
                        artifact.storage_key(work.record.sync_id, generation), content
                    )
                await destination.feed_prepared(prepared)
        if no_documents:
            async with self._sessions() as db:
                if not await self._store.prepare(
                    db,
                    work,
                    generation,
                    destination.collection_id,
                    (),
                    coverage=coverage,
                    text_representations=(),
                ):
                    return ProjectionResult()
        async with self._sessions() as db:
            published = await self._store.publish(db, work, generation, len(chunks))
        return ProjectionResult(
            published=published,
            conversion_failed=published and any(p.outcome == "failed" for p in coverage.parts),
        )

    async def _build_gmail_text(
        self,
        selected: list[BaseEntity],
        coverage: ExtractionCoverage,
        context: ProjectionContext,
        work: ProjectionWork,
        generation: UUID,
    ) -> tuple[BuiltTextBatch, ExtractionCoverage] | None:
        """Prepare a complete body before bounded, independently failing attachments."""
        body = [e for e in selected if _part_index(e.entity_id) == 0]
        files = [e for e in selected if _part_index(e.entity_id) != 0]
        if len(body) != 1 or coverage.parts[0].kind != "body":
            raise ValueError("Gmail projection requires exactly one body at part zero")
        body_ids = {e.entity_id for e in body}
        body_tracker = ProjectionConversionTracker()
        body_text = await self._processor.build_text(
            body, context, ProjectionRuntime(body_tracker), strict_conversion=True
        )
        _check_converted_parts(body_ids, body_text, body_tracker)
        if not await self._prepare_mail_text(work, "gmail", generation, body_text.representations):
            return None
        file_ids = {e.entity_id for e in files}
        file_tracker = ProjectionConversionTracker(allow_failures=True)
        file_text = await self._processor.build_text(
            files, context, ProjectionRuntime(file_tracker), strict_conversion=True
        )
        _check_converted_parts(file_ids, file_text, file_tracker)
        failed_parts = {_part_index(identity) for identity in file_text.failed_entity_ids}
        parts = []
        for part in coverage.parts:
            if part.part_index in failed_parts:
                if part.kind != "file" or part.outcome != "indexed":
                    raise ValueError("Only captured selected attachments may fail conversion")
                part = ExtractionOutcome(
                    **part.model_dump(exclude={"outcome", "reason"}),
                    outcome="failed",
                    reason="conversion_failed",
                )
            parts.append(part)
        return (
            BuiltTextBatch(
                entities=body_text.entities + file_text.entities,
                representations=body_text.representations + file_text.representations,
                failed_entity_ids=file_text.failed_entity_ids,
            ),
            ExtractionCoverage(parts=tuple(parts)),
        )

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
        skip_failed: bool = False,
    ) -> ProjectionBatchResult:
        """Failed rows stay pending; other rows in the page continue to make progress."""
        async with self._sessions() as db:
            pending = await self._store.pending(
                db,
                organization_id,
                sync_id,
                after_id=after_id,
                limit=limit,
                **({"skip_failed": True} if skip_failed else {}),
            )
        published = superseded = failed = 0
        for work in pending[:limit]:
            try:
                result = await self.project_one(work, source_name, destination, logger)
                if result.published:
                    published += 1
                else:
                    superseded += 1
                failed += int(result.conversion_failed)
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


def _check_converted_parts(
    expected: set[str], built: BuiltTextBatch, tracker: ProjectionConversionTracker
) -> None:
    """A converter's explicit failures explain every omission; unknown losses fail closed."""
    entities = [e.entity_id for e in built.entities]
    representations = [item.entity_id for item in built.representations]
    failed = built.failed_entity_ids
    if (
        len(set(entities)) != len(entities)
        or len(set(representations)) != len(representations)
        or len(set(failed)) != len(failed)
        or set(entities) != set(representations)
        or set(entities) & set(failed)
        or set(entities) | set(failed) != expected
        or len(failed) != tracker.failed_count
    ):
        raise ValueError("Projection conversion did not account for its exact selected parts")


def _part_index(identity: str) -> int:
    """Only generated canonical identities may account for conversion outcomes."""
    locator = ProjectionLocator.parse(identity)
    if locator is None:
        raise ValueError("Projection conversion lost its canonical part identity")
    return locator.part_index


def _select_inputs(
    mapped: ProjectionInputs,
    work: ProjectionWork,
    source_name: str,
    generation: UUID,
    supports_file_extension: Callable[[str], bool],
) -> tuple[list[BaseEntity], ExtractionCoverage]:
    """Classify only deterministic omissions, preserving stable pre-filter part ordinals."""
    selected = []
    outcomes = []
    for item in mapped.parts:
        part, entity = item.part, item.entity
        if item.omission == "unsupported_format":
            outcome, reason = "unsupported", "unsupported_format"
        elif entity is None:
            outcome, reason = "unavailable_original", "original_not_captured"
        elif (
            part.kind == "file"
            and part.extension is not None
            and not supports_file_extension(part.extension)
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


def _stamp_content(built: BuiltTextBatch, coverage: ExtractionCoverage) -> None:
    """Capture exact construction boundaries before the existing chunker runs."""
    texts = {item.entity_id: item for item in built.representations}
    parts = {part.part_index: part for part in coverage.parts if part.outcome == "indexed"}
    for entity in built.entities:
        text = texts[entity.entity_id]
        part = parts[_part_index(entity.entity_id)]
        if entity.textual_representation != text.text or entity.airweave_system_metadata is None:
            raise ValueError("Prepared text lost its canonical content identity")
        entity.airweave_system_metadata.content_provenance = ContentProvenance(
            part=MatchedPart(
                part_index=part.part_index,
                key=part.key,
                kind=part.kind,
                title=(entity.name or "")[:512],
            ),
            content_start=text.content_start,
            content_end=len(text.text),
        )
