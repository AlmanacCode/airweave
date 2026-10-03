"""Current-publication text reads, with the same visibility fence as search."""

import hashlib
from typing import Literal
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import publication_matches
from airweave.domains.entities.canonical.query import (
    BlobUnavailable,
    CanonicalQueryService,
    RecordNotFound,
    StaleRecordRevision,
)
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.entities.canonical.text_models import (
    TextArtifact,
    TextRead,
    TextRepresentation,
    TextRepresentationList,
)
from airweave.domains.storage.exceptions import StorageException
from airweave.domains.storage.limits import MAX_FILE_SIZE_BYTES
from airweave.domains.storage.protocols import StorageBackend
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.sync import Sync


class TextUnavailable(CanonicalStoreError):
    """No retained text at this exact current publication; refresh/reproject explicitly."""

    code = "text_unavailable"


class CanonicalTextReader:
    """Derived text has no independent authority or provider access."""

    def __init__(self, records: CanonicalQueryService, storage: StorageBackend):
        """Reuse record authorization and the configured blob backend."""
        self.records, self.storage = records, storage

    async def _snapshot(
        self,
        db: AsyncSession,
        organization: UUID,
        sync_id: UUID,
        record_id: UUID,
        revision: int,
    ) -> tuple[ProjectionGeneration | None, tuple[TextArtifact, ...]]:
        record = await self.records.read(db, organization, sync_id, record_id)
        if record.deleted_at is not None or record.content_access != "available":
            raise RecordNotFound("Record content is not currently available")
        if record.revision != revision:
            raise StaleRecordRevision("Record changed; reread before requesting text")
        row = await db.scalar(
            select(ProjectionGeneration)
            .join(
                Entity,
                Entity.indexed_generation == ProjectionGeneration.id,
            )
            .where(
                Entity.id == record_id,
                Entity.sync_id == sync_id,
                Entity.organization_id == organization,
                ProjectionGeneration.organization_id == organization,
                ProjectionGeneration.sync_id == sync_id,
                ProjectionGeneration.record_id == record_id,
                ProjectionGeneration.revision == revision,
            )
            .execution_options(populate_existing=True)
        )
        if row is None or row.text_representations is None:
            return None, ()
        try:
            artifacts = tuple(
                TextArtifact.model_validate(item) for item in row.text_representations
            )
            coverage = ExtractionCoverage.model_validate(row.extraction_coverage)
            if len({item.id for item in artifacts}) != len(artifacts):
                raise ValueError("Duplicate artifact")
            if {item.part_index for item in artifacts} != {
                part.part_index for part in coverage.parts if part.outcome == "indexed"
            }:
                raise ValueError("Artifact coverage mismatch")
            for artifact in artifacts:
                artifact.storage_key(sync_id, row.id)
        except (ValidationError, ValueError):
            raise TextUnavailable("Retained text metadata is unavailable") from None
        if artifacts:
            # Exact coverage membership above covers every descriptor. The remaining
            # publication/ancestor predicate is common to the whole generation.
            artifact = artifacts[0]
            locator = ProjectionLocator(
                record_id=record_id,
                revision=revision,
                pipeline_version=row.pipeline_version,
                generation=row.id,
                part_index=artifact.part_index,
            )
            visible = await db.scalar(
                select(Entity.id)
                .join(Sync)
                .where(
                    Entity.organization_id == organization,
                    Entity.sync_id == sync_id,
                    Sync.organization_id == organization,
                    publication_matches(locator),
                )
            )
            if visible is None:
                return None, ()
        return row, artifacts

    @staticmethod
    def _describe(row: ProjectionGeneration, artifact: TextArtifact) -> TextRepresentation:
        coverage = ExtractionCoverage.model_validate(row.extraction_coverage)
        part = next(part for part in coverage.parts if part.part_index == artifact.part_index)
        return TextRepresentation(
            id=artifact.id,
            record_id=row.record_id,
            revision=row.revision,
            generation=row.id,
            pipeline_version=row.pipeline_version,
            part_key=part.key,
            kind=artifact.kind,
            preparation=artifact.preparation,
            content_characters=(
                artifact.characters - artifact.content_start
                if artifact.content_start is not None
                else None
            ),
            index_characters=artifact.characters,
        )

    async def list(
        self,
        db: AsyncSession,
        organization: UUID,
        sync_id: UUID,
        record_id: UUID,
        revision: int,
    ) -> TextRepresentationList:
        """Describe current retained text without reading blobs or claiming legacy coverage."""
        row, artifacts = await self._snapshot(db, organization, sync_id, record_id, revision)
        return TextRepresentationList(
            record_id=record_id,
            revision=revision,
            status="available" if row is not None and artifacts else "unavailable",
            representations=tuple(self._describe(row, item) for item in artifacts),
        )

    async def read(
        self,
        db: AsyncSession,
        organization: UUID,
        sync_id: UUID,
        record_id: UUID,
        revision: int,
        generation: UUID,
        representation_id: UUID,
        *,
        offset: int = 0,
        limit: int = 12000,
        view: Literal["content", "index"] = "content",
    ) -> TextRead:
        """Default content view excludes metadata; generated-only text requires index view."""
        if offset < 0 or not 1 <= limit <= 100000:
            raise TextUnavailable("Text range must have nonnegative offset and limit 1–100000")
        row, artifacts = await self._snapshot(db, organization, sync_id, record_id, revision)
        artifact = next((item for item in artifacts if item.id == representation_id), None)
        if row is None or row.id != generation or artifact is None:
            raise TextUnavailable("Text publication changed or is not available; reread the record")
        if view == "content" and artifact.content_start is None:
            raise TextUnavailable("This is generated search text; select its index view explicitly")
        description = self._describe(row, artifact)
        text = await self._read_verified(artifact, sync_id, generation)
        db.expire_all()
        current, current_artifacts = await self._snapshot(
            db,
            organization,
            sync_id,
            record_id,
            revision,
        )
        if current is None or current.id != generation or artifact not in current_artifacts:
            raise TextUnavailable("Text publication changed during read; reread the record")
        if view == "content":
            text = text[artifact.content_start :]
        if offset > len(text):
            raise TextUnavailable("Text offset exceeds this representation")
        end = min(len(text), offset + limit)
        return TextRead(
            representation=description,
            view=view,
            offset=offset,
            total_characters=len(text),
            text=text[offset:end],
            next_offset=end if end < len(text) else None,
        )

    async def _read_verified(self, artifact: TextArtifact, sync_id: UUID, generation: UUID) -> str:
        """Bound storage reads and verify UTF-8, digest and both recorded lengths."""
        if artifact.size_bytes > MAX_FILE_SIZE_BYTES:
            raise BlobUnavailable("Retained text exceeds its storage bound")
        try:
            raw = await self.storage.read_file(
                artifact.storage_key(sync_id, generation),
                max_bytes=artifact.size_bytes,
            )
            if (
                len(raw) != artifact.size_bytes
                or hashlib.sha256(raw).hexdigest() != artifact.sha256
            ):
                raise ValueError("Text digest mismatch")
            text = raw.decode("utf-8")
            if len(text) != artifact.characters:
                raise ValueError("Text character count mismatch")
        except (StorageException, ValueError):
            raise BlobUnavailable("Retained text bytes are unavailable; retry later") from None
        return text
