"""Bounded recurring reclamation; retirement fences publication before any remote delete."""

from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.mail_body import current_mail_body
from airweave.domains.entities.canonical.projection_models import (
    ProjectionCleanupPage,
    ProjectionDocument,
    ProjectionGenerationRef,
)
from airweave.domains.entities.canonical.store import content_is_available
from airweave.domains.entities.canonical.text_models import TextArtifact
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.sync import Sync


class ProjectionGCStore:
    """Retain manifests indefinitely: timed-out feed threads have no bounded lifetime."""

    async def due(
        self, db: AsyncSession, *, now: datetime, limit: int = 20
    ) -> tuple[ProjectionGenerationRef, ...]:
        """Cross-tenant discovery returns IDs through the fixed control capability."""
        rows = await db.execute(
            text("SELECT organization_id,generation_id FROM owned_due_generations(:now,:limit)"),
            {"now": now, "limit": limit},
        )
        return tuple(
            ProjectionGenerationRef(organization_id=organization, generation_id=generation)
            for organization, generation in rows
        )

    async def claim(
        self,
        db: AsyncSession,
        generation: UUID,
        *,
        now: datetime,
        limit: int = 100,
    ) -> ProjectionCleanupPage | None:
        """Irrevocably retire under the same Sync lock used by publication."""
        if not 1 <= limit <= 100:
            raise ValueError("Cleanup document budget must be between 1 and 100")
        async with UnitOfWork(db):
            initial = await db.get(ProjectionGeneration, generation)
            if initial is None:
                return None
            sync = await db.scalar(
                select(Sync)
                .where(Sync.id == initial.sync_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            row = await db.scalar(
                select(ProjectionGeneration)
                .where(
                    ProjectionGeneration.id == generation,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if row.next_gc_at > now:
                return None
            current = await db.scalar(
                select(Entity).where(
                    Entity.id == row.record_id,
                    Entity.sync_id == row.sync_id,
                    Entity.organization_id == row.organization_id,
                )
            )
            if current is not None and row.mail_body_text is not None and sync is not None:
                designated = await db.scalar(
                    select(
                        current_mail_body()
                        .with_only_columns(ProjectionGeneration.id)
                        .scalar_subquery()
                    )
                    .select_from(Entity)
                    .join(Sync, Sync.id == Entity.sync_id)
                    .where(Entity.id == current.id)
                )
                available = await db.scalar(
                    select(content_is_available()).where(Entity.id == current.id)
                )
                if designated == generation and current.deleted_at is None and available:
                    row.next_gc_at = now + timedelta(days=1)
                    return None
                if designated == generation and row.retired_at is None:
                    sync.mail_text_sequence += 1
            if current is not None and current.indexed_generation == generation:
                available = await db.scalar(
                    select(content_is_available()).where(Entity.id == current.id)
                )
                if not row.documents or (
                    current.deleted_at is None
                    and available
                    and (
                        row.mail_body_text is None
                        or (
                            row.revision == current.record_revision
                            and sync is not None
                            and row.pipeline_version == sync.index_pipeline_version
                        )
                    )
                ):
                    row.next_gc_at = now + timedelta(days=1)
                    return None
                # Capture and parent transitions share the Sync lock. Clear publication
                # before retiring: restoration then requires a fresh generation, never
                # resurrects documents that an already-issued delete may still remove.
                current.indexed_generation = None
                current.indexed_revision = None
                current.indexed_pipeline_version = None
                current.indexed_chunk_count = None
            if row.retired_at is None:
                row.retired_at = now
            row.next_gc_at = now + timedelta(minutes=5)
            row.gc_attempt = uuid4()
            await db.flush()
            artifacts = tuple(
                TextArtifact.model_validate(item) for item in (row.text_representations or [])
            )
            start, end = row.delete_cursor, row.delete_cursor + limit
            artifact_start = max(0, start - len(row.documents or []))
            artifact_end = max(0, end - len(row.documents or []))
            return ProjectionCleanupPage(
                generation=generation,
                attempt=row.gc_attempt,
                cursor=row.delete_cursor,
                artifact_keys=tuple(
                    item.storage_key(row.sync_id, row.id)
                    for item in artifacts[artifact_start:artifact_end]
                ),
                documents=tuple(
                    ProjectionDocument.model_validate(item)
                    for item in (row.documents or [])[row.delete_cursor : row.delete_cursor + limit]
                ),
            )

    async def acknowledge(
        self,
        db: AsyncSession,
        page: ProjectionCleanupPage,
        *,
        now: datetime,
        error: str | None = None,
    ) -> None:
        """Retry failed pages; successful full passes remain scheduled for late writes."""
        async with UnitOfWork(db):
            row = await db.scalar(
                select(ProjectionGeneration)
                .where(
                    ProjectionGeneration.id == page.generation,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if (
                row is None
                or row.retired_at is None
                or row.delete_cursor != page.cursor
                or row.gc_attempt != page.attempt
            ):
                return
            row.gc_attempt = None
            if error:
                row.gc_error = error[:200]
                row.next_gc_at = now + timedelta(minutes=5)
                return
            row.gc_error = None
            row.delete_cursor += len(page.documents) + len(page.artifact_keys)
            if row.delete_cursor >= len(row.documents or []) + len(row.text_representations or []):
                row.delete_cursor = 0
                row.gc_passes += 1
                row.last_gc_at = now
                row.next_gc_at = now + timedelta(
                    minutes=min(1440, 15 * 2 ** min(row.gc_passes - 1, 7))
                )
            else:
                row.next_gc_at = now
            await db.flush()
