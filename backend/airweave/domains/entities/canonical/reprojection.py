"""Scoped operator version change; existing projection rows remain the durable queue."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.projection_models import canonical_projection_workflow_id
from airweave.domains.entities.canonical.search_metadata import SEARCH_METADATA_PIPELINE_VERSION
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class ReprojectionPlan(BaseModel):
    """Inspectable target and counts, with no provider credentials."""

    model_config = ConfigDict(frozen=True)
    organization_id: UUID
    sync_id: UUID
    source_connection_id: UUID
    provider: str
    current_version: int
    target_version: int
    captured_records: int
    target_pending_records: int
    workflow_id: str
    applied: bool


async def plan_reprojection(
    db: AsyncSession,
    organization_id: UUID,
    sync_id: UUID,
    *,
    expected_version: int,
    target_version: int,
    apply: bool = False,
) -> ReprojectionPlan:
    """Caller commits before starting workflow; identical retries never bump twice."""
    if expected_version < 1 or target_version != expected_version + 1:
        raise ValueError("Target version must be exactly expected version plus one")
    if target_version < SEARCH_METADATA_PIPELINE_VERSION:
        raise ValueError("Target version lacks current canonical search metadata")
    sync = await db.scalar(
        select(Sync)
        .where(
            Sync.id == sync_id,
            Sync.organization_id == organization_id,
        )
        .with_for_update()
    )
    if sync is None:
        raise ValueError("Selected organization/sync is unavailable")
    connection = (
        await db.scalars(
            select(SourceConnection)
            .join(
                Collection,
                (
                    (Collection.readable_id == SourceConnection.readable_collection_id)
                    & (Collection.organization_id == organization_id)
                ),
            )
            .where(
                SourceConnection.organization_id == organization_id,
                SourceConnection.sync_id == sync_id,
                SourceConnection.is_authenticated.is_(True),
            )
        )
    ).one_or_none()
    if connection is None:
        raise ValueError("Selected authenticated source binding is unavailable")
    if sync.index_pipeline_version not in (expected_version, target_version):
        raise ValueError("Projection version changed; inspect before choosing a new operation")
    count, pending = (
        await db.execute(
            select(
                func.count(),
                func.count().filter(
                    Entity.indexed_pipeline_version.is_distinct_from(target_version)
                    | Entity.indexed_revision.is_distinct_from(Entity.record_revision)
                    | Entity.indexed_generation.is_(None)
                ),
            ).where(
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Entity.record_revision > 0,
            )
        )
    ).one()
    if not count:
        raise ValueError("Selected source has no captured canonical records")
    plan = ReprojectionPlan(
        organization_id=organization_id,
        sync_id=sync_id,
        source_connection_id=connection.id,
        provider=connection.short_name,
        current_version=sync.index_pipeline_version,
        target_version=target_version,
        captured_records=count,
        target_pending_records=pending,
        workflow_id=canonical_projection_workflow_id(organization_id, sync_id),
        applied=apply,
    )
    if apply:
        sync.index_pipeline_version = target_version
        await db.flush()
    return plan
