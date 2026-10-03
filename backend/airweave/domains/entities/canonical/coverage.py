"""Describe persisted capture promises independently of indexing counts or job success."""

from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.coverage_models import (
    CaptureCoverage,
    CaptureScopeSummary,
    FullCaptureCoverage,
)
from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    CaptureCycle,
)
from airweave.domains.entities.canonical.requests import WriterFence
from airweave.domains.entities.canonical.store import content_is_available
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor


async def mixed_scope_summary(
    db: AsyncSession, organization_id: UUID, sync_id: UUID, cycle: CaptureCycle
) -> CaptureScopeSummary | None:
    """Aggregate the current visible forest, withholding counts until roots are refreshed."""
    sync = await db.scalar(
        select(Sync).where(Sync.id == sync_id, Sync.organization_id == organization_id)
    )
    if sync is None or sync.writer_job_id is None or sync.writer_attempt_id is None:
        return None
    fence = WriterFence(
        organization_id=organization_id,
        sync_id=sync_id,
        job_id=sync.writer_job_id,
        epoch=sync.writer_epoch,
        attempt_id=sync.writer_attempt_id,
        attempt_number=sync.writer_attempt_number,
    )
    common = (
        CaptureScan.organization_id == organization_id,
        CaptureScan.sync_id == sync_id,
        CaptureScan.cycle_id == cycle.version.cycle_id,
        CaptureScan.phase == "complete",
    )
    roots = cycle.configuration.root_record_types
    inventory_roots = tuple(kind for kind in roots if cycle.configuration.children_of(kind))
    modes: dict[str, str | None] = {}
    if roots:
        rows = await db.execute(
            select(
                CaptureScan.record_type,
                CaptureScan.execution_state["plan"]["mode"].astext,
            ).where(
                *common,
                CaptureScan.record_type.in_(roots),
                CaptureScan.parent_record_id.is_(None),
                CaptureScan.container_id.is_(None),
                or_(
                    CaptureScan.record_type.not_in(inventory_roots),
                    CaptureScan.membership_attempt_id == fence.attempt_id,
                ),
            )
        )
        modes = dict(rows.tuples().all())
    if not set(roots).issubset(modes):
        return None
    eligible = len(roots)
    full = sum(mode == "full" for mode in modes.values())
    changes = sum(mode == "changes" for mode in modes.values())
    for kind, parents in cycle.configuration.parents.items():
        parent_types = tuple(parent for parent in parents if parent is not None)
        if not parent_types:
            continue
        completed = and_(
            *common,
            CaptureScan.record_type == kind,
            CaptureScan.parent_record_id == Entity.id,
            CaptureScan.parent_visibility_epoch == Entity.visibility_epoch,
        )
        if cycle.configuration.children_of(kind):
            completed = and_(completed, CaptureScan.membership_attempt_id == fence.attempt_id)
        counts = (
            await db.execute(
                select(
                    func.count(Entity.id),
                    func.count(
                        case((CaptureScan.execution_state["plan"]["mode"].astext == "full", 1))
                    ),
                    func.count(
                        case((CaptureScan.execution_state["plan"]["mode"].astext == "changes", 1))
                    ),
                )
                .select_from(Entity)
                .outerjoin(CaptureScan, completed)
                .where(
                    Entity.organization_id == organization_id,
                    Entity.sync_id == sync_id,
                    Entity.entity_definition_short_name.in_(parent_types),
                    content_is_available(),
                )
            )
        ).one()
        eligible += counts[0]
        full += counts[1]
        changes += counts[2]
    return CaptureScopeSummary(
        eligible=eligible,
        completed_full=full,
        completed_changes=changes,
        unfinished=eligible - full - changes,
    )


async def capture_coverage(
    db: AsyncSession,
    organization_id: UUID,
    sync_ids: tuple[UUID, ...],
    *,
    include_scope_summary: bool = True,
) -> dict[UUID, CaptureCoverage]:
    """Read only existing tenant-scoped cycle metadata, never create an authority."""
    rows = await db.scalars(
        select(SyncCursor).where(
            SyncCursor.organization_id == organization_id, SyncCursor.sync_id.in_(sync_ids)
        )
    )
    result = {}
    for row in rows:
        raw = row.cursor_data.get(CYCLE_KEY)
        if raw is None:
            continue
        try:
            cycle = CaptureCycle.model_validate(raw)
        except ValidationError:
            continue  # Unknown/corrupt capture state cannot certify coverage.
        policies = {kind: cycle.configuration.policy(kind) for kind in cycle.configuration.parents}
        discovery = (
            "incomplete"
            if any(p != "exhaustive" for p in policies.values())
            else "pending"
            if cycle.phase == "active"
            else "scope_enumeration_complete"
        )
        evidence = cycle.last_full_capture
        if evidence is not None and evidence.configuration_digest != cycle.configuration.digest():
            continue
        promoted = cycle.promoted_checkpoint
        if cycle.mode == "changes":
            if (
                evidence is None
                or promoted is None
                or (promoted.configuration_digest != cycle.configuration.digest())
            ):
                continue
            discovery = evidence.discovery
        summary = (
            await mixed_scope_summary(db, organization_id, row.sync_id, cycle)
            if cycle.mode == "mixed" and include_scope_summary
            else None
        )
        if cycle.mode == "mixed" and (
            evidence is not None
            or promoted is not None
            or (
                include_scope_summary
                and cycle.phase == "complete"
                and (summary is None or summary.unfinished != 0)
            )
        ):
            continue  # Withhold unknown/incompatible coverage instead of emitting a false promise.
        if (
            cycle.mode == "mixed"
            and not include_scope_summary
            and discovery == "scope_enumeration_complete"
        ):
            # Persisted phase does not re-attest today's mutable scope forest.
            # Omitted validation is unknown, not evidence that capture is pending.
            continue
        result[row.sync_id] = CaptureCoverage(
            phase=cycle.phase,
            policies=policies,
            discovery=discovery,
            mode=cycle.mode,
            scope_summary=summary,
            last_full_capture=FullCaptureCoverage(
                cycle_id=evidence.cycle_id,
                completed_at=evidence.completed_at,
                discovery=evidence.discovery,
            )
            if evidence
            else None,
            provider_checkpoint_promoted_at=promoted.promoted_at if promoted else None,
        )
    return result
