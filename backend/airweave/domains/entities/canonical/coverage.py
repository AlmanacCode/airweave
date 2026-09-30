"""Describe persisted capture promises independently of indexing counts or job success."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    CaptureCycle,
    CompletionPolicy,
)
from airweave.domains.entities.canonical.cycle_store import root_ready
from airweave.domains.entities.canonical.requests import WriterFence
from airweave.domains.entities.canonical.store import content_is_available
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor


class FullCaptureCoverage(BaseModel):
    """Public coverage evidence excludes private provider continuation values."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    completed_at: AwareDatetime
    discovery: Literal["incomplete", "scope_enumeration_complete"]


class CaptureScopeSummary(BaseModel):
    """Counts describe eligible exact scopes, never original/indexed record totals."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    eligible: int = Field(ge=0, strict=True)
    completed_full: int = Field(ge=0, strict=True)
    completed_changes: int = Field(ge=0, strict=True)
    unfinished: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def partition(self):
        """Every eligible scope belongs to exactly one progress category."""
        if self.eligible != self.completed_full + self.completed_changes + self.unfinished:
            raise ValueError("Scope progress must partition eligible scopes")
        return self


class CaptureCoverage(BaseModel):
    """No cycle means unknown; completion only certifies the declared scope policies."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    phase: Literal["active", "complete"]
    mode: Literal["full", "changes", "mixed"] = "full"
    scope_summary: CaptureScopeSummary | None = None
    last_full_capture: FullCaptureCoverage | None = None
    provider_checkpoint_promoted_at: AwareDatetime | None = None
    policies: dict[str, CompletionPolicy]
    discovery: Literal["incomplete", "pending", "scope_enumeration_complete"]


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
    if not await root_ready(db, fence, cycle):
        return None
    eligible = full = changes = 0
    common = (
        CaptureScan.organization_id == organization_id,
        CaptureScan.sync_id == sync_id,
        CaptureScan.cycle_id == cycle.version.cycle_id,
        CaptureScan.phase == "complete",
    )
    for kind in cycle.configuration.root_record_types:
        eligible += 1
        mode = await db.scalar(
            select(CaptureScan.execution_state["plan"]["mode"].astext).where(
                *common,
                CaptureScan.record_type == kind,
                CaptureScan.parent_record_id.is_(None),
                CaptureScan.container_id.is_(None),
            )
        )
        full += int(mode == "full")
        changes += int(mode == "changes")
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
    db: AsyncSession, organization_id: UUID, sync_ids: tuple[UUID, ...]
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
            if cycle.mode == "mixed"
            else None
        )
        if cycle.mode == "mixed" and (
            evidence is not None
            or promoted is not None
            or (cycle.phase == "complete" and (summary is None or summary.unfinished != 0))
        ):
            continue  # Withhold unknown/incompatible coverage instead of emitting a false promise.
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
