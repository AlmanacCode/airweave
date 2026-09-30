"""Describe persisted capture promises independently of indexing counts or job success."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    CaptureCycle,
    CompletionPolicy,
)
from airweave.models.sync_cursor import SyncCursor


class FullCaptureCoverage(BaseModel):
    """Public coverage evidence excludes private provider continuation values."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    completed_at: AwareDatetime
    discovery: Literal["incomplete", "scope_enumeration_complete"]


class CaptureCoverage(BaseModel):
    """No cycle means unknown; completion only certifies the declared scope policies."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    phase: Literal["active", "complete"]
    mode: Literal["full", "changes"] = "full"
    last_full_capture: FullCaptureCoverage | None = None
    provider_checkpoint_promoted_at: AwareDatetime | None = None
    policies: dict[str, CompletionPolicy]
    discovery: Literal["incomplete", "pending", "scope_enumeration_complete"]


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
        result[row.sync_id] = CaptureCoverage(
            phase=cycle.phase,
            policies=policies,
            discovery=discovery,
            mode=cycle.mode,
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
