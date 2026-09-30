"""Describe persisted capture promises independently of indexing counts or job success."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    CaptureCycle,
    CompletionPolicy,
)
from airweave.models.sync_cursor import SyncCursor


class CaptureCoverage(BaseModel):
    """No cycle means unknown; completion only certifies the declared scope policies."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    phase: Literal["active", "complete"]
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
        result[row.sync_id] = CaptureCoverage(
            phase=cycle.phase, policies=policies, discovery=discovery
        )
    return result
