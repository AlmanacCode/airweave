"""Legacy editing must not bypass the generation-controlled account lifecycle."""

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.source_connection import SourceConnection


def require_provider_source(short_name: str) -> None:
    """Native imports cannot use provider configuration, OAuth or pull-sync routes."""
    if short_name == "almanac":
        raise HTTPException(status_code=409, detail="Manage this source through its native import")


async def require_unmanaged_source(
    db: AsyncSession, source_id: UUID, organization: UUID, *, short_name: str
) -> None:
    """Owned source identity/configuration is writable only through ensure."""
    require_provider_source(short_name)
    owned = await db.scalar(
        select(OwnedProvisioning.id).where(
            OwnedProvisioning.source_connection_id == source_id,
            OwnedProvisioning.organization_id == organization,
        )
    )
    if owned is not None:
        raise HTTPException(
            status_code=409, detail="Manage this source through its Almanac account"
        )


async def require_provider_sync(db: AsyncSession, sync_id: UUID, organization: UUID) -> None:
    """Check the job's actual sync owner, not a caller-supplied source URL."""
    native = await db.scalar(
        select(SourceConnection.id)
        .where(
            SourceConnection.sync_id == sync_id,
            SourceConnection.organization_id == organization,
            SourceConnection.short_name == "almanac",
        )
        .limit(1)
    )
    if native is not None:
        raise HTTPException(status_code=409, detail="Cancel this job through its native import")
