"""Legacy editing must not bypass the generation-controlled account lifecycle."""

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.models.owned_provisioning import OwnedProvisioning


async def require_unmanaged_source(db: AsyncSession, source_id: UUID, organization: UUID) -> None:
    """Owned source identity/configuration is writable only through ensure."""
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
