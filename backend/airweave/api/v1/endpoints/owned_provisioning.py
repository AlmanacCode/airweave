"""Backend-only account provisioning; org keys retain their existing broad authority."""

from uuid import UUID

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.db.session import get_db
from airweave.domains.owned_provisioning.models import EnsureSource, ProvisionedSource

router = TrailingSlashRouter()


def backend_actor(ctx: ApiContext = Depends(deps.get_context)) -> ApiContext:
    """A user session cannot act as Almanac's credential/identity authority."""
    if not ctx.is_api_key_auth:
        raise HTTPException(status_code=403, detail="Owned provisioning requires a backend API key")
    return ctx


@router.put("/{account_id}", response_model=ProvisionedSource)
async def ensure_owned_source(
    account_id: UUID,
    request: EnsureSource,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> ProvisionedSource:
    """Retry the identical generation after uncertain outcomes; IDs remain stable."""
    return await container.owned_provisioning.ensure(db, ctx, account_id, request)


@router.get("/{account_id}", response_model=ProvisionedSource)
async def read_owned_source(
    account_id: UUID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> ProvisionedSource:
    """Read provisioning state without contacting the provider or starting capture."""
    return await container.owned_provisioning.get(db, ctx, account_id)
