"""Backend-key native source binding; no OAuth, imports or provider execution."""

from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.backend_actor import backend_actor
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.db.session import get_db
from airweave.domains.native_ingestion.source_models import EnsureNativeSource, NativeSource

router = TrailingSlashRouter()


@router.put("", response_model=NativeSource)
async def ensure_native_source(
    request: EnsureNativeSource,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeSource:
    """Ensure one immutable owner/dataset binding in the authenticated organization."""
    return await container.native_sources.ensure(db, ctx.organization.id, request)


@router.get("/{source_id}", response_model=NativeSource)
async def read_native_source(
    source_id: UUID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeSource:
    """Read the existing binding without starting synchronization."""
    return await container.native_sources.get(db, ctx.organization.id, source_id)
