"""Backend-key native source binding; no OAuth, imports or provider execution."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.backend_actor import backend_owned_actor
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.publication_models import (
    NativeInventoryPage,
    NativePublication,
)
from airweave.domains.native_ingestion.source_models import (
    EnsureNativeSource,
    NativeSource,
    WithdrawNativeSource,
)

router = TrailingSlashRouter()


@router.put("", response_model=NativeSource)
async def ensure_native_source(
    request: EnsureNativeSource,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> NativeSource:
    """Ensure one immutable owner/dataset binding in the authenticated organization."""
    return await container.native_sources.ensure(db, ctx.organization.id, request)


@router.get("/{source_id}", response_model=NativeSource)
async def read_native_source(
    source_id: UUID,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> NativeSource:
    """Read the existing binding without starting synchronization."""
    return await container.native_sources.get(db, ctx.organization.id, source_id)


@router.get("/{source_id}/publication", response_model=NativePublication)
async def read_native_publication(
    source_id: UUID,
    owner_id: Annotated[str, Query(min_length=1)],
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> NativePublication:
    """Read current writer intent and terminal ACK under an explicit owner binding."""
    try:
        return await container.native_sources.publication(
            db, ctx.organization.id, source_id, owner_id
        )
    except NativeAdmissionError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error


@router.get("/{source_id}/records", response_model=NativeInventoryPage)
async def list_native_inventory(
    source_id: UUID,
    owner_id: Annotated[str, Query(min_length=1)],
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    after: UUID | None = None,
    roots_only: bool = False,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> NativeInventoryPage:
    """Read bounded retained identities/access; never infer upstream absence from this page."""
    try:
        return await container.native_sources.inventory(
            db,
            ctx.organization.id,
            source_id,
            owner_id,
            limit=limit,
            after=after,
            roots_only=roots_only,
        )
    except NativeAdmissionError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error


@router.post("/{source_id}/unavailable", response_model=NativeSource)
async def withdraw_native_source(
    source_id: UUID,
    request: WithdrawNativeSource,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> NativeSource:
    """Attest explicit owner loss without scanning or rewriting retained originals."""
    return await container.native_sources.withdraw(
        db, ctx.organization.id, source_id, request.owner_id
    )
