"""One restricted control-plane operation; data-plane authentication stays org scoped."""

from uuid import UUID

from fastapi import Depends, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.backend_actor import backend_actor
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.config import settings
from airweave.domains.owned_tenants.models import EnsureOwnedTenant, OwnedTenant
from airweave.domains.owned_tenants.service import OwnedTenantService
from airweave.domains.owned_tenants.store import OwnedTenantStore

router = TrailingSlashRouter()


def enrollment_actor(ctx: ApiContext = Depends(backend_actor)) -> ApiContext:
    """Existing verified key must match both configured control org and key allowlist."""
    if (
        settings.OWNED_TENANT_CONTROL_ORGANIZATION_ID is None
        or not settings.OWNED_TENANT_CONTROL_API_KEY_IDS
    ):
        raise HTTPException(503, "Owned tenant enrollment is not configured")
    if ctx.auth_metadata is None:
        raise HTTPException(403, "Owned tenant enrollment requires its control key")
    try:
        key_id = UUID(ctx.auth_metadata["api_key_id"])
    except (KeyError, ValueError, TypeError):
        raise HTTPException(403, "Owned tenant enrollment requires its control key") from None
    if (
        ctx.organization.id != settings.OWNED_TENANT_CONTROL_ORGANIZATION_ID
        or key_id not in settings.OWNED_TENANT_CONTROL_API_KEY_IDS
    ):
        raise HTTPException(403, "Owned tenant enrollment requires its control key")
    return ctx


def enrollment_service() -> OwnedTenantService:
    """Local composition; no new container service or worker."""
    return OwnedTenantService(OwnedTenantStore())


@router.post("/ensure", response_model=OwnedTenant)
async def ensure_owned_tenant(
    request: EnsureOwnedTenant,
    response: Response,
    db: AsyncSession = Depends(deps.get_db),
    ctx: ApiContext = Depends(enrollment_actor),
    service: OwnedTenantService = Depends(enrollment_service),
) -> OwnedTenant:
    """Only the trusted backend supplies its authenticated owner's opaque subject."""
    response.headers["Cache-Control"] = "no-store"
    return await service.ensure(db, request)
