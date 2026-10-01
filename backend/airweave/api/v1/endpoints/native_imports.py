"""Backend-only native import intent/recovery; no publisher-controlled fences."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Path
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.backend_actor import backend_actor
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.db.session import get_db
from airweave.domains.entities.canonical.store import WriterBusy
from airweave.domains.native_ingestion.errors import NativeAdmissionError, NativeImportNotFound
from airweave.domains.native_ingestion.import_models import NativeImportState, StartNativeImport

router = TrailingSlashRouter()
RequestKey = Annotated[str, Path(min_length=1, max_length=128)]


@router.put("/{source_id}/imports/{request_key}", response_model=NativeImportState)
async def start_native_import(
    source_id: UUID,
    request_key: RequestKey,
    request: StartNativeImport,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeImportState:
    """Start or recover identical intent in the authenticated organization."""
    try:
        return await container.native_imports.start(
            db, ctx.organization.id, source_id, request_key, request
        )
    except (NativeAdmissionError, WriterBusy) as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error


@router.get("/{source_id}/imports/{request_key}", response_model=NativeImportState)
async def read_native_import(
    source_id: UUID,
    request_key: RequestKey,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeImportState:
    """Read import state without activating any writer."""
    try:
        return await container.native_imports.read(db, ctx.organization.id, source_id, request_key)
    except NativeImportNotFound as error:
        raise HTTPException(404, {"code": error.code, "message": str(error)}) from error
    except NativeAdmissionError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error
