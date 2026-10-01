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
from airweave.domains.entities.canonical.store import CanonicalStoreError, WriterBusy
from airweave.domains.native_ingestion.errors import NativeAdmissionError, NativeImportNotFound
from airweave.domains.native_ingestion.import_models import NativeImportState, StartNativeImport
from airweave.domains.native_ingestion.page_models import CommitNativePage, NativePageAck
from airweave.domains.native_ingestion.scope_models import (
    BeginNativeScope,
    NativeScopeRef,
    NativeScopeState,
    ReconcileNativeScope,
)

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


@router.put("/{source_id}/imports/{request_key}/scopes", response_model=NativeScopeState)
async def begin_scope(
    source_id: UUID,
    request_key: RequestKey,
    request: BeginNativeScope,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeScopeState:
    """Begin scope using server-held import authority."""
    try:
        return await container.native_imports.begin_scope(
            db, ctx.organization.id, source_id, request_key, request
        )
    except NativeImportNotFound as error:
        raise HTTPException(404, {"code": error.code, "message": str(error)}) from error
    except CanonicalStoreError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error


@router.post("/{source_id}/imports/{request_key}/scopes/read", response_model=NativeScopeState)
async def read_scope(
    source_id: UUID,
    request_key: RequestKey,
    request: NativeScopeRef,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeScopeState:
    """Read scope using server-held import authority."""
    try:
        return await container.native_imports.read_scope(
            db, ctx.organization.id, source_id, request_key, request
        )
    except NativeImportNotFound as error:
        raise HTTPException(404, {"code": error.code, "message": str(error)}) from error
    except CanonicalStoreError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error


@router.post("/{source_id}/imports/{request_key}/scopes/reconcile", response_model=NativeScopeState)
async def reconcile_scope(
    source_id: UUID,
    request_key: RequestKey,
    request: ReconcileNativeScope,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativeScopeState:
    """Reconcile scope using server-held import authority."""
    try:
        return await container.native_imports.reconcile_scope(
            db, ctx.organization.id, source_id, request_key, request
        )
    except NativeImportNotFound as error:
        raise HTTPException(404, {"code": error.code, "message": str(error)}) from error
    except CanonicalStoreError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error


@router.put("/{source_id}/imports/{request_key}/pages", response_model=NativePageAck)
async def page(
    source_id: UUID,
    request_key: RequestKey,
    request: CommitNativePage,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> NativePageAck:
    """Page using server-held import authority."""
    try:
        return await container.native_imports.page(
            db, ctx.organization.id, source_id, request_key, request
        )
    except NativeImportNotFound as error:
        raise HTTPException(404, {"code": error.code, "message": str(error)}) from error
    except CanonicalStoreError as error:
        raise HTTPException(409, {"code": error.code, "message": str(error)}) from error
