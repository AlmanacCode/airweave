"""Backend-only attested device capture; source contents never enter validation diagnostics."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Path, Query, Request
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.backend_actor import backend_actor
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.db.session import get_db
from airweave.domains.device_ingestion.models import (
    BindDevice,
    DeviceBeginRequest,
    DevicePageAck,
    DevicePrincipal,
    DeviceRunState,
    DeviceSourceState,
    DeviceUploadHandle,
    DeviceUploadIntent,
    EnsureDeviceSource,
    RevokeDevice,
)
from airweave.domains.device_ingestion.store import DeviceSourceNotFound
from airweave.domains.entities.canonical.store import CanonicalStoreError

router = TrailingSlashRouter()
RequestKey = Annotated[str, Path(min_length=1, max_length=128)]
OwnerID = Annotated[str, Query(min_length=1, max_length=255)]


def admission_error(error: CanonicalStoreError) -> HTTPException:
    """Stable codes without source body, arbitrary error attributes or credentials."""
    return HTTPException(
        404 if isinstance(error, DeviceSourceNotFound) else 409,
        {"code": error.code, "message": str(error)},
    )


@router.put("", response_model=DeviceSourceState)
async def ensure_source(
    request: EnsureDeviceSource,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceSourceState:
    """Ensure immutable owner/account/source identity without fabricating OAuth credentials."""
    try:
        return await container.device_ingestion.ensure(db, ctx.organization.id, request)
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.get("/{source_id}", response_model=DeviceSourceState)
async def read_source(
    source_id: UUID,
    owner_id: OwnerID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceSourceState:
    """Owner-scoped read returns enrollment metadata only."""
    try:
        return await container.device_ingestion.get(db, ctx.organization.id, source_id, owner_id)
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.put("/{source_id}/binding", response_model=DeviceSourceState)
async def bind_device(
    source_id: UUID,
    request: BindDevice,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceSourceState:
    """Explicit backend-attested reauthorization replaces a primary device with CAS."""
    try:
        return await container.device_ingestion.bind(db, ctx.organization.id, source_id, request)
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.post("/{source_id}/revoke", response_model=DeviceSourceState)
async def revoke_device(
    source_id: UUID,
    request: RevokeDevice,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceSourceState:
    """Remote generation revoke stops writes and withdraws retained-source read authority."""
    try:
        return await container.device_ingestion.revoke(db, ctx.organization.id, source_id, request)
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.put("/{source_id}/runs/{request_key}", response_model=DeviceRunState)
async def start_run(
    source_id: UUID,
    request_key: RequestKey,
    request: DeviceBeginRequest,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceRunState:
    """Recover exact bounded-run intent; server alone creates the writer fence."""
    try:
        return await container.device_ingestion.start(
            db, ctx.organization.id, source_id, request_key, request
        )
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.get("/{source_id}/runs/{request_key}", response_model=DeviceRunState)
async def read_run(
    source_id: UUID,
    request_key: RequestKey,
    owner_id: OwnerID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceRunState:
    """Read current/terminal metadata without restoring a device or exposing private data."""
    try:
        return await container.device_ingestion.get_run(
            db, ctx.organization.id, source_id, request_key, owner_id
        )
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.put("/{source_id}/runs/{request_key}/pages", response_model=DevicePageAck)
async def commit_page(
    source_id: UUID,
    request_key: RequestKey,
    request: Request,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DevicePageAck:
    """Forward exact UTF-8 JSON bytes; never roundtrip a native Int64 through JS numbers."""
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > 2 * 1024 * 1024:
            raise HTTPException(413, {"code": "device_page_too_large"})
        body.extend(chunk)
    try:
        return await container.device_ingestion.page(
            db, ctx.organization.id, source_id, request_key, bytes(body)
        )
    except ValidationError as error:
        raise HTTPException(
            422, {"code": "invalid_device_page", "message": "Device page schema is invalid"}
        ) from error
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.post("/{source_id}/runs/{request_key}/complete", response_model=DeviceRunState)
async def complete_run(
    source_id: UUID,
    request_key: RequestKey,
    request: DevicePrincipal,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceRunState:
    """Certify bounded collection only after its final page; indexing remains unverified."""
    try:
        return await container.device_ingestion.complete(
            db, ctx.organization.id, source_id, request_key, request
        )
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.put("/{source_id}/runs/{request_key}/uploads/{handle}/intent")
async def declare_upload(
    source_id: UUID,
    request_key: RequestKey,
    handle: UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceUploadHandle:
    """Declare immutable attachment membership and expected bytes before binary transfer."""
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > 2 * 1024 * 1024:
            raise HTTPException(413, {"code": "device_upload_intent_too_large"})
        body.extend(chunk)
    try:
        intent = DeviceUploadIntent.model_validate_json(bytes(body))
        return await container.device_ingestion.declare_upload(
            db, ctx.organization.id, source_id, request_key, handle, intent
        )
    except (ValueError, ValidationError) as error:
        raise HTTPException(422, {"code": "invalid_device_upload_intent"}) from error
    except CanonicalStoreError as error:
        raise admission_error(error) from error


@router.put("/{source_id}/runs/{request_key}/uploads/{handle}/content")
async def upload_content(
    source_id: UUID,
    request_key: RequestKey,
    handle: UUID,
    request: Request,
    owner_id: OwnerID,
    device_id: UUID,
    generation: Annotated[int, Query(ge=1)],
    store_generation: UUID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(backend_actor),
    container: Container = Depends(deps.get_container),
) -> DeviceUploadHandle:
    """Bound raw binary without accepting multipart paths or storage selectors."""
    content = bytearray()
    async for chunk in request.stream():
        if len(content) + len(chunk) > 8 * 1024 * 1024:
            raise HTTPException(413, {"code": "device_attachment_too_large"})
        content.extend(chunk)
    publisher = DevicePrincipal(
        owner_id=owner_id,
        device_id=device_id,
        generation=generation,
        store_generation=store_generation,
    )
    try:
        return await container.device_ingestion.upload(
            db, ctx.organization.id, source_id, request_key, handle, publisher, bytes(content)
        )
    except CanonicalStoreError as error:
        raise admission_error(error) from error
