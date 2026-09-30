"""Internal service API for committed source records; provider APIs are never read here."""

from typing import Literal
from uuid import UUID

from fastapi import Depends, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.db.session import get_db
from airweave.domains.entities.canonical.calendar_query import CalendarRangeNotCaptured
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_models import (
    DocumentRead,
    MailThreadPage,
    RecordChangePage,
    RecordFilters,
    RecordListQuery,
    RecordPage,
)
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.search.owned_models import OwnedSearchRequest, OwnedSearchResponse

router = TrailingSlashRouter()


@router.post("/search", response_model=OwnedSearchResponse)
async def search_records(
    request: OwnedSearchRequest,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    container: Container = Depends(deps.get_container),
) -> OwnedSearchResponse:
    """Retrieve bounded indexed originals; no provider requests or agent execution."""
    return await container.owned_search.search(db, ctx, request)


async def record_error_response(request: Request, error: CanonicalStoreError) -> JSONResponse:
    """Preserve machine-readable recovery codes without exposing SQL/provider diagnostics."""
    status = {
        "source_not_found": 404,
        "record_not_found": 404,
        "blob_not_found": 404,
        "stale_record_revision": 409,
        "blob_unavailable": 503,
        "document_unavailable": 409,
        "document_incomplete": 409,
        "calendar_changed_restart": 409,
        "calendar_read_incomplete": 409,
        "calendar_range_not_captured": 409,
    }.get(error.code, 400)
    detail = {"code": error.code, "message": str(error), "retryable": status == 503}
    if isinstance(error, CalendarRangeNotCaptured):
        detail["action"] = error.action
    return JSONResponse(status_code=status, content={"error": detail})


@router.get("/{sync_id}/records", response_model=RecordPage)
async def list_records(
    sync_id: UUID,
    record_type: str | None = None,
    container_id: str | None = None,
    state: Literal["active", "deleted", "all"] = "active",
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> RecordPage:
    """List exact stored records in stable ID order; continuation is a live traversal."""
    query = RecordListQuery(
        filters=RecordFilters(record_type=record_type, container_id=container_id, state=state),
        cursor=cursor,
        limit=limit,
    )
    return await service.list_records(db, ctx.organization.id, sync_id, query)


@router.get("/{sync_id}/records/changes", response_model=RecordChangePage)
async def record_changes(
    sync_id: UUID,
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> RecordChangePage:
    """Read observed changes from the beginning, or resume a source-bound cursor."""
    return await service.changes(db, ctx.organization.id, sync_id, cursor=cursor, limit=limit)


@router.get("/{sync_id}/records/{record_id}", response_model=SourceRecord)
async def read_record(
    sync_id: UUID,
    record_id: UUID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> SourceRecord:
    """Read current committed provider state, including tombstones and completeness."""
    return await service.read(db, ctx.organization.id, sync_id, record_id)


@router.get("/{sync_id}/records/{record_id}/document", response_model=DocumentRead)
async def read_stored_document(
    sync_id: UUID,
    record_id: UUID,
    response: Response,
    revision: int = Query(ge=1),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
    container: Container = Depends(deps.get_container),
) -> DocumentRead:
    """Read exact retained native Docs content; never fetch or fall back to a provider."""
    result = await service.document(
        db, ctx.organization.id, sync_id, record_id, revision, container.storage_backend
    )
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return result


@router.get("/{sync_id}/mail/threads/{thread_id}", response_model=MailThreadPage)
async def mail_thread(
    sync_id: UUID,
    thread_id: str = Path(min_length=1, max_length=512),
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> MailThreadPage:
    """Read observed Gmail messages in chronological order without querying the provider."""
    return await service.mail_thread(
        db, ctx.organization.id, sync_id, thread_id, cursor=cursor, limit=limit
    )


@router.get("/{sync_id}/records/{record_id}/blobs/{sha256}")
async def record_blob(
    sync_id: UUID,
    record_id: UUID,
    sha256: str = Path(pattern=r"^[a-f0-9]{64}$"),
    revision: int = Query(ge=1),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
    container: Container = Depends(deps.get_container),
) -> Response:
    """Return verified owned bytes only; clients cannot supply storage keys or remote URLs."""
    content = await service.blob(
        db, ctx.organization.id, sync_id, record_id, revision, sha256, container.storage_backend
    )
    return Response(
        content,
        media_type="application/octet-stream",
        headers={
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
            "Content-Disposition": "attachment",
            "ETag": '"' + sha256 + '"',
        },
    )
