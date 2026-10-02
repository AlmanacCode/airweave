"""Internal service API for committed source records; provider APIs are never read here."""

from asyncio import FIRST_COMPLETED, create_task, gather, wait
from typing import Literal
from uuid import UUID

from fastapi import Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from airweave.api import deps
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.db.session import get_db
from airweave.domains.entities.canonical.calendar_query import CalendarRangeNotCaptured
from airweave.domains.entities.canonical.projection_store import current_extraction
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_models import (
    DocumentRead,
    IndexedRecordRead,
    MailThreadPage,
    RecordChangePage,
    RecordFilters,
    RecordListQuery,
    RecordPage,
    SpreadsheetRead,
)
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.entities.canonical.text_models import TextRead, TextRepresentationList
from airweave.domains.search.owned_models import OwnedSearchRequest, OwnedSearchResponse
from airweave.platform.sources.records.sheets_manifest import GridBounds

router = TrailingSlashRouter()


async def _search_disconnected(request: Request) -> None:
    # FastAPI has consumed this read-only search POST's body before entering the route.
    while (await request.receive())["type"] != "http.disconnect":
        pass


@router.post("/search", response_model=OwnedSearchResponse)
async def search_records(
    request: OwnedSearchRequest,
    http_request: Request,
    sessions: async_sessionmaker[AsyncSession] = Depends(deps.get_search_session_factory),
    ctx: ApiContext = Depends(deps.get_owned_search_context),
    container: Container = Depends(deps.get_container),
) -> OwnedSearchResponse:
    """Retrieve bounded indexed originals; no provider requests or agent execution."""
    work = create_task(container.owned_search.search(sessions, ctx, request))
    disconnected = create_task(_search_disconnected(http_request))
    try:
        done, _ = await wait((work, disconnected), return_when=FIRST_COMPLETED)
        if work in done:
            return await work
        await disconnected
        raise HTTPException(499, "Search client disconnected")
    finally:
        # Join async work before request-scoped dependencies close. Already-running
        # synchronous Vespa or inference work cannot be forcibly stopped here.
        work.cancel()
        disconnected.cancel()
        await gather(work, disconnected, return_exceptions=True)


async def record_error_response(request: Request, error: CanonicalStoreError) -> JSONResponse:
    """Preserve machine-readable recovery codes without exposing SQL/provider diagnostics."""
    status = {
        "source_not_found": 404,
        "record_not_found": 404,
        "blob_not_found": 404,
        "stale_record_revision": 409,
        "blob_unavailable": 503,
        "document_unavailable": 409,
        "spreadsheet_unavailable": 409,
        "text_unavailable": 409,
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
    parent_record_id: UUID | None = None,
    state: Literal["active", "deleted", "all"] = "active",
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> RecordPage:
    """List exact stored records in stable ID order; continuation is a live traversal."""
    query = RecordListQuery(
        filters=RecordFilters(
            record_type=record_type,
            container_id=container_id,
            parent_record_id=parent_record_id,
            state=state,
        ),
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


@router.get("/{sync_id}/records/{record_id}", response_model=IndexedRecordRead)
async def read_record(
    sync_id: UUID,
    record_id: UUID,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> IndexedRecordRead:
    """Read current committed provider state, including tombstones and completeness."""
    record = await service.read(db, ctx.organization.id, sync_id, record_id)
    extraction = await current_extraction(
        db, ctx.organization.id, sync_id, record_id, record.revision
    )
    return IndexedRecordRead(**record.model_dump(), extraction=extraction)


@router.get("/{sync_id}/records/{record_id}/spreadsheet", response_model=SpreadsheetRead)
async def read_stored_spreadsheet(
    sync_id: UUID,
    record_id: UUID,
    response: Response,
    revision: int = Query(ge=1),
    sheet_id: int | None = Query(None, ge=0),
    start_row: int | None = Query(None, ge=0),
    end_row: int | None = Query(None, gt=0),
    start_column: int | None = Query(None, ge=0),
    end_column: int | None = Query(None, gt=0),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
    container: Container = Depends(deps.get_container),
) -> SpreadsheetRead:
    """Read exact retained spreadsheet cells; no live provider fallback."""
    values = (sheet_id, start_row, end_row, start_column, end_column)
    if any(value is not None for value in values) and any(value is None for value in values):
        raise HTTPException(status_code=422, detail="Supply all grid bounds or none")
    try:
        bounds = (
            None
            if sheet_id is None
            else GridBounds(
                sheet_id=sheet_id,
                start_row=start_row,
                end_row=end_row,
                start_column=start_column,
                end_column=end_column,
            )
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="Grid bounds must be nonempty") from None
    result = await service.spreadsheet(
        db, ctx.organization.id, sync_id, record_id, revision, container.storage_backend, bounds
    )
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return result


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


@router.get(
    "/{sync_id}/records/{record_id}/text-representations", response_model=TextRepresentationList
)
async def list_text_representations(
    sync_id: UUID,
    record_id: UUID,
    response: Response,
    revision: int = Query(ge=1),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
    container: Container = Depends(deps.get_container),
) -> TextRepresentationList:
    """Describe derived text only from the current authorized index publication."""
    from airweave.domains.entities.canonical.text_query import CanonicalTextReader

    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return await CanonicalTextReader(service, container.storage_backend).list(
        db,
        ctx.organization.id,
        sync_id,
        record_id,
        revision,
    )


@router.get(
    "/{sync_id}/records/{record_id}/text-representations/{representation_id}",
    response_model=TextRead,
)
async def read_text_representation(
    sync_id: UUID,
    record_id: UUID,
    response: Response,
    representation_id: UUID,
    generation: UUID,
    revision: int = Query(ge=1),
    offset: int = Query(0, ge=0),
    limit: int = Query(12000, ge=1, le=100000),
    view: Literal["content", "index"] = "content",
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
    container: Container = Depends(deps.get_container),
) -> TextRead:
    """Read complete retained converter text in bounded character ranges."""
    from airweave.domains.entities.canonical.text_query import CanonicalTextReader

    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return await CanonicalTextReader(service, container.storage_backend).read(
        db,
        ctx.organization.id,
        sync_id,
        record_id,
        revision,
        generation,
        representation_id,
        offset=offset,
        limit=limit,
        view=view,
    )
