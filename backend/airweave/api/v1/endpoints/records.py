"""Internal service API for committed source records; provider APIs are never read here."""

from asyncio import FIRST_COMPLETED, Task, create_task, gather, wait
from typing import Literal
from uuid import UUID

from fastapi import Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from airweave.api import deps
from airweave.api.backend_actor import backend_owned_actor
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.container import Container
from airweave.domains.entities.canonical.calendar_query import CalendarRangeNotCaptured
from airweave.domains.entities.canonical.drive_models import (
    DriveFilters,
    DriveListQuery,
    DriveMetadataPage,
    DriveMetadataRead,
)
from airweave.domains.entities.canonical.drive_models import (
    NativeID as DriveID,
)
from airweave.domains.entities.canonical.drive_query import CanonicalDriveQuery
from airweave.domains.entities.canonical.mail_models import (
    MailFilters,
    MailMessagePage,
    MailMessageQuery,
)
from airweave.domains.entities.canonical.mail_query import CanonicalMailQuery
from airweave.domains.entities.canonical.models import SourceRecord
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
from airweave.domains.entities.canonical.slack_models import (
    SlackChannel,
    SlackThreadPage,
    SlackThreadQuery,
    SlackTimestamp,
)
from airweave.domains.entities.canonical.slack_query import CanonicalSlackQuery
from airweave.domains.entities.canonical.status_models import SourceStatus
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.entities.canonical.text_models import TextRead, TextRepresentationList
from airweave.domains.entities.canonical.wispr_models import (
    MeetingFilters,
    MeetingListQuery,
    MeetingPage,
)
from airweave.domains.entities.canonical.wispr_query import CanonicalWisprQuery
from airweave.domains.search.owned_models import (
    OwnedCandidatesResponse,
    OwnedRankRequest,
    OwnedRankResponse,
    OwnedSearchRequest,
    OwnedSearchResponse,
)
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
    sessions: async_sessionmaker[AsyncSession] = Depends(deps.get_tenant_session_factory),
    ctx: ApiContext = Depends(deps.get_owned_context),
    container: Container = Depends(deps.get_container),
) -> OwnedSearchResponse:
    """Retrieve bounded indexed originals; no provider requests or agent execution."""
    work = create_task(container.owned_search.search(sessions, ctx, request))
    return await _await_search(work, http_request)


@router.post("/search/candidates", response_model=OwnedCandidatesResponse)
async def search_candidates(
    request: OwnedSearchRequest,
    http_request: Request,
    sessions: async_sessionmaker[AsyncSession] = Depends(deps.get_tenant_session_factory),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> OwnedCandidatesResponse:
    """Internal unranked candidates; the caller must check native Almanac authority."""
    work = create_task(container.owned_search.candidates(sessions, ctx, request))
    return await _await_search(work, http_request)


@router.post("/search/rank", response_model=OwnedRankResponse)
async def rank_candidates(
    request: OwnedRankRequest,
    http_request: Request,
    sessions: async_sessionmaker[AsyncSession] = Depends(deps.get_tenant_session_factory),
    ctx: ApiContext = Depends(backend_owned_actor),
    container: Container = Depends(deps.get_container),
) -> OwnedRankResponse:
    """Rank only the shortlist already approved by the trusted product backend."""
    work = create_task(container.owned_search.rank_candidates(sessions, ctx, request))
    return await _await_search(work, http_request)


async def _await_search[T](work: Task[T], http_request: Request) -> T:
    """Cancel either retrieval path when its caller disconnects."""
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
        "mail_changed_restart": 409,
        "meetings_changed_restart": 409,
        "slack_thread_changed_restart": 409,
        "slack_thread_unavailable": 409,
        "calendar_read_incomplete": 409,
        "calendar_range_not_captured": 409,
        "drive_changed_restart": 409,
        "drive_metadata_unavailable": 409,
    }.get(error.code, 400)
    detail = {"code": error.code, "message": str(error), "retryable": status == 503}
    if isinstance(error, CalendarRangeNotCaptured):
        detail["action"] = error.action
    return JSONResponse(status_code=status, content={"error": detail})


@router.get("/{sync_id}/drive/files", response_model=DriveMetadataPage)
async def drive_files(
    sync_id: UUID,
    folder: DriveID | None = None,
    drive: DriveID | None = None,
    name: str | None = Query(default=None, min_length=1, max_length=512),
    mime_type: str | None = Query(default=None, pattern=r"^[\w.+-]+/[\w.+-]+$"),
    sort: Literal["name", "updated"] = "name",
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None, min_length=1, max_length=16384),
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> DriveMetadataPage:
    """Enumerate current saved file metadata without projection or provider requests."""
    return await CanonicalDriveQuery(service.signing_key).files(
        db,
        ctx.organization.id,
        sync_id,
        DriveListQuery(
            filters=DriveFilters(
                folder=folder, drive=drive, name=name, mime_type=mime_type, sort=sort
            ),
            limit=limit,
            cursor=cursor,
        ),
    )


@router.get("/{sync_id}/drive/files/{file_id}", response_model=DriveMetadataRead)
async def drive_file(
    sync_id: UUID,
    file_id: DriveID,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> DriveMetadataRead:
    """Resolve one native file identity within this authorized captured Drive."""
    return await CanonicalDriveQuery(service.signing_key).file(
        db, ctx.organization.id, sync_id, file_id
    )


@router.get("/{sync_id}/status", response_model=SourceStatus)
async def retained_source_status(
    sync_id: UUID,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> SourceStatus:
    """Observe saved import/preparation facts; never execute provider or model calls."""
    return await service.status(db, ctx.organization.id, sync_id)


@router.get("/{sync_id}/records", response_model=RecordPage)
async def list_records(
    sync_id: UUID,
    record_type: str | None = None,
    container_id: str | None = None,
    parent_record_id: UUID | None = None,
    state: Literal["active", "deleted", "all"] = "active",
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> RecordChangePage:
    """Read observed changes from the beginning, or resume a source-bound cursor."""
    return await service.changes(db, ctx.organization.id, sync_id, cursor=cursor, limit=limit)


@router.get("/{sync_id}/records/{record_id}", response_model=IndexedRecordRead)
async def read_record(
    sync_id: UUID,
    record_id: UUID,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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


@router.get("/{sync_id}/slack/threads/{thread_ts}", response_model=SlackThreadPage)
async def slack_thread(
    sync_id: UUID,
    thread_ts: SlackTimestamp,
    channel: SlackChannel,
    response: Response,
    limit: int = Query(default=15, ge=1, le=15),
    cursor: str | None = Query(default=None, max_length=16384),
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> SlackThreadPage:
    """Read observed native Slack thread messages; never fetch omitted history."""
    result = await CanonicalSlackQuery(service.signing_key).thread(
        db,
        ctx.organization.id,
        sync_id,
        SlackThreadQuery(channel=channel, thread_ts=thread_ts, limit=limit, cursor=cursor),
    )
    response.headers["Cache-Control"] = "private, no-store"
    return result


@router.get("/{sync_id}/wispr/meetings", response_model=MeetingPage)
async def wispr_meetings(
    sync_id: UUID,
    response: Response,
    after: AwareDatetime | None = None,
    before: AwareDatetime | None = None,
    limit: int = Query(default=5, ge=1, le=100),
    cursor: str | None = None,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> MeetingPage:
    """List captured meetings by native start; bodies stay on exact retained reads."""
    try:
        filters = MeetingFilters(after=after, before=before)
    except ValidationError:
        raise HTTPException(422, "Invalid retained meeting interval") from None
    result = await CanonicalWisprQuery(service.signing_key).meetings(
        db,
        ctx.organization.id,
        sync_id,
        MeetingListQuery(filters=filters, limit=limit, cursor=cursor),
    )
    response.headers["Cache-Control"] = "private, no-store"
    return result


@router.get("/{sync_id}/mail/messages", response_model=MailMessagePage)
async def mail_messages(
    sync_id: UUID,
    query: str = Query(default="", max_length=4096),
    from_addresses: list[str] = Query(default=[]),
    to_addresses: list[str] = Query(default=[]),
    after: AwareDatetime | None = None,
    before: AwareDatetime | None = None,
    folder: Literal["inbox", "sent", "trash", "spam", "drafts"] | None = None,
    unread: bool | None = None,
    limit: int = Query(default=100, ge=1, le=100),
    cursor: str | None = None,
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> MailMessagePage:
    """Enumerate retained Gmail metadata; literal body search never contacts the provider."""
    try:
        filters = MailFilters(
            query=query,
            from_addresses=tuple(from_addresses),
            to_addresses=tuple(to_addresses),
            after=after,
            before=before,
            folder=folder,
            unread=unread,
        )
    except ValidationError:
        raise HTTPException(422, "Invalid retained mail filters") from None
    return await CanonicalMailQuery(service.signing_key).messages(
        db,
        ctx.organization.id,
        sync_id,
        MailMessageQuery(filters=filters, limit=limit, cursor=cursor),
    )


@router.get("/{sync_id}/mail/messages/{message_id}", response_model=SourceRecord)
async def mail_message(
    sync_id: UUID,
    message_id: str = Path(min_length=1, max_length=512),
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
    service: CanonicalQueryService = Depends(deps.get_canonical_query_service),
) -> SourceRecord:
    """Read one retained native message identity without querying the provider."""
    return await service.mail_message(db, ctx.organization.id, sync_id, message_id)


@router.get("/{sync_id}/mail/threads/{thread_id}", response_model=MailThreadPage)
async def mail_thread(
    sync_id: UUID,
    thread_id: str = Path(min_length=1, max_length=512),
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=100),
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
    db: AsyncSession = Depends(deps.get_tenant_db),
    ctx: ApiContext = Depends(deps.get_owned_context),
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
