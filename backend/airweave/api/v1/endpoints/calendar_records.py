"""Calendar range reads use captured provider instances, never live provider requests."""

from uuid import UUID
from zoneinfo import ZoneInfoNotFoundError

from fastapi import Depends, HTTPException, Path, Query
from pydantic import AwareDatetime, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api import deps
from airweave.api.context import ApiContext
from airweave.api.router import TrailingSlashRouter
from airweave.core.config import settings
from airweave.db.session import get_db
from airweave.domains.entities.canonical.calendar_exact import read_event
from airweave.domains.entities.canonical.calendar_query import (
    CalendarRange,
    CalendarRangePage,
    CalendarRangeService,
)
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.store import SourceNotFound
from airweave.models.source_connection import SourceConnection

router = TrailingSlashRouter()


async def _authorized_connection(db: AsyncSession, organization_id: UUID, sync_id: UUID):
    identity = await db.scalar(
        select(SourceConnection.id).where(
            SourceConnection.organization_id == organization_id,
            SourceConnection.sync_id == sync_id,
            SourceConnection.short_name == "google_calendar",
            SourceConnection.is_authenticated.is_(True),
        )
    )
    if identity is None:
        raise SourceNotFound("Calendar source is not available")
    return identity


@router.get("/{sync_id}/calendar/events", response_model=CalendarRangePage)
async def calendar_events(
    sync_id: UUID,
    calendar_id: str = Query(min_length=1),
    start: AwareDatetime = Query(),
    end: AwareDatetime = Query(),
    timezone: str = "UTC",
    limit: int = Query(100, ge=1, le=250),
    cursor: str | None = None,
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
) -> CalendarRangePage:
    """List observed occurrences; requests beyond coverage include an explicit sync action."""
    try:
        query = CalendarRange(start=start, end=end, timezone=timezone, limit=limit, cursor=cursor)
    except (ValidationError, ZoneInfoNotFoundError) as exc:
        raise HTTPException(422, "Invalid timezone or window; reads are limited to31days") from exc
    connection_id = await _authorized_connection(db, ctx.organization.id, sync_id)
    result = await CalendarRangeService(settings.STATE_SECRET).read(
        db, ctx.organization.id, sync_id, calendar_id, query
    )

    if await _authorized_connection(db, ctx.organization.id, sync_id) != connection_id:
        raise SourceNotFound("Calendar source changed while reading")
    return result


@router.get("/{sync_id}/calendar/events/{event_id}", response_model=SourceRecord)
async def calendar_event(
    sync_id: UUID,
    event_id: str = Path(min_length=1, max_length=1024),
    calendar_id: str = Query(min_length=1, max_length=1024),
    db: AsyncSession = Depends(get_db),
    ctx: ApiContext = Depends(deps.get_context),
) -> SourceRecord:
    """Read one captured provider identity; no live lookup or window expansion."""
    connection = await _authorized_connection(db, ctx.organization.id, sync_id)
    result = await read_event(db, ctx.organization.id, sync_id, calendar_id, event_id)
    if await _authorized_connection(db, ctx.organization.id, sync_id) != connection:
        raise SourceNotFound("Calendar source changed while reading")
    return result
