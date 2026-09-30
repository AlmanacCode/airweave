"""Bounded, store-only calendar ranges over provider-expanded original occurrences."""

import json
from datetime import datetime, time, timedelta, timezone
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jose import JWTError, jwt
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import String, and_, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.checkpoint import CanonicalCheckpoint
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query import InvalidRecordCursor, RecordNotFound
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    SourceNotFound,
    content_is_available,
    source_record,
)
from airweave.models import Entity, Sync, SyncCursor, SyncJob
from airweave.platform.cursors.google_calendar import CalendarWindowCoverage, GoogleCalendarCursor


class CalendarRangeError(CanonicalStoreError):
    """Calendar reads cannot silently fetch providers or truncate their input."""

    code = "calendar_range_budget_exceeded"


class CalendarRangeNotCaptured(CanonicalStoreError):
    """Ask for an explicit configured capture before reading this interval."""

    code = "calendar_range_not_captured"

    def __init__(self, start: datetime, end: datetime):
        """Carry a machine-readable explicit sync request, without executing it."""
        super().__init__(
            "Requested range is not captured; configure occurrence_window and run sync"
        )
        self.action = {
            "operation": "sync_calendar_window",
            "config": {"occurrence_window": {"start": start.isoformat(), "end": end.isoformat()}},
        }


class CalendarChanged(CanonicalStoreError):
    """A continuation cannot mix changed local records or coverage generations."""

    code = "calendar_changed_restart"


class CalendarRange(BaseModel):
    """Fixed request window and ordering; only cursor position varies between pages."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    start: AwareDatetime
    end: AwareDatetime
    timezone: str
    limit: int = Field(default=100, ge=1, le=250)
    cursor: str | None = None

    @model_validator(mode="after")
    def validate_window(self):
        """Validate timezone and bounded half-open interval."""
        if not timedelta(0) < self.end - self.start <= timedelta(days=31):
            raise ValueError("Calendar read window must be positive and at most31days")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("Unknown calendar timezone") from exc
        return self


class CalendarRangeCursor(BaseModel):
    """Signed scope, exact query, checkpoint and stable position."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_calendar_range_v1"] = "canonical_calendar_range_v1"
    organization_id: UUID
    sync_id: UUID
    calendar_id: str
    start: AwareDatetime
    end: AwareDatetime
    timezone: str
    sequence: int
    coverage_scan_id: UUID
    after_start: AwareDatetime
    after_id: UUID


class CalendarRangePage(BaseModel):
    """Current stored occurrences alongside their last successful coverage evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    records: tuple[SourceRecord, ...]
    next_cursor: str | None
    has_more: bool
    coverage: CalendarWindowCoverage
    consistency: Literal["observed"] = "observed"
    refresh_state: Literal["last_completed", "refresh_in_progress", "refresh_incomplete"]
    observed_change_sequence: int
    scanned_records: int


def occurrence_interval(payload: dict, calendar_timezone: str) -> tuple[datetime, datetime]:
    """All-day dates remain native; only comparison uses their calendar timezone."""
    start, end = payload.get("start", {}), payload.get("end", {})
    zone = ZoneInfo(calendar_timezone)
    if start.get("date") and end.get("date"):
        from datetime import date

        return (
            datetime.combine(date.fromisoformat(start["date"]), time(), zone),
            datetime.combine(date.fromisoformat(end["date"]), time(), zone),
        )
    values = []
    for part in (start, end):
        value = datetime.fromisoformat(part["dateTime"].replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo(part.get("timeZone") or calendar_timezone))
        values.append(value)
    return values[0], values[1]


class CalendarRangeService:
    """Read current owned rows under explicit input bounds and optimistic consistency."""

    def __init__(self, signing_key: str):
        """Use the deployment cursor signing key."""
        self.signing_key = signing_key

    async def read(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        calendar_id: str,
        query: CalendarRange,
    ) -> CalendarRangePage:
        """Return a bounded observed range, failing on changed continuation state."""
        sync = await db.scalar(
            select(Sync).where(Sync.id == sync_id, Sync.organization_id == organization_id)
        )
        if sync is None:
            raise SourceNotFound("Source does not exist in this organization")
        calendar = await db.scalar(
            select(Entity).where(
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Entity.entity_definition_short_name == "calendar",
                Entity.native_id == calendar_id,
                Entity.deleted_at.is_(None),
                content_is_available(),
            )
        )
        if calendar is None:
            raise RecordNotFound("Calendar is not currently available")
        stored_cursor = await db.scalar(
            select(SyncCursor).where(
                SyncCursor.sync_id == sync_id, SyncCursor.organization_id == organization_id
            )
        )
        state = GoogleCalendarCursor.model_validate(
            stored_cursor.cursor_data if stored_cursor else {}
        )
        coverage = state.occurrence_coverage.get(calendar_id)
        stamp_data = (
            stored_cursor.cursor_data.get("canonical_checkpoint") if stored_cursor else None
        )
        if (
            coverage is None
            or stamp_data is None
            or query.start < coverage.start
            or query.end > coverage.end
        ):
            raise CalendarRangeNotCaptured(query.start, query.end)
        stamp = CanonicalCheckpoint.model_validate(stamp_data)
        if calendar.source_payload.get("timeZone") != coverage.timezone:
            raise CalendarChanged("Calendar timezone changed; refresh captured window")
        sequence = sync.observed_change_sequence
        position = self._position(query, organization_id, sync_id, calendar_id, coverage, sequence)
        rows = await self._bounded_rows(db, organization_id, sync_id, calendar_id)
        matches = self._matching_rows(rows, coverage, query, position)
        # Recheck both journal and checkpoint: no snapshot or mixed-page claim.
        db.expire_all()
        latest = await db.scalar(
            select(Sync).where(Sync.id == sync_id, Sync.organization_id == organization_id)
        )
        latest_cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == sync_id))
        latest_scan = (
            latest_cursor.cursor_data.get("occurrence_coverage", {})
            .get(calendar_id, {})
            .get("scan_id")
            if latest_cursor
            else None
        )
        if (
            latest is None
            or latest.observed_change_sequence != sequence
            or latest_scan != str(coverage.scan_id)
        ):
            raise CalendarChanged("Calendar changed while reading; restart range query")
        refresh_state = "last_completed"
        if (
            latest.writer_attempt_id != stamp.writer_attempt_id
            or sequence != stamp.observed_change_sequence
        ):
            job = await db.get(SyncJob, latest.writer_job_id) if latest.writer_job_id else None
            refresh_state = (
                "refresh_in_progress"
                if job
                and str(getattr(job.status, "value", job.status)).lower() in {"running", "pending"}
                else "refresh_incomplete"
            )
        more = len(matches) > query.limit
        page = matches[: query.limit]
        next_cursor = None
        if more:
            next_cursor = jwt.encode(
                CalendarRangeCursor(
                    organization_id=organization_id,
                    sync_id=sync_id,
                    calendar_id=calendar_id,
                    start=query.start,
                    end=query.end,
                    timezone=query.timezone,
                    sequence=sequence,
                    coverage_scan_id=coverage.scan_id,
                    after_start=page[-1][0],
                    after_id=page[-1][1],
                ).model_dump(mode="json"),
                self.signing_key,
                algorithm="HS256",
            )
        return CalendarRangePage(
            records=tuple(item[2] for item in page),
            next_cursor=next_cursor,
            has_more=more,
            coverage=coverage,
            refresh_state=refresh_state,
            observed_change_sequence=sequence,
            scanned_records=len(rows),
        )

    @staticmethod
    async def _bounded_rows(
        db: AsyncSession, organization_id: UUID, sync_id: UUID, calendar_id: str
    ) -> list[Entity]:
        """Gate payload transfer and count in one PostgreSQL statement snapshot."""
        filters = (
            Entity.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Entity.record_revision > 0,
            Entity.entity_definition_short_name == "event_occurrence",
            Entity.container_id == calendar_id,
            Entity.deleted_at.is_(None),
            content_is_available(),
        )
        budget = (
            select(
                func.count(Entity.id).label("records"),
                func.coalesce(
                    func.sum(func.octet_length(cast(Entity.source_payload, String))), 0
                ).label("bytes"),
            )
            .where(*filters)
            .cte("occurrence_budget")
        )
        # Outer join always returns the budget, even for an empty or rejected scope.
        # An oversized single JSON value never travels to the application.
        result = await db.execute(
            select(budget.c.records, budget.c.bytes, Entity)
            .select_from(budget)
            .outerjoin(
                Entity,
                and_(*filters, budget.c.records <= 10000, budget.c.bytes <= 20 * 1024 * 1024),
            )
            .order_by(Entity.id)
        )
        data = result.all()
        if data[0][0] > 10000 or data[0][1] > 20 * 1024 * 1024:
            raise CalendarRangeError(
                "Stored occurrence input exceeds budget; narrow capture horizon"
            )
        return [row[2] for row in data if row[2] is not None]

    @staticmethod
    def _matching_rows(
        rows: list[Entity],
        coverage: CalendarWindowCoverage,
        query: CalendarRange,
        position: CalendarRangeCursor | None,
    ) -> list[tuple[datetime, UUID, SourceRecord]]:
        """Apply schedule semantics only after bounding the calendar's stored inputs."""
        bytes_read = 0
        matches = []
        for row in rows:
            bytes_read += len(json.dumps(row.source_payload).encode())
            if bytes_read > 20 * 1024 * 1024:
                raise CalendarRangeError("Stored occurrence input exceeds byte budget")
            if row.source_payload.get("status") == "cancelled":
                continue
            try:
                start, end = occurrence_interval(row.source_payload, coverage.timezone)
                if end <= start:
                    raise ValueError("Invalid interval")
            except (KeyError, ValueError, TypeError) as exc:
                raise CalendarRangeError(
                    "Captured occurrence has an invalid schedule; refresh source"
                ) from exc
            start = start.astimezone(timezone.utc)
            if end > query.start and start < query.end:
                if position is None or (start, row.id) > (position.after_start, position.after_id):
                    matches.append((start, row.id, source_record(row)))
        matches.sort(key=lambda item: (item[0], item[1]))
        return matches

    def _position(
        self,
        query: CalendarRange,
        organization_id: UUID,
        sync_id: UUID,
        calendar_id: str,
        coverage: CalendarWindowCoverage,
        sequence: int,
    ) -> CalendarRangeCursor | None:
        if query.cursor is None:
            return None
        try:
            position = CalendarRangeCursor.model_validate(
                jwt.decode(query.cursor, self.signing_key, algorithms=["HS256"])
            )
        except (JWTError, ValidationError, ValueError) as exc:
            raise InvalidRecordCursor("Invalid Calendar cursor; restart range query") from exc
        if position.purpose != "canonical_calendar_range_v1" or (
            position.organization_id,
            position.sync_id,
            position.calendar_id,
            position.start,
            position.end,
            position.timezone,
        ) != (organization_id, sync_id, calendar_id, query.start, query.end, query.timezone):
            raise InvalidRecordCursor("Calendar cursor belongs to another scope or range")
        if position.sequence != sequence or position.coverage_scan_id != coverage.scan_id:
            raise CalendarChanged("Calendar changed since prior page; restart range query")
        return position
