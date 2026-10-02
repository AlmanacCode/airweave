"""SQL-first retained meeting traversal; no native body or provider reads."""

from uuid import UUID

from jose import JWTError, jwt
from pydantic import ValidationError
from sqlalchemy import Boolean, and_, case, cast, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.query import InvalidRecordCursor
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    SourceNotFound,
    content_is_available,
)
from airweave.domains.entities.canonical.wispr_models import (
    MeetingCursor,
    MeetingListQuery,
    MeetingPage,
    MeetingPreview,
)
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class MeetingsChanged(CanonicalStoreError):
    """Native capture changed during traversal; restart with the same filters."""

    code = "meetings_changed_restart"


class CanonicalWisprQuery:
    """Newest-first inventory over validated native starts on authorized body records."""

    def __init__(self, signing_key: str):
        """Reuse the existing canonical signing key."""
        self.signing_key = signing_key

    async def _sequence(self, db: AsyncSession, organization: UUID, sync: UUID) -> int:
        sequence = await db.scalar(
            select(Sync.observed_change_sequence).where(
                Sync.id == sync,
                Sync.organization_id == organization,
                source_is_readable(organization, sync),
                select(SourceConnection.id)
                .where(
                    SourceConnection.organization_id == organization,
                    SourceConnection.sync_id == sync,
                    SourceConnection.short_name == "wispr",
                )
                .exists(),
            )
        )
        if sequence is None:
            raise SourceNotFound("Wispr source is unavailable in this organization")
        return sequence

    async def meetings(
        self, db: AsyncSession, organization: UUID, sync: UUID, query: MeetingListQuery
    ) -> MeetingPage:
        """Filter/order before LIMIT; missing facts never establish range membership."""
        sequence = await self._sequence(db, organization, sync)
        cursor = None
        if query.cursor is not None:
            try:
                cursor = MeetingCursor.model_validate(
                    jwt.decode(query.cursor, self.signing_key, algorithms=["HS256"])
                )
            except (JWTError, ValidationError, ValueError) as error:
                raise InvalidRecordCursor("Invalid meeting cursor; restart traversal") from error
            if (
                cursor.organization_id != organization
                or cursor.sync_id != sync
                or cursor.filters != query.filters
            ):
                raise InvalidRecordCursor("Preserve the original meeting account and filters")
            if cursor.sequence != sequence:
                raise MeetingsChanged("Meetings changed since the previous page; restart traversal")
        scope = and_(
            Entity.organization_id == organization,
            Entity.sync_id == sync,
            Entity.entity_definition_short_name == "meeting",
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
            source_is_readable(organization, sync),
        )
        first = Entity.source_payload["responses"][0]["response"]
        transcript = first["has_transcript"]
        candidate = select(
            Entity.id,
            Entity.sync_id,
            Entity.record_revision.label("revision"),
            Entity.native_id,
            func.coalesce(
                first["title"].astext, Entity.source_payload["listing"]["title"].astext, "Meeting"
            ).label("title"),
            Entity.meeting_started_at.label("started_at"),
            Entity.source_updated_at.label("modified_at"),
            case(
                (func.jsonb_typeof(transcript) == "boolean", cast(transcript.astext, Boolean)),
                else_=None,
            ).label("has_transcript"),
            Entity.observed_at,
        ).where(scope, Entity.meeting_started_at.is_not(None))
        if query.filters.after is not None:
            candidate = candidate.where(Entity.meeting_started_at >= query.filters.after)
        if query.filters.before is not None:
            candidate = candidate.where(Entity.meeting_started_at < query.filters.before)
        if cursor is not None:
            candidate = candidate.where(
                tuple_(Entity.meeting_started_at, Entity.id)
                < tuple_(cursor.after_started_at, cursor.after_id)
            )
        rows = tuple(
            (
                await db.execute(
                    candidate.order_by(Entity.meeting_started_at.desc(), Entity.id.desc()).limit(
                        query.limit + 1
                    )
                )
            ).mappings()
        )
        missing = await db.scalar(
            select(func.count())
            .select_from(Entity)
            .where(scope, Entity.meeting_started_at.is_(None))
        )
        capture = (await capture_coverage(db, organization, (sync,))).get(sync)
        if await self._sequence(db, organization, sync) != sequence:
            raise MeetingsChanged("Meetings changed during this page; restart traversal")
        meetings = tuple(MeetingPreview.model_validate(row) for row in rows[: query.limit])
        more = len(rows) > query.limit
        token = None
        if more:
            last = meetings[-1]
            token = jwt.encode(
                MeetingCursor(
                    organization_id=organization,
                    sync_id=sync,
                    filters=query.filters,
                    sequence=sequence,
                    after_started_at=last.started_at,
                    after_id=last.id,
                ).model_dump(mode="json"),
                self.signing_key,
                algorithm="HS256",
            )
        return MeetingPage(
            meetings=meetings,
            next_cursor=token,
            has_more=more,
            capture=capture,
            metadata_missing=missing or 0,
        )
