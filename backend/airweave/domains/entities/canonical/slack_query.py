"""SQL-first observed Slack thread read with existing source/ancestor authority."""

from uuid import UUID

from jose import JWTError, jwt
from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError
from sqlalchemy import Numeric, and_, case, cast, func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.query import InvalidRecordCursor
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.slack_models import (
    SlackFileReference,
    SlackThreadCursor,
    SlackThreadMessage,
    SlackThreadPage,
    SlackThreadQuery,
)
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    SourceNotFound,
    content_is_available,
)
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class SlackThreadChanged(CanonicalStoreError):
    """Retained capture changed; restart rather than combine different versions."""

    code = "slack_thread_changed_restart"


class SlackThreadUnavailable(CanonicalStoreError):
    """A selected original cannot safely form the native read contract."""

    code = "slack_thread_unavailable"


class _NativeMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    text: str | None = None
    user: str | None = None
    bot_id: str | None = None
    subtype: str | None = None
    blocks: tuple[dict[str, JsonValue], ...] = ()
    attachments: tuple[dict[str, JsonValue], ...] = ()
    files: tuple[dict[str, JsonValue], ...] = ()


def _valid_timestamp(value):
    return and_(
        func.jsonb_typeof(value) == "string",
        func.length(value.astext) <= 64,
        value.astext.op("~")(r"^[0-9]+\.[0-9]+$"),
    )


class CanonicalSlackQuery:
    """One bounded observed thread, without provider, index, or blob calls."""

    def __init__(self, signing_key: str):
        """Use the same service-owned signing authority as canonical record reads."""
        self.signing_key = signing_key

    async def _sequence(self, db: AsyncSession, org: UUID, sync: UUID, channel: str) -> int:
        channel_visible = (
            select(Entity.id)
            .where(
                Entity.organization_id == org,
                Entity.sync_id == sync,
                Entity.entity_definition_short_name == "channel",
                Entity.native_id == channel,
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                content_is_available(),
            )
            .exists()
        )
        sequence = await db.scalar(
            select(Sync.observed_change_sequence).where(
                Sync.id == sync,
                Sync.organization_id == org,
                source_is_readable(org, sync),
                channel_visible,
                select(SourceConnection.id)
                .where(
                    SourceConnection.organization_id == org,
                    SourceConnection.sync_id == sync,
                    SourceConnection.short_name == "slack",
                )
                .exists(),
            )
        )
        if sequence is None:
            raise SourceNotFound("The retained Slack source or channel is unavailable")
        return sequence

    def _cursor(
        self, org: UUID, sync: UUID, query: SlackThreadQuery, sequence: int
    ) -> SlackThreadCursor | None:
        if query.cursor is None:
            return None
        try:
            cursor = SlackThreadCursor.model_validate(
                jwt.decode(query.cursor, self.signing_key, algorithms=["HS256"])
            )
        except (JWTError, ValidationError, ValueError) as error:
            raise InvalidRecordCursor("Invalid Slack cursor; restart this thread read") from error
        if (
            cursor.organization_id,
            cursor.sync_id,
            cursor.channel,
            cursor.thread_ts,
            cursor.limit,
        ) != (org, sync, query.channel, query.thread_ts, query.limit):
            raise InvalidRecordCursor("Preserve the original Slack account, thread and limit")
        if cursor.sequence != sequence:
            raise SlackThreadChanged("Stored Slack messages changed; restart this thread read")
        return cursor

    async def thread(
        self, db: AsyncSession, org: UUID, sync: UUID, query: SlackThreadQuery
    ) -> SlackThreadPage:
        """Read one sequence-fenced page of authorized observed native messages."""
        sequence = await self._sequence(db, org, sync, query.channel)
        cursor = self._cursor(org, sync, query, sequence)
        scope = and_(
            Entity.organization_id == org,
            Entity.sync_id == sync,
            Entity.entity_definition_short_name == "message",
            Entity.container_id == query.channel,
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
            source_is_readable(org, sync),
        )
        ts, thread_ts = Entity.source_payload["ts"], Entity.source_payload["thread_ts"]
        ready = and_(
            _valid_timestamp(ts),
            ts.astext == Entity.native_id,
            or_(thread_ts.astext.is_(None), _valid_timestamp(thread_ts)),
        )
        # CASE protects the CAST independently of planner predicate ordering.
        numeric_ts = case((ready, cast(ts.astext, Numeric())), else_=None)
        thread = func.coalesce(thread_ts.astext, ts.astext)
        statement = select(Entity).where(scope, ready, thread == query.thread_ts)
        if cursor is not None:
            statement = statement.where(
                tuple_(numeric_ts, Entity.id)
                > tuple_(cast(cursor.after_ts, Numeric()), cursor.after_id)
            )
        rows = tuple(
            await db.scalars(statement.order_by(numeric_ts, Entity.id).limit(query.limit + 1))
        )
        page_rows = rows[: query.limit]
        child_refs: dict[str, list[SlackFileReference]] = {}
        if page_rows:
            children = (
                await db.execute(
                    select(
                        Entity.id, Entity.record_revision, Entity.native_id, Entity.parent_native_id
                    )
                    .where(
                        Entity.organization_id == org,
                        Entity.sync_id == sync,
                        Entity.entity_definition_short_name == "file",
                        Entity.parent_record_type == "message",
                        Entity.parent_native_id.in_([row.native_id for row in page_rows]),
                        Entity.parent_container_id == query.channel,
                        Entity.record_revision > 0,
                        Entity.deleted_at.is_(None),
                        content_is_available(),
                        source_is_readable(org, sync),
                    )
                    .order_by(Entity.id)
                )
            ).all()
            for child in children:
                child_refs.setdefault(child.parent_native_id, []).append(
                    SlackFileReference(
                        id=child.id, revision=child.record_revision, native_id=child.native_id
                    )
                )
        root_present, missing = (
            await db.execute(
                select(
                    func.count(
                        case(
                            (
                                and_(
                                    ready, ts.astext == query.thread_ts, thread == query.thread_ts
                                ),
                                1,
                            )
                        )
                    ),
                    func.count(case((~func.coalesce(ready, False), 1))),
                ).where(scope)
            )
        ).one()
        capture = (await capture_coverage(db, org, (sync,))).get(sync)
        if await self._sequence(db, org, sync, query.channel) != sequence:
            raise SlackThreadChanged("Stored Slack messages changed during this read; restart")
        messages = []
        for row in page_rows:
            try:
                native = _NativeMessage.model_validate(row.source_payload)
                messages.append(
                    SlackThreadMessage(
                        id=row.id,
                        revision=row.record_revision,
                        ts=row.source_payload["ts"],
                        thread_ts=row.source_payload.get("thread_ts"),
                        observed_at=row.observed_at,
                        captured_files=child_refs.get(row.native_id, ()),
                        **native.model_dump(),
                    )
                )
            except ValidationError:
                raise SlackThreadUnavailable(
                    "Stored Slack message content is malformed; refresh capture"
                ) from None
        more = len(rows) > query.limit
        token = None
        if more:
            last = messages[-1]
            token = jwt.encode(
                SlackThreadCursor(
                    organization_id=org,
                    sync_id=sync,
                    channel=query.channel,
                    thread_ts=query.thread_ts,
                    limit=query.limit,
                    sequence=sequence,
                    after_ts=last.ts,
                    after_id=last.id,
                ).model_dump(mode="json"),
                self.signing_key,
                algorithm="HS256",
            )
        return SlackThreadPage(
            sync_id=sync,
            channel=query.channel,
            thread_ts=query.thread_ts,
            messages=messages,
            root_present=bool(root_present),
            next_cursor=token,
            has_more=more,
            capture=capture,
            metadata_missing=missing,
        )
