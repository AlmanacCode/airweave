"""SQL-first retained Gmail traversal with capture and prepared-text fences."""

from uuid import UUID

from jose import JWTError, jwt
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError
from sqlalchemy import and_, case, false, func, or_, select, true, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.mail_body import current_mail_body
from airweave.domains.entities.canonical.mail_facts_v1 import GmailMetadata
from airweave.domains.entities.canonical.mail_models import (
    MailFilters,
    MailIndexing,
    MailMatch,
    MailMessageCursor,
    MailMessagePage,
    MailMessageQuery,
)
from airweave.domains.entities.canonical.query import InvalidRecordCursor
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    SourceNotFound,
    content_is_available,
)
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class _MailRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    record_revision: int
    native_id: str
    gmail_metadata: GmailMetadata
    source_created_at: AwareDatetime
    observed_at: AwareDatetime


class MailChanged(CanonicalStoreError):
    """Capture or text preparation changed during traversal; restart with the same filters."""

    code = "mail_changed_restart"


def _metadata_scope(organization: UUID, sync: UUID):
    return and_(
        Entity.organization_id == organization,
        Entity.sync_id == sync,
        Entity.entity_definition_short_name == "message",
        Entity.record_revision > 0,
        Entity.deleted_at.is_(None),
        content_is_available(),
        source_is_readable(organization, sync),
    )


def _metadata_ready():
    return and_(
        Entity.gmail_metadata.is_not(None),
        Entity.gmail_metadata_revision == Entity.record_revision,
        Entity.source_created_at.is_not(None),
        func.coalesce(
            func.jsonb_typeof(Entity.gmail_metadata["participants_folded"]) == "array", false()
        ),
    )


def _filters(filters: MailFilters):
    conditions = []
    for field, values in (("sender", filters.from_addresses), ("to", filters.to_addresses)):
        if values:
            conditions.append(
                or_(
                    *(
                        Entity.gmail_metadata.contains({field: [{"address": value}]})
                        for value in values
                    )
                )
            )
    if filters.after is not None:
        conditions.append(Entity.source_created_at >= filters.after)
    if filters.before is not None:
        conditions.append(Entity.source_created_at < filters.before)
    if filters.folder is not None:
        label = {
            "inbox": "INBOX",
            "sent": "SENT",
            "trash": "TRASH",
            "spam": "SPAM",
            "drafts": "DRAFT",
        }[filters.folder]
        conditions.append(Entity.gmail_metadata.contains({"labels": [label]}))
    if filters.unread is not None:
        unread = Entity.gmail_metadata.contains({"labels": ["UNREAD"]})
        conditions.append(unread if filters.unread else ~unread)
    return conditions


class CanonicalMailQuery:
    """One source page; bodies are filtered inside SQL and never returned or fetched."""

    def __init__(self, signing_key: str):
        """Reuse the canonical record cursor signing key."""
        self.signing_key = signing_key

    async def _scope(
        self, db: AsyncSession, organization: UUID, sync: UUID
    ) -> tuple[int, int, int]:
        row = (
            await db.execute(
                select(
                    Sync.observed_change_sequence,
                    Sync.mail_text_sequence,
                    Sync.index_pipeline_version,
                ).where(
                    Sync.id == sync,
                    Sync.organization_id == organization,
                    source_is_readable(organization, sync),
                    select(SourceConnection.id)
                    .where(
                        SourceConnection.organization_id == organization,
                        SourceConnection.sync_id == sync,
                        SourceConnection.short_name == "gmail",
                    )
                    .exists(),
                )
            )
        ).one_or_none()
        if row is None:
            raise SourceNotFound("Gmail source is unavailable in this organization")
        return tuple(row)

    def _cursor(
        self, token: str, organization: UUID, sync: UUID, filters: MailFilters
    ) -> MailMessageCursor:
        try:
            cursor = MailMessageCursor.model_validate(
                jwt.decode(token, self.signing_key, algorithms=["HS256"])
            )
        except (JWTError, ValidationError, ValueError) as error:
            raise InvalidRecordCursor("Invalid mail cursor; restart this traversal") from error
        if (
            cursor.organization_id != organization
            or cursor.sync_id != sync
            or cursor.filters != filters
        ):
            raise InvalidRecordCursor("Preserve the original mail account and filters")
        return cursor

    async def messages(
        self, db: AsyncSession, organization: UUID, sync: UUID, query: MailMessageQuery
    ) -> MailMessagePage:
        """Keyset pagination over native internalDate; completeness remains retained-only."""
        sequence, text_sequence, pipeline = await self._scope(db, organization, sync)
        cursor = (
            self._cursor(query.cursor, organization, sync, query.filters)
            if query.cursor is not None
            else None
        )
        if cursor is not None and (
            cursor.sequence != sequence
            or cursor.pipeline_version != pipeline
            or (query.filters.query and cursor.text_sequence != text_sequence)
        ):
            raise MailChanged("Mail changed since the previous page; restart traversal")
        body = current_mail_body().lateral("prepared_mail_body")
        scope = _metadata_scope(organization, sync)
        facets = _filters(query.filters)
        candidate = (
            select(
                Entity.id,
                Entity.sync_id,
                Entity.record_revision,
                Entity.native_id,
                Entity.gmail_metadata,
                Entity.source_created_at,
                Entity.observed_at,
            )
            .join(Sync, Sync.id == Entity.sync_id)
            .outerjoin(body, true())
        )
        candidate = candidate.where(scope, _metadata_ready(), *facets)
        if query.filters.query:
            participants = func.jsonb_array_elements_text(
                Entity.gmail_metadata["participants_folded"]
            ).table_valued("value")
            candidate = candidate.where(
                or_(
                    func.strpos(Entity.gmail_metadata["subject_folded"].astext, query.filters.query)
                    > 0,
                    select(1)
                    .select_from(participants)
                    .where(func.strpos(participants.c.value, query.filters.query) > 0)
                    .correlate(Entity)
                    .exists(),
                    func.strpos(body.c.mail_body_text, query.filters.query) > 0,
                )
            )
        if cursor is not None:
            candidate = candidate.where(
                tuple_(Entity.source_created_at, Entity.id)
                > tuple_(cursor.after_created_at, cursor.after_id)
            )
        rows = tuple(
            _MailRow.model_validate(row)
            for row in (
                await db.execute(
                    candidate.order_by(Entity.source_created_at, Entity.id).limit(query.limit + 1)
                )
            ).mappings()
        )
        counts = (
            await db.execute(
                select(
                    func.count(case((body.c.mail_body_status == "complete", 1))),
                    func.count(case((body.c.mail_body_status == "partial", 1))),
                    func.count(case((body.c.id.is_(None), 1))),
                )
                .select_from(Entity)
                .join(Sync, Sync.id == Entity.sync_id)
                .outerjoin(body, true())
                .where(scope, _metadata_ready(), *facets)
            )
        ).one()
        # Missing metadata has unknown filter membership, so disclose source-wide gaps.
        missing = await db.scalar(
            select(func.count()).select_from(Entity).where(scope, ~_metadata_ready())
        )
        capture = (await capture_coverage(db, organization, (sync,))).get(sync)
        latest = await self._scope(db, organization, sync)
        if (
            latest[0] != sequence
            or latest[2] != pipeline
            or (query.filters.query and latest[1] != text_sequence)
        ):
            raise MailChanged("Mail changed while reading; restart traversal")
        messages = []
        for row in rows[: query.limit]:
            facts = row.gmail_metadata
            if facts.native_id != row.native_id or facts.sent_at != row.source_created_at:
                raise MailChanged("Retained mail metadata requires rederivation")
            messages.append(
                MailMatch(
                    id=row.id,
                    sync_id=row.sync_id,
                    revision=row.record_revision,
                    native_id=facts.native_id,
                    thread_id=facts.thread_id,
                    subject=facts.subject,
                    sender=facts.sender,
                    to=facts.to,
                    sent_at=facts.sent_at,
                    labels=facts.labels,
                    snippet=facts.snippet,
                    observed_at=row.observed_at,
                )
            )
        more = len(rows) > query.limit
        next_cursor = None
        if more:
            last = rows[query.limit - 1]
            position = MailMessageCursor(
                organization_id=organization,
                sync_id=sync,
                filters=query.filters,
                sequence=sequence,
                text_sequence=text_sequence if query.filters.query else None,
                pipeline_version=pipeline,
                after_created_at=last.source_created_at,
                after_id=last.id,
            )
            next_cursor = jwt.encode(
                position.model_dump(mode="json"), self.signing_key, algorithm="HS256"
            )
        return MailMessagePage(
            messages=tuple(messages),
            next_cursor=next_cursor,
            has_more=more,
            capture=capture,
            indexing=MailIndexing(
                metadata_missing=missing or 0,
                text_ready=counts[0],
                text_partial=counts[1],
                text_unavailable=counts[2],
            ),
        )
