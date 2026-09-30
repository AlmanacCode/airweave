"""Exact reads and continuation contracts over the owned record store."""

from uuid import UUID

from jose import JWTError, jwt
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query_models import (
    RecordChangePage,
    RecordCursor,
    RecordListQuery,
    RecordPage,
)
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError


class InvalidRecordCursor(CanonicalStoreError):
    """A cursor belongs to another scope/query or cannot be verified."""

    code = "invalid_cursor"


class RecordNotFound(CanonicalStoreError):
    """No canonical record is visible at this identity."""

    code = "record_not_found"


class CanonicalQueryService:
    """Own read semantics; cursor signatures never replace tenant authorization."""

    def __init__(
        self, records: CanonicalRecordStore, queries: CanonicalQueryStore, signing_key: str
    ) -> None:
        """Bind persistence and the existing deployment secret used for signed cursors."""
        self.records = records
        self.queries = queries
        self.signing_key = signing_key

    def _encode(self, cursor: RecordCursor) -> str:
        return jwt.encode(cursor.model_dump(mode="json"), self.signing_key, algorithm="HS256")

    def _decode(self, token: str, organization_id: UUID, sync_id: UUID, mode: str) -> RecordCursor:
        try:
            cursor = RecordCursor.model_validate(
                jwt.decode(token, self.signing_key, algorithms=["HS256"])
            )
        except (JWTError, ValidationError, ValueError) as exc:
            raise InvalidRecordCursor("Invalid continuation cursor; restart this query") from exc
        if (
            cursor.organization_id != organization_id
            or cursor.sync_id != sync_id
            or cursor.mode != mode
        ):
            raise InvalidRecordCursor("Cursor belongs to a different source or operation")
        return cursor

    async def read(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, record_id: UUID
    ) -> SourceRecord:
        """Return exact current state, including an explicit tombstone if deleted."""
        record = await self.records.read(db, organization_id, sync_id, record_id)
        if record is None:
            raise RecordNotFound("Record not found in this source")
        return record

    async def list_records(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, query: RecordListQuery
    ) -> RecordPage:
        """Traverse current committed rows; callers must preserve filters on continuation."""
        after_id = None
        if query.cursor is not None:
            cursor = self._decode(query.cursor, organization_id, sync_id, "list")
            if cursor.filters != query.filters:
                raise InvalidRecordCursor("Preserve the original filters when continuing a list")
            after_id = cursor.after_id
        rows = await self.queries.list_records(
            db, organization_id, sync_id, query.filters, after_id=after_id, limit=query.limit
        )
        more = len(rows) > query.limit
        page = rows[: query.limit]
        next_cursor = None
        if more:
            next_cursor = self._encode(
                RecordCursor(
                    mode="list",
                    organization_id=organization_id,
                    sync_id=sync_id,
                    filters=query.filters,
                    after_id=page[-1].id,
                )
            )
        return RecordPage(records=page, next_cursor=next_cursor, has_more=more)

    async def changes(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> RecordChangePage:
        """Start at the beginning; finish each committed window before polling the next."""
        position = (
            self._decode(cursor, organization_id, sync_id, "changes")
            if cursor
            else RecordCursor(mode="changes", organization_id=organization_id, sync_id=sync_id)
        )
        page = await self.records.changes(
            db,
            organization_id,
            sync_id,
            after=position.after_sequence,
            limit=limit,
            high_watermark=position.high_watermark,
        )
        next_cursor = self._encode(
            RecordCursor(
                mode="changes",
                organization_id=organization_id,
                sync_id=sync_id,
                after_sequence=page.next_sequence,
                high_watermark=page.high_watermark if page.has_more else None,
            )
        )
        return RecordChangePage(
            changes=page.changes,
            next_cursor=next_cursor,
            has_more=page.has_more,
            high_watermark=page.high_watermark,
        )
