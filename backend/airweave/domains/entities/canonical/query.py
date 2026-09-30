"""Exact reads and continuation contracts over the owned record store."""

from uuid import UUID

from jose import JWTError, jwt
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.blob_materializer import BlobIntegrityError, read_blob
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query_models import (
    MailThreadCursor,
    MailThreadPage,
    RecordChangePage,
    RecordCursor,
    RecordListQuery,
    RecordPage,
)
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.storage.exceptions import StorageException
from airweave.domains.storage.protocols import StorageBackend


class InvalidRecordCursor(CanonicalStoreError):
    """A cursor belongs to another scope/query or cannot be verified."""

    code = "invalid_cursor"


class RecordNotFound(CanonicalStoreError):
    """No canonical record is visible at this identity."""

    code = "record_not_found"


class StaleRecordRevision(CanonicalStoreError):
    """The client must reread the current record before using its blob reference."""

    code = "stale_record_revision"


class BlobNotFound(CanonicalStoreError):
    """No authorized committed bytes match this record and digest."""

    code = "blob_not_found"


class BlobUnavailable(CanonicalStoreError):
    """Committed bytes are missing or fail integrity verification."""

    code = "blob_unavailable"


class CanonicalQueryService:
    """Own read semantics; cursor signatures never replace tenant authorization."""

    def __init__(
        self, records: CanonicalRecordStore, queries: CanonicalQueryStore, signing_key: str
    ) -> None:
        """Bind persistence and the existing deployment secret used for signed cursors."""
        self.records = records
        self.queries = queries
        self.signing_key = signing_key

    def _encode(self, cursor: RecordCursor | MailThreadCursor) -> str:
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

    async def mail_thread(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        thread_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> MailThreadPage:
        """Return stored messages only; observation time never proves complete coverage."""
        if not thread_id or len(thread_id) > 512 or not 1 <= limit <= 100:
            raise ValueError("Invalid thread query bounds")
        position = None
        if cursor is not None:
            try:
                position = MailThreadCursor.model_validate(
                    jwt.decode(cursor, self.signing_key, algorithms=["HS256"])
                )
            except (JWTError, ValidationError, ValueError) as exc:
                raise InvalidRecordCursor("Invalid thread cursor; restart this query") from exc
            if (position.organization_id, position.sync_id, position.thread_id) != (
                organization_id,
                sync_id,
                thread_id,
            ):
                raise InvalidRecordCursor("Cursor belongs to a different account or thread")
        rows = await self.queries.mail_thread(
            db,
            organization_id,
            sync_id,
            thread_id,
            after_id=position.after_id if position else None,
            after_created_at=position.after_created_at if position else None,
            limit=limit,
        )
        more = len(rows) > limit
        messages = rows[:limit]
        next_cursor = None
        if more:
            last = messages[-1]
            next_cursor = self._encode(
                MailThreadCursor(
                    organization_id=organization_id,
                    sync_id=sync_id,
                    thread_id=thread_id,
                    after_created_at=last.source_created_at,
                    after_id=last.id,
                )
            )
        return MailThreadPage(
            thread_id=thread_id, messages=messages, next_cursor=next_cursor, has_more=more
        )

    async def blob(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        record_id: UUID,
        revision: int,
        sha256: str,
        storage: StorageBackend,
    ) -> bytes:
        """Resolve only committed references and recheck visibility after storage I/O."""
        record = await self.read(db, organization_id, sync_id, record_id)
        self._check_blob_record(record, revision)
        ref = next((item for item in record.blobs if item.sha256 == sha256), None)
        if ref is None:
            raise BlobNotFound("Blob reference not found on this record")
        try:
            content = await read_blob(record, ref, storage)
        except (StorageException, BlobIntegrityError) as exc:
            raise BlobUnavailable("Committed blob is unavailable; retry later") from exc
        # Drop identity-map values before the second READ COMMITTED visibility check.
        db.expire_all()
        current = await self.read(db, organization_id, sync_id, record_id)
        self._check_blob_record(current, revision)
        if ref not in current.blobs:
            raise BlobNotFound("Blob reference no longer belongs to this record")
        return content

    @staticmethod
    def _check_blob_record(record: SourceRecord, revision: int) -> None:
        if record.content_access != "available" or record.deleted_at is not None:
            raise RecordNotFound("Record content is not currently available")
        if record.revision != revision:
            raise StaleRecordRevision("Record changed; reread before requesting its blob")
