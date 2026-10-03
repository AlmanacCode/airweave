"""Unipile v2 reads through the shared canonical frontier, never a private mirror."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import NoReturn

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter

from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import CapturePage
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    parent_container_key,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceError,
    SourceRateLimitError,
    SourceServerError,
)
from airweave.domains.sources.token_providers.protocol import AuthProviderKind
from airweave.domains.storage import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.configs.config import WhatsAppCaptureConfig
from airweave.platform.http_client.unipile_transport import UnipileError, UnipileWhatsAppClient
from airweave.platform.sources.records.whatsapp_collections import (
    WhatsAppCollectionPage,
    WhatsAppParticipantCollection,
    WhatsAppReactionCollection,
    reaction_page_digest,
)
from airweave.platform.sources.records.whatsapp_models import (
    WhatsAppChat,
    WhatsAppMessage,
    WhatsAppPage,
)


def _source_failure(error: UnipileError) -> NoReturn:
    """Translate native I/O evidence into the engine's existing error categories."""
    if error.kind == "authentication":
        raise SourceAuthError(
            str(error),
            source_short_name="whatsapp",
            status_code=error.status or 401,
            token_provider_kind=AuthProviderKind.CREDENTIAL,
        ) from error
    if error.kind == "rate_limit" and error.retry_after is not None:
        raise SourceRateLimitError(
            retry_after=error.retry_after,
            source_short_name="whatsapp",
            message=str(error),
        ) from error
    if error.kind in {"transient", "rate_limit"}:
        # Shared server recovery owns unknown timing; no provider delay is invented.
        raise SourceServerError(
            str(error),
            source_short_name="whatsapp",
            status_code=error.status,
        ) from error
    raise SourceError(str(error), source_short_name="whatsapp") from error


class WhatsAppProgress(BaseModel):
    """Bounded continuation committed atomically with records by the shared engine."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    cursor: str | None = Field(default=None, min_length=1, max_length=8192)
    offset: int = Field(default=0, ge=0)
    pages: int = Field(default=0, ge=0)
    previous_page_digest: str | None = None
    seen_cursor_hashes: tuple[str, ...] = Field(default=(), max_length=128)


class WhatsAppCapture:
    """Declared discovery only: traversal cannot establish full history or absence."""

    canonical_record_types = (
        "whatsapp_chat",
        "whatsapp_message",
        "whatsapp_chat_participants",
        "whatsapp_message_reactions",
    )
    canonical_container_parents = {
        "whatsapp_message": "whatsapp_chat",
        "whatsapp_chat_participants": "whatsapp_chat",
        "whatsapp_message_reactions": "whatsapp_message",
    }

    def __init__(self, client: UnipileWhatsAppClient, config: WhatsAppCaptureConfig):
        """Compose a bound reader; the canonical engine owns persistence and scheduling."""
        if client.account_id != config.account_id:
            raise UnipileError("identity")
        self.client = client
        self.config = config
        self.capture_cycle_configuration = CycleConfiguration.from_source(
            fingerprint=hashlib.sha256(
                json.dumps({"version": 1, **config.model_dump()}, sort_keys=True).encode()
            ).hexdigest(),
            record_types=self.canonical_record_types,
            container_parents=self.canonical_container_parents,
            completion_policies=dict.fromkeys(self.canonical_record_types, "discovery_only"),
        )

    async def validate(self) -> None:
        """Expose native authentication and transient evidence to the shared engine."""
        try:
            await self._validate()
        except UnipileError as error:
            _source_failure(error)

    async def acquire_chat_refresh(
        self, *, event_account_id: str, event_chat_id: str
    ) -> CaptureRecord:
        """Acquire an exact chat original; this never persists or admits an event.

        The caller must have run validate() once for this currently attested run
        and owns the current authorized binding, shared writer fencing, admission
        and ordering. Use only for nonconflicting creates/updates; never overwrite
        pending or committed deletion state. Native404 is not a tombstone.
        """
        try:
            if event_account_id != self.config.account_id or not event_chat_id:
                raise UnipileError("identity")
            chat = await self.client.chat(event_chat_id)
            if chat.id != event_chat_id or chat.is_channel:
                raise UnipileError("identity")
            return self._chat(chat, datetime.now(timezone.utc))
        except UnipileError as error:
            _source_failure(error)

    @staticmethod
    def _chat(chat: WhatsAppChat, observed_at: datetime) -> CaptureRecord:
        """Share exact native chat retention between listing and targeted acquisition."""
        return CaptureRecord(
            identity=RecordIdentity(record_type="whatsapp_chat", native_id=chat.id),
            payload_schema_version=1,
            payload=chat.original(),
            completeness="partial",
            observed_at=observed_at,
        )

    async def acquire_message_refresh(
        self,
        *,
        event_account_id: str,
        event_chat_id: str,
        event_message_id: str,
        parent: SourceRecord,
        files: FileService,
    ) -> CaptureRecord:
        """Acquire an exact native original; this never persists or admits an event.

        The caller must have run validate() for this currently attested run, supplies
        the current authorized chat/binding, and owns shared writer fencing, admission
        and ordering. Use only for nonconflicting creates/updates;
        this result is NOT safe to overwrite pending or committed deletion state.
        Sparse event content is deliberately not an input. Native404 is not a tombstone.
        """
        try:
            if (
                event_account_id != self.config.account_id
                or not event_chat_id
                or not event_message_id
                or parent.identity.record_type != "whatsapp_chat"
                or parent.identity.native_id != event_chat_id
                or parent.identity.container_id is not None
                or parent.parent is not None
                or parent.payload_schema_version != 1
                or parent.deleted_at is not None
                or parent.content_access != "available"
            ):
                raise UnipileError("identity")
            chat = WhatsAppChat.model_validate(parent.payload)
            if chat.id != event_chat_id or chat.is_channel:
                raise UnipileError("identity")
            message = await self.client.message(event_chat_id, event_message_id)
            if message.id != event_message_id or message.chat_id != event_chat_id:
                raise UnipileError("identity")
            return await self._message(message, parent.identity, files, datetime.now(timezone.utc))
        except UnipileError as error:
            _source_failure(error)

    async def _validate(self) -> None:
        """Re-attest exact native principal; successful login is not history completeness."""
        account = await self.client.account()
        if account.id != self.config.account_id or account.user_id != self.config.account_user_id:
            raise UnipileError("identity")
        if account.is_locked:
            raise UnipileError("permission")
        if account.status == "disconnected":
            raise UnipileError("authentication")
        if account.status != "running":
            raise UnipileError("transient")
        user = await self.client.owner_profile(account)
        if user.id != self.config.native_user_id:
            raise UnipileError("identity")

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Messages belong to the exact retained native chat, not its name."""
        if (
            parent.identity.record_type == "whatsapp_message"
            and record_type == "whatsapp_message_reactions"
        ):
            return CompletedScope(
                record_type=record_type,
                container_id=parent_container_key(parent.identity),
                parent=parent.identity,
            )
        if parent.identity.record_type != "whatsapp_chat" or record_type not in {
            "whatsapp_message",
            "whatsapp_chat_participants",
        }:
            raise ValueError("Invalid WhatsApp parent scope")
        return CompletedScope(
            record_type=record_type,
            container_id=parent_container_key(parent.identity)
            if record_type == "whatsapp_chat_participants"
            else parent.identity.native_id,
            parent=parent.identity,
        )

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Listing omission is neither deletion nor proof of account access loss."""
        raise ValueError("WhatsApp listing absence is not authoritative removal evidence")

    def _advance(
        self,
        page: WhatsAppPage[WhatsAppChat] | WhatsAppPage[WhatsAppMessage],
        previous: WhatsAppProgress,
    ) -> tuple[ScanContinuation, bool]:
        pages = previous.pages + 1
        if pages > self.config.max_pages_per_scope:
            raise ValueError("WhatsApp max_pages_per_scope exceeded; increase the declared budget")
        digest = hashlib.sha256(
            json.dumps(sorted(item.id for item in page.data)).encode()
        ).hexdigest()
        if page.data and digest == previous.previous_page_digest:
            raise ValueError("WhatsApp repeated the previous page without progress")
        if self.config.pagination == "cursor":
            cursor = page.cursor_after(previous.cursor)
            if cursor is None:
                return ScanContinuation(), True
            cursor_digest = hashlib.sha256(cursor.encode()).hexdigest()
            if cursor_digest in previous.seen_cursor_hashes:
                raise ValueError("WhatsApp cursor repeated without progress")
            progress = WhatsAppProgress(
                cursor=cursor,
                pages=pages,
                previous_page_digest=digest,
                seen_cursor_hashes=(*previous.seen_cursor_hashes, cursor_digest)[-128:],
            )
        else:
            offset = page.offset_after(previous.offset, self.config.page_size)
            if offset is None:
                return ScanContinuation(), True
            progress = WhatsAppProgress(offset=offset, pages=pages, previous_page_digest=digest)
        return ScanContinuation(value=progress.model_dump(mode="json")), False

    def _check_page_size(
        self, page: WhatsAppPage[WhatsAppChat] | WhatsAppPage[WhatsAppMessage]
    ) -> None:
        if len(page.data) > self.config.page_size:
            raise ValueError("WhatsApp response exceeded the declared page_size")

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Keep native failures inside shared source classification and recovery."""
        try:
            return await self._capture_page(scope, continuation, files=files, parent=parent)
        except UnipileError as error:
            _source_failure(error)

    async def _capture_page(  # noqa: C901 -- explicit provider scope dispatch
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Fetch one bounded page. No SQL, durable cursor mutation or inferred tombstones."""
        if scope.record_type == "whatsapp_chat_participants":
            return await self._participants(scope, continuation, parent)
        if scope.record_type == "whatsapp_message_reactions":
            return await self._reactions(scope, continuation, parent)
        progress = WhatsAppProgress.model_validate(continuation.value)
        if (self.config.pagination == "cursor" and progress.offset) or (
            self.config.pagination == "offset" and (progress.cursor or progress.seen_cursor_hashes)
        ):
            raise ValueError("WhatsApp continuation belongs to another pagination mode")
        if progress.pages >= self.config.max_pages_per_scope:
            raise ValueError("WhatsApp max_pages_per_scope exceeded; increase the declared budget")
        await self.validate()
        params = {"limit": self.config.page_size}
        if self.config.pagination == "cursor":
            params["cursor"] = progress.cursor
        else:
            params["offset"] = progress.offset
        now = datetime.now(timezone.utc)
        if (
            scope.record_type == "whatsapp_chat"
            and scope.parent is None
            and scope.container_id is None
            and parent is None
        ):
            page = await self.client.chats(**params)
            self._check_page_size(page)
            # Unexpected unsupported kinds fail explicitly rather than claim complete scope.
            if any(chat.is_channel for chat in page.data):
                raise ValueError("WhatsApp channel returned outside declared direct/group scope")
            records = tuple(self._chat(chat, now) for chat in page.data)
        elif parent is not None and scope == self.child_scope(parent, "whatsapp_message"):
            if parent.payload.get("id") != parent.identity.native_id:
                raise ValueError("WhatsApp parent payload identity mismatch")
            page = await self.client.messages(parent.identity.native_id, **params)
            self._check_page_size(page)
            records = tuple(
                [await self._message(message, parent.identity, files, now) for message in page.data]
            )
        else:
            raise ValueError("Unsupported WhatsApp capture scope")
        if len({record.identity.entity_key for record in records}) != len(records):
            raise ValueError("WhatsApp page repeats a native identity")
        next_position, final = self._advance(page, progress)
        return CapturePage(records=records, continuation=next_position, final=final)

    async def _participants(  # noqa: C901 -- bounded atomic provider collection acquisition
        self, scope: CompletedScope, continuation: ScanContinuation, parent: SourceRecord | None
    ) -> CapturePage:
        """Acquire one bounded collection completely before the driver's atomic commit."""
        if (
            continuation.value
            or parent is None
            or scope != self.child_scope(parent, "whatsapp_chat_participants")
            or parent.identity.container_id is not None
            or parent.parent is not None
            or parent.payload_schema_version != 1
        ):
            raise ValueError("Invalid WhatsApp participant collection scope")
        chat = WhatsAppChat.model_validate(parent.payload)
        if chat.id != parent.identity.native_id or chat.is_channel:
            raise ValueError("WhatsApp participant parent identity disagrees")
        if not chat.is_group:
            return CapturePage(records=(), continuation=ScanContinuation(), final=True)
        await self.validate()
        pages = []
        cursor = None
        offset = 0
        byte_count = item_count = 0
        cursors = set()
        seen_member_ids = set()
        while True:
            if len(pages) >= self.config.maximum_participant_pages:
                raise ValueError("WhatsApp maximum_participant_pages exceeded")
            query = {"cursor": cursor} if cursor is not None else {"limit": self.config.page_size}
            if self.config.participant_pagination == "offset":
                query["offset"] = offset
            page = await self.client.participants(chat.id, **query)
            self._check_page_size(page)
            response = page.original()
            byte_count += len(json.dumps(response, ensure_ascii=False).encode())
            item_count += len(page.data)
            if byte_count > self.config.maximum_participant_bytes:
                raise ValueError("WhatsApp maximum_participant_bytes exceeded")
            if item_count > self.config.maximum_participant_items:
                raise ValueError("WhatsApp maximum_participant_items exceeded")
            for participant in page.data:
                if participant.user.id in seen_member_ids:
                    raise ValueError(
                        "WhatsApp participant identity repeated; ambiguous enumeration"
                    )
                seen_member_ids.add(participant.user.id)
            pages.append(WhatsAppCollectionPage(query=query, response=response))
            if self.config.participant_pagination == "cursor":
                cursor = page.cursor_after(cursor)
                if cursor is None:
                    break
                if cursor in cursors:
                    raise ValueError("WhatsApp participant cursor repeated without progress")
                cursors.add(cursor)
            else:
                next_offset = page.offset_after(offset, self.config.page_size)
                if next_offset is None:
                    break
                offset = next_offset
        collection = WhatsAppParticipantCollection(
            chat_id=chat.id,
            pagination=self.config.participant_pagination,
            page_size=self.config.page_size,
            pages=pages,
        )
        payload = collection.model_dump(mode="json")
        if (
            len(json.dumps(payload, ensure_ascii=False).encode())
            > self.config.maximum_participant_bytes
        ):
            raise ValueError("WhatsApp maximum_participant_bytes exceeded")
        record = CaptureRecord(
            identity=RecordIdentity(
                record_type="whatsapp_chat_participants",
                native_id=chat.id,
                container_id=scope.container_id,
            ),
            parent=parent.identity,
            payload=payload,
            observed_at=datetime.now(timezone.utc),
        )
        return CapturePage(records=(record,), continuation=ScanContinuation(), final=True)

    async def _reactions(  # noqa: C901 -- bounded atomic provider collection acquisition
        self, scope: CompletedScope, continuation: ScanContinuation, parent: SourceRecord | None
    ) -> CapturePage:
        """Fetch eligible owners regardless of absent/empty counters, never restricted content."""
        if (
            continuation.value
            or parent is None
            or scope != self.child_scope(parent, "whatsapp_message_reactions")
            or parent.payload_schema_version != 1
            or parent.parent is None
            or parent.parent.record_type != "whatsapp_chat"
            or parent.parent.container_id is not None
            or parent.identity.container_id != parent.parent.native_id
        ):
            raise ValueError("Invalid WhatsApp reaction collection scope")
        if parent.deleted_at is not None or parent.content_access != "available":
            return CapturePage(records=(), continuation=ScanContinuation(), final=True)
        message = WhatsAppMessage.model_validate(parent.payload)
        if (
            message.id != parent.identity.native_id
            or message.chat_id != parent.identity.container_id
        ):
            raise ValueError("WhatsApp reaction parent identity disagrees")
        if (
            message.is_deleted
            or message.is_hidden
            or message.is_event
            or message.view_mode is not None
        ):
            return CapturePage(records=(), continuation=ScanContinuation(), final=True)
        await self.validate()
        pages = []
        cursor = None
        offset = byte_count = item_count = 0
        cursors = set()
        page_digests = set()
        while True:
            if len(pages) >= self.config.maximum_reaction_pages:
                raise ValueError("WhatsApp maximum_reaction_pages exceeded")
            query = {"cursor": cursor} if cursor is not None else {"limit": self.config.page_size}
            if self.config.reaction_pagination == "offset":
                query["offset"] = offset
            page = await self.client.reactions(message.chat_id, message.id, **query)
            self._check_page_size(page)
            response = page.original()
            byte_count += len(json.dumps(response, ensure_ascii=False).encode())
            item_count += len(page.data)
            if byte_count > self.config.maximum_reaction_bytes:
                raise ValueError("WhatsApp maximum_reaction_bytes exceeded")
            if item_count > self.config.maximum_reaction_items:
                raise ValueError("WhatsApp maximum_reaction_items exceeded")
            if page.data:
                digest = reaction_page_digest(response["data"])
                if digest in page_digests:
                    raise ValueError("WhatsApp reaction whole page repeated; ambiguous enumeration")
                page_digests.add(digest)
            pages.append(WhatsAppCollectionPage(query=query, response=response))
            if self.config.reaction_pagination == "cursor":
                cursor = page.cursor_after(cursor)
                if cursor is None:
                    break
                if cursor in cursors:
                    raise ValueError("WhatsApp reaction cursor repeated without progress")
                cursors.add(cursor)
            else:
                next_offset = page.offset_after(offset, self.config.page_size)
                if next_offset is None:
                    break
                offset = next_offset
        collection = WhatsAppReactionCollection(
            chat_id=message.chat_id,
            message_id=message.id,
            pagination=self.config.reaction_pagination,
            page_size=self.config.page_size,
            pages=pages,
        )
        payload = collection.model_dump(mode="json")
        if (
            len(json.dumps(payload, ensure_ascii=False).encode())
            > self.config.maximum_reaction_bytes
        ):
            raise ValueError("WhatsApp maximum_reaction_bytes exceeded")
        record = CaptureRecord(
            identity=RecordIdentity(
                record_type="whatsapp_message_reactions",
                native_id=message.id,
                container_id=scope.container_id,
            ),
            parent=parent.identity,
            payload=payload,
            observed_at=datetime.now(timezone.utc),
        )
        return CapturePage(records=(record,), continuation=ScanContinuation(), final=True)

    async def _message(
        self,
        message: WhatsAppMessage,
        parent: RecordIdentity,
        files: FileService,
        observed_at: datetime,
    ) -> CaptureRecord:
        if message.chat_id != parent.native_id:
            raise UnipileError("identity")
        restricted = message.is_deleted or message.is_hidden or message.view_mode is not None
        blobs = []
        if not restricted:
            for index, attachment in enumerate(message.attachments):
                if attachment.is_unavailable or (
                    attachment.file_size is not None
                    and attachment.file_size > self.config.maximum_attachment_bytes
                ):
                    continue
                try:
                    content, media_type = await self.client.attachment(
                        message.chat_id,
                        message.id,
                        attachment.id,
                        max_bytes=self.config.maximum_attachment_bytes,
                        expected_mimetype=attachment.mimetype,
                    )
                except FileSkippedException:
                    continue
                blob = await files.store_canonical_blob(content, media_type=media_type)
                blobs.append(
                    blob.model_copy(
                        update={
                            "source_path": f"/attachments/{index}",
                            "filename": attachment.filename,
                        }
                    )
                )
        return CaptureRecord(
            identity=RecordIdentity(
                record_type="whatsapp_message", native_id=message.id, container_id=message.chat_id
            ),
            parent=parent,
            payload=message.original(),
            descendant_visibility_fields=("is_hidden", "view_mode", "is_event"),
            completeness="partial",
            observed_at=observed_at,
            source_created_at=TypeAdapter(
                AwareDatetime, config=ConfigDict(hide_input_in_errors=True)
            ).validate_python(message.timestamp),
            kind="delete" if message.is_deleted else "upsert",
            removal_reason="provider_deleted" if message.is_deleted else None,
            blobs=tuple(blobs),
        )
