"""Original Gmail message capture, independent of derived thread/search entities."""

import base64
import hashlib
import json
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime, timezone

import httpx
from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.page_source import InvalidScanContinuation
from airweave.domains.entities.canonical.requests import (
    BlobReference,
    CaptureRecord,
    RecordIdentity,
)
from airweave.domains.sources.exceptions import SourceEntityNotFoundError, SourceError
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService

BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
Get = Callable[..., Awaitable[dict]]


class _HistoryMessage(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(min_length=1)


class _HistoryChange(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    message: _HistoryMessage


class _HistoryEntry(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    messages: list[_HistoryMessage] = Field(default_factory=list)
    messagesAdded: list[_HistoryChange] = Field(default_factory=list)
    messagesDeleted: list[_HistoryChange] = Field(default_factory=list)
    labelsAdded: list[_HistoryChange] = Field(default_factory=list)
    labelsRemoved: list[_HistoryChange] = Field(default_factory=list)


class _HistoryResponse(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    history: list[_HistoryEntry] = Field(default_factory=list)
    historyId: str = Field(min_length=1)
    nextPageToken: str | None = Field(default=None, min_length=1)


class HistoryPage(BaseModel):
    """One provider page; its mailbox boundary is not a committed checkpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    message_ids: tuple[str, ...]
    history_id: str
    next_page_token: str | None
    fingerprint: str


class HydratedMessages(BaseModel):
    """Bounded current observations; callers atomically commit records and offset."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    records: tuple[CaptureRecord, ...] = Field(max_length=500)
    next_offset: int
    complete: bool


def parse_history_page(raw: dict) -> HistoryPage:
    """Normalize only one page, preserving first occurrence order across all event arrays."""
    response = _HistoryResponse.model_validate(raw)
    affected: dict[str, None] = {}
    for entry in response.history:
        for message in entry.messages:
            affected[message.id] = None
        for changes in (
            entry.messagesAdded,
            entry.messagesDeleted,
            entry.labelsAdded,
            entry.labelsRemoved,
        ):
            for change in changes:
                affected[change.message.id] = None
    ids = tuple(affected)
    # Intermediate mailbox historyId can advance without changing this page.
    # Terminal pages bind the candidate checkpoint to the exact hydrated response.
    # Include native events: a later edit can leave deduplicated IDs unchanged.
    fingerprint = hashlib.sha256(
        json.dumps(
            [
                raw.get("history", []),
                response.nextPageToken,
                response.historyId if response.nextPageToken is None else None,
            ],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    return HistoryPage(
        message_ids=ids,
        history_id=response.historyId,
        next_page_token=response.nextPageToken,
        fingerprint=fingerprint,
    )


def _external_parts(part: dict, path: str = "/payload") -> Iterator[tuple[dict, str]]:
    """Keep a JSON Pointer to each external MIME body in untouched provider JSON."""
    if part.get("body", {}).get("attachmentId"):
        yield part, path + "/body"
    for index, child in enumerate(part.get("parts", [])):
        yield from _external_parts(child, f"{path}/parts/{index}")


class GmailCapture:
    """Fetch raw provider messages with explicit reconciliation and cursor barriers."""

    def __init__(
        self,
        get: Get,
        query: str | None,
        *,
        files: FileService | None = None,
        attachment_get: Get | None = None,
    ):
        """Reuse the source managed HTTP/auth transport and exact provider query."""
        self.get = get
        self.query = query
        self.files = files
        self.attachment_get = attachment_get

    async def message(self, message_id: str) -> CaptureRecord:
        """Observe current state; a disappeared message is a provider deletion."""
        identity = RecordIdentity(record_type="message", native_id=message_id)
        try:
            payload = await self.get(f"{BASE}/messages/{message_id}", params={"format": "full"})
        except (httpx.HTTPStatusError, SourceEntityNotFoundError) as exc:
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code != 404:
                raise
            return CaptureRecord(
                identity=identity,
                payload={"id": message_id},
                kind="delete",
                removal_reason="provider_deleted",
                observed_at=datetime.now(timezone.utc),
            )
        if payload.get("id") != message_id:
            raise ValueError("Gmail message response identity mismatch")
        blobs, complete = await self.capture_bodies(message_id, payload)
        created = payload.get("internalDate")
        return CaptureRecord(
            identity=identity,
            payload=payload,
            observed_at=datetime.now(timezone.utc),
            source_created_at=(
                datetime.fromtimestamp(int(created) / 1000, timezone.utc)
                if created is not None
                else None
            ),
            completeness="complete" if complete else "partial",
            blobs=blobs,
        )

    async def capture_bodies(
        self,
        message_id: str,
        payload: dict,
    ) -> tuple[tuple[BlobReference, ...], bool]:
        """Persist MIME bytes before their record; only deliberate size skips are partial."""
        blobs = []
        complete = True
        for part, path in _external_parts(payload.get("payload", {})):
            if self.files is None or self.attachment_get is None:
                complete = False
                continue
            try:
                content = await self.body_bytes(message_id, part["body"])
                blob = await self.files.store_canonical_blob(
                    content, media_type=part.get("mimeType")
                )
            except FileSkippedException:
                complete = False
                continue
            blobs.append(
                BlobReference.model_validate(
                    {
                        **blob.model_dump(),
                        "source_path": path,
                        "filename": part.get("filename") or None,
                    }
                )
            )
        return tuple(blobs), complete

    async def body_bytes(self, message_id: str, body: dict) -> bytes:
        """Validate declared, encoded and decoded lengths before accepting provider bytes."""
        maximum = self.files.MAX_FILE_SIZE_BYTES
        if int(body.get("size", 0)) > maximum:
            raise FileSkippedException("MIME body exceeds size limit", "MIME body")
        encoded_limit = ((maximum + 2) // 3) * 4
        result = await self.attachment_get(
            f"{BASE}/messages/{message_id}/attachments/{body['attachmentId']}",
            max_bytes=encoded_limit + 4096,
        )
        encoded = result["data"]
        if len(encoded) > encoded_limit:
            raise FileSkippedException("Encoded MIME body exceeds size limit", "MIME body")
        content = base64.b64decode(
            encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
        if len(content) > maximum:
            raise FileSkippedException("Decoded MIME body exceeds size limit", "MIME body")
        if len(content) != int(result["size"]) or len(content) != int(body["size"]):
            raise ValueError("Gmail MIME body size does not match provider metadata")
        return content

    async def history_page(
        self, boundary: str, token: str | None = None, *, max_results: int = 500
    ) -> HistoryPage:
        """Fetch one history page; boundaries stay opaque and no cursor is advanced."""
        if not boundary or not 1 <= max_results <= 500:
            raise ValueError("History requires a boundary and a page size from 1 to 500")
        params = {"startHistoryId": boundary, "maxResults": max_results}
        if token is not None:
            params["pageToken"] = token
        return parse_history_page(await self.get(f"{BASE}/history", params=params))

    async def hydrate_history_page(
        self,
        page: HistoryPage,
        *,
        offset: int = 0,
        limit: int = 500,
        expected_fingerprint: str | None = None,
    ) -> HydratedMessages:
        """Reuse exact current reads, never apply stale history deletion/label payloads."""
        if expected_fingerprint is not None and page.fingerprint != expected_fingerprint:
            raise InvalidScanContinuation("Gmail history page changed before offset replay")
        if offset and expected_fingerprint is None:
            raise ValueError("Resuming Gmail history hydration requires its page fingerprint")
        if not 0 <= offset <= len(page.message_ids) or not 1 <= limit <= 500:
            raise ValueError("Invalid Gmail history hydration offset or batch size")
        return await self.hydrate_messages(page.message_ids, offset=offset, limit=limit)

    async def hydrate_messages(
        self, message_ids: tuple[str, ...], *, offset: int = 0, limit: int = 50
    ) -> HydratedMessages:
        """Stop at complete originals; bounded byte targets never truncate native content."""
        if not 0 <= offset <= len(message_ids) or not 1 <= limit <= 500:
            raise ValueError("Invalid Gmail message hydration boundary")
        records = []
        total_bytes = 0
        for mid in message_ids[offset : offset + limit]:
            record = await self.message(mid)
            size = len(record.model_dump_json().encode())
            if size > 32 * 1024 * 1024:
                raise SourceError(
                    "Gmail original exceeds the 32 MiB record limit", source_short_name="gmail"
                )
            records.append(record)
            total_bytes += size
            if total_bytes >= 8 * 1024 * 1024:
                break
        end = offset + len(records)
        return HydratedMessages(
            records=tuple(records), next_offset=end, complete=end == len(message_ids)
        )
