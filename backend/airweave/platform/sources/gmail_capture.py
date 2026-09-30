"""Original Gmail message capture, independent of derived thread/search entities."""

import base64
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
from datetime import datetime, timezone

import httpx

from airweave.domains.entities.canonical.requests import (
    BlobReference,
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    StartedScope,
)
from airweave.domains.entities.canonical.source import SourceObservation
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor

BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
Get = Callable[..., Awaitable[dict]]


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
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
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
            blobs.append(blob.model_copy(update={"source_path": path}))
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

    async def history(self, boundary: str) -> tuple[list[str], str]:
        """Drain history before emitting, so an expired boundary can restart cleanly."""
        params = {"startHistoryId": boundary, "maxResults": 500}
        affected: dict[str, None] = {}
        tokens: set[str] = set()
        while True:
            page = await self.get(f"{BASE}/history", params=dict(params))
            for entry in page.get("history", []):
                # `messages` is the full changed-ID list; typed lists can overlap it.
                for message in entry.get("messages", []):
                    affected[message["id"]] = None
                for field in ("messagesAdded", "messagesDeleted", "labelsAdded", "labelsRemoved"):
                    for change in entry.get(field, []):
                        affected[change["message"]["id"]] = None
            token = page.get("nextPageToken")
            if not token:
                return list(affected), str(page["historyId"])
            if token in tokens:
                raise ValueError("Gmail history pagination repeated a token")
            tokens.add(token)
            params["pageToken"] = token

    async def enumerate(self) -> AsyncGenerator[CaptureRecord, None]:
        """List every page in the provider's exact configured query scope."""
        params = {"maxResults": 500, "includeSpamTrash": "true"}
        if self.query:
            params["q"] = self.query
        tokens: set[str] = set()
        while True:
            page = await self.get(f"{BASE}/messages", params=dict(params))
            for item in page.get("messages", []):
                yield await self.message(item["id"])
            token = page.get("nextPageToken")
            if not token:
                return
            if token in tokens:
                raise ValueError("Gmail messages pagination repeated a token")
            tokens.add(token)
            params["pageToken"] = token

    async def saved_history(self, cursor: SyncCursor | None) -> tuple[list[str], str] | None:
        """Only canonical unfiltered checkpoints can resume this record collection."""
        saved = cursor.data if cursor else {}
        boundary = saved.get("history_id") if saved.get("canonical_query") == "" else None
        if not boundary:
            return None
        try:
            return await self.history(boundary)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            return None

    async def generate(self, cursor: SyncCursor | None) -> AsyncGenerator[SourceObservation, None]:
        """Filtered queries reconcile each run; unfiltered mailboxes use Gmail history.

        Gmail queries cannot be faithfully evaluated against history JSON. Full query
        enumeration is a best-effort changing mailbox view, not a snapshot. Never
        advance an incremental cursor for such a view. External MIME bytes require
        successful immutable storage; missing storage or oversize bodies remain partial.
        """
        if self.query:
            yield StartedScope(record_type="message")
            async for record in self.enumerate():
                yield record
            yield CompletedScope(record_type="message")
            if cursor:
                cursor.update(history_id="", canonical_query=self.query)
            return

        history = await self.saved_history(cursor)
        if history is None:
            profile = await self.get(f"{BASE}/profile")
            boundary = str(profile["historyId"])
            yield StartedScope(record_type="message")
            async for record in self.enumerate():
                yield record
            # If this boundary expires mid-crawl, fail the run. Starting another full
            # crawl in this attempt would incorrectly retain earlier seen records.
            history = await self.history(boundary)
            full = True
        else:
            full = False
        message_ids, latest = history
        for message_id in message_ids:
            yield await self.message(message_id)
        if full:
            yield CompletedScope(record_type="message")
        if cursor:
            cursor.update(history_id=latest, canonical_query="")
