"""Deterministic Gmail search inputs from committed payloads and immutable blobs only."""

import base64
import hashlib
import html
import mimetypes
from datetime import datetime, timezone
from email.message import Message
from email.utils import formataddr, getaddresses
from pathlib import Path

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.entities._base import Breadcrumb
from airweave.platform.entities.gmail import GmailAttachmentEntity, GmailMessageEntity


def _header(part: dict, name: str) -> str:
    return next(
        (h["value"] for h in part.get("headers", []) if h["name"].lower() == name.lower()), ""
    )


def _attachment(part: dict) -> bool:
    mime = part.get("mimeType", "").lower()
    return (
        bool(part.get("filename"))
        or _header(part, "Content-Disposition").lower().startswith("attachment")
        or (not mime.startswith("multipart/") and mime not in {"text/plain", "text/html"})
    )


def _has_html(part: dict) -> bool:
    """Find an HTML alternative even when wrapped by multipart/related."""
    return part.get("mimeType") == "text/html" or any(
        _has_html(child) for child in part.get("parts", [])
    )


async def _body(
    part: dict,
    path: str,
    record: SourceRecord,
    storage: StorageBackend,
) -> bytes:
    body = part.get("body", {})
    if body.get("attachmentId"):
        matching = [b for b in record.blobs if b.source_path == path + "/body"]
        if len(matching) != 1:
            raise ValueError("Gmail MIME body requires exactly one stored blob reference")
        content = await read_blob(record, matching[0], storage)
    else:
        encoded = body.get("data", "")
        content = base64.b64decode(
            encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
    if len(content) != int(body.get("size", 0)):
        raise ValueError("Gmail MIME size differs from committed provider metadata")
    return content


def _decode_text(content: bytes, part: dict) -> str:
    header = Message()
    header["Content-Type"] = _header(part, "Content-Type") or part["mimeType"]
    return content.decode(header.get_content_charset() or "utf-8", errors="strict")


class GmailProjection:
    """One message projection; per-call state never escapes into the source record."""

    def __init__(self, record: SourceRecord, storage: StorageBackend, directory: Path):
        """Bind committed source authority and disposable output directory."""
        self.record = record
        self.storage = storage
        self.directory = directory
        self.attachments: list[ProjectionInput] = []

    async def attachment(self, part: dict, path: str) -> None:
        """Project retained attachment bytes; keep missing bytes explicit on the original."""
        filename = part.get("filename") or "attachment"
        mime = part.get("mimeType", "application/octet-stream")
        suffix = Path(filename).suffix.lower() or mimetypes.guess_extension(mime) or ".bin"
        descriptor = ExtractionPart(
            part_index=len(self.attachments) + 1,
            key=path,
            kind="file",
            media_type=mime,
            extension=suffix,
        )
        if (
            self.record.completeness == "partial"
            and part.get("body", {}).get("attachmentId")
            and not any(blob.source_path == path + "/body" for blob in self.record.blobs)
        ):
            self.attachments.append(ProjectionInput(part=descriptor, entity=None))
            return
        content = await _body(part, path, self.record, self.storage)
        local = await write_blob(content, self.directory, suffix=suffix)
        message_id = self.record.identity.native_id
        entity = GmailAttachmentEntity(
            breadcrumbs=[
                Breadcrumb(
                    entity_id=f"msg_{message_id}",
                    name="Email message",
                    entity_type="GmailMessageEntity",
                )
            ],
            attachment_key=f"attach_{message_id}_{hashlib.sha256(path.encode()).hexdigest()}",
            filename=filename,
            message_id=message_id,
            attachment_id=part.get("body", {}).get("attachmentId") or part.get("partId") or path,
            thread_id=self.record.payload["threadId"],
            url=f"https://mail.google.com/mail/u/0/#inbox/{message_id}",
            size=len(content),
            file_type=suffix.lstrip("."),
            mime_type=mime,
            local_path=str(local),
        )
        self.attachments.append(ProjectionInput(part=descriptor, entity=entity))

    async def render(self, part: dict, path: str) -> str:
        """Select MIME alternatives, preserve mixed bodies, and materialize attachments."""
        mime = part.get("mimeType", "").lower()
        if mime.startswith("multipart/"):
            children = list(enumerate(part.get("parts", [])))
            if mime == "multipart/alternative":
                html_parts = [(i, p) for i, p in children if _has_html(p)]
                plain_parts = [(i, p) for i, p in children if p.get("mimeType") == "text/plain"]
                # Prefer one complete alternative, not duplicate plain and HTML text.
                children = html_parts[-1:] or plain_parts[-1:] or children[-1:]
            return "\n".join(
                [await self.render(child, f"{path}/parts/{index}") for index, child in children]
            )
        if _attachment(part):
            await self.attachment(part, path)
            return ""
        content = await _body(part, path, self.record, self.storage)
        text = _decode_text(content, part)
        return text if mime == "text/html" else "<pre>" + html.escape(text) + "</pre>"

    async def map(self) -> ProjectionInputs:
        """Produce file entities for strict conversion without any provider network calls."""
        data = self.record.payload
        if self.record.deleted_at is not None or self.record.completeness not in {
            "complete",
            "partial",
        }:
            raise ValueError("Gmail projection requires an active captured source record")
        if data.get("id") != self.record.identity.native_id or not data.get("threadId"):
            raise ValueError("Gmail projection identity is inconsistent")
        body = await self.render(data["payload"], "/payload")
        local = await write_blob(body.encode("utf-8"), self.directory, suffix=".html")
        entity = GmailMessageEntity.from_api(data, thread_id=data["threadId"], breadcrumbs=[])
        entity.local_path = str(local)
        entity.size = len(body.encode("utf-8"))
        internal = datetime.fromtimestamp(int(data["internalDate"]) / 1000, timezone.utc)
        entity.internal_date = internal
        entity.internal_timestamp = internal
        if entity.sent_at.tzinfo is None:
            entity.sent_at = entity.sent_at.replace(tzinfo=timezone.utc)
        for field in ("to", "cc", "bcc"):
            addresses = getaddresses([_header(data["payload"], field)])
            setattr(entity, field, [formataddr(address) for address in addresses if address[1]])
        return ProjectionInputs(
            parts=(
                ProjectionInput(
                    part=ExtractionPart(
                        part_index=0,
                        key="/payload/body",
                        kind="body",
                        media_type="text/html",
                        extension=".html",
                    ),
                    entity=entity,
                ),
                *self.attachments,
            )
        )


async def map_gmail(
    record: SourceRecord,
    storage: StorageBackend,
    directory: Path,
) -> ProjectionInputs:
    """Map immutable Gmail capture into existing message and attachment entities."""
    return await GmailProjection(record, storage, directory).map()
