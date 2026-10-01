"""Offline Outlook projection from native metadata and retained, verified RFC822 bytes."""

import hashlib
import html
import mimetypes
import re
from datetime import datetime
from email import policy
from email.errors import MessageError
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import quote
from pathlib import Path

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.extraction_models import CharsetRecovery, ExtractionPart
from airweave.domains.entities.canonical.mime_text import decode_mime_text
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.entities._base import Breadcrumb
from airweave.platform.entities.outlook_mail import OutlookAttachmentEntity, OutlookMessageEntity
from airweave.platform.sources.outlook_mail_models import OutlookMessage, OutlookRecipient

# Fail rather than publish a truncated body for pathological MIME trees.
MAX_MIME_DEPTH = 32
MAX_MIME_PARTS = 1024
_UNSUPPORTED = {"multipart/encrypted", "application/pkcs7-mime", "application/x-pkcs7-mime"}


def _address(recipient: OutlookRecipient | None) -> str | None:
    if recipient is None or not recipient.emailAddress.address:
        return None
    address = recipient.emailAddress
    # Search metadata is readable text, not a wire header requiring RFC2047 encoding.
    return f'"{quote(address.name)}" <{address.address}>' if address.name else address.address


def _addresses(recipients: list[OutlookRecipient]) -> list[str]:
    return [address for recipient in recipients if (address := _address(recipient))]


def _date(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Outlook metadata timestamp requires a timezone")
    return parsed


def _decoded(part: EmailMessage) -> bytes:
    encoding = str(part.get("Content-Transfer-Encoding", "7bit")).strip().lower()
    if encoding not in {"7bit", "8bit", "binary", "base64", "quoted-printable"}:
        raise ValueError("Unsupported MIME transfer encoding")
    if encoding == "quoted-printable":
        # The stdlib deliberately accepts invalid escapes. Preserve no silently repaired text.
        raw = part.get_payload(decode=False)
        if not isinstance(raw, str) or re.search(r"=(?![0-9A-Fa-f]{2}|\r?\n)", raw):
            raise ValueError("Malformed quoted-printable MIME payload")
    content = part.get_payload(decode=True)
    if not isinstance(content, bytes):
        raise ValueError("MIME leaf must contain bytes")
    if part.defects:
        raise ValueError("Malformed MIME transfer payload")
    return content


def _parse(content: bytes) -> EmailMessage:
    try:
        message = BytesParser(policy=policy.default.clone(raise_on_defect=True)).parsebytes(content)
        pending = [(message, 0)]
        count = 0
        while pending:
            part, depth = pending.pop()
            count += 1
            if depth > MAX_MIME_DEPTH or count > MAX_MIME_PARTS:
                raise ValueError("Outlook MIME tree exceeds projection depth or part limit")
            if part.defects or any(header.defects for header in part.values()):
                raise ValueError("Malformed Outlook MIME headers")
            if part.is_multipart():
                pending.extend((child, depth + 1) for child in part.iter_parts())
            else:
                _decoded(part)
        return message
    except (MessageError, RecursionError) as error:
        raise ValueError("Malformed Outlook MIME content") from error


def _file(part: EmailMessage) -> bool:
    return (
        bool(part.get_filename())
        or part.get_content_disposition() == "attachment"
        or (not part.is_multipart() and part.get_content_type() not in {"text/plain", "text/html"})
    )


def _suffix(part: EmailMessage) -> str:
    # MIME type is authoritative over misleading names (e.g. a PDF called image.png).
    mime = part.get_content_type()
    mime_suffix = mimetypes.guess_extension(mime) if mime != "application/octet-stream" else None
    if mime_suffix and re.fullmatch(r"\.[a-z0-9]{1,12}", mime_suffix):
        return mime_suffix
    filename_suffix = Path(part.get_filename() or "").suffix.lower()
    return filename_suffix if re.fullmatch(r"\.[a-z0-9]{1,12}", filename_suffix) else ".bin"


class OutlookProjection:
    """Disposable extraction inputs; canonical originals remain unchanged."""

    def __init__(self, record: SourceRecord, directory: Path, native: OutlookMessage):
        """Bind validated native metadata and caller-owned scratch directory."""
        self.record = record
        self.directory = directory
        self.native = native
        self.parts: list[ProjectionInput] = []
        self.fragments: list[str] = []
        self.charset_recoveries: list[CharsetRecovery] = []

    def unsupported(self, part: EmailMessage, path: str) -> None:
        """Account for retained content whose semantics are not yet extractable."""
        self.parts.append(
            ProjectionInput(
                part=ExtractionPart(
                    part_index=len(self.parts) + 1,
                    key=path,
                    kind="file",
                    media_type=part.get_content_type(),
                ),
                entity=None,
                omission="unsupported_format",
            )
        )

    async def attachment(self, part: EmailMessage, path: str) -> None:
        """Materialize safe content-hash paths, independent of provider filenames."""
        content = _decoded(part)
        suffix = _suffix(part)
        local = await write_blob(content, self.directory, suffix=suffix)
        message_id = self.native.id
        self.parts.append(
            ProjectionInput(
                part=ExtractionPart(
                    part_index=len(self.parts) + 1,
                    key=path,
                    kind="file",
                    media_type=part.get_content_type(),
                    extension=suffix,
                ),
                entity=OutlookAttachmentEntity(
                    composite_id=f"{message_id}:{hashlib.sha256(path.encode()).hexdigest()}",
                    name=part.get_filename() or "Inline content",
                    message_id=message_id,
                    attachment_id=path,
                    content_type=part.get_content_type(),
                    is_inline=part.get_content_disposition() == "inline"
                    or bool(part.get("Content-ID")),
                    content_id=str(part["Content-ID"]) if part["Content-ID"] else None,
                    metadata={"identity_kind": "mime_path"},
                    message_web_url=self.native.webLink,
                    url=self.native.webLink or "https://outlook.office.com/mail/",
                    size=len(content),
                    file_type=suffix[1:],
                    mime_type=part.get_content_type(),
                    local_path=str(local),
                    breadcrumbs=[
                        Breadcrumb(
                            entity_id=message_id,
                            name=self.native.subject or "Email message",
                            entity_type="OutlookMessageEntity",
                        )
                    ],
                ),
            )
        )

    async def render(self, part: EmailMessage, path: str, *, body: bool = True) -> None:
        """Select one alternative while still retaining independent inline/file parts."""
        mime = part.get_content_type()
        if mime.startswith("message/") or mime in _UNSUPPORTED:
            self.unsupported(part, path)
            return
        if part.is_multipart():
            if part.get_content_disposition() == "attachment":
                self.unsupported(part, path)
                return
            children = list(part.iter_parts())
            selected = set(range(len(children)))
            if mime == "multipart/alternative":
                # get_body understands nested related parts and their explicit start CID.
                preferred = part.get_body(preferencelist=("html", "plain"))
                selected = {
                    i
                    for i, child in enumerate(children)
                    if preferred is not None and any(node is preferred for node in child.walk())
                }
            elif mime == "multipart/related":
                start = part.get_param("start")
                selected = {
                    next(
                        (
                            i
                            for i, child in enumerate(children)
                            if start and child.get("Content-ID") == start
                        ),
                        0,
                    )
                }
            for index, child in enumerate(children):
                await self.render(child, f"{path}/parts/{index}", body=body and index in selected)
            return
        if _file(part) or (not body and part.get("Content-ID")):
            await self.attachment(part, path)
        elif body:
            decoded = decode_mime_text(
                _decoded(part), part.get_content_charset() or "us-ascii", path
            )
            if decoded.recovery is not None:
                self.charset_recoveries.append(decoded.recovery)
            text = decoded.text
            self.fragments.append(
                text if mime == "text/html" else "<pre>" + html.escape(text) + "</pre>"
            )

    async def body(self) -> ProjectionInput:
        """Expose full decoded text and independently retained native message metadata."""
        content = "\n".join(self.fragments).encode("utf-8")
        local = await write_blob(content, self.directory, suffix=".html")
        native = self.native
        return ProjectionInput(
            part=ExtractionPart(
                part_index=0,
                key="/mime/body",
                kind="body",
                media_type="text/html",
                extension=".html",
                charset_recoveries=tuple(self.charset_recoveries),
            ),
            entity=OutlookMessageEntity(
                id=native.id,
                breadcrumbs=[],
                folder_id=native.parentFolderId,
                subject=native.subject.strip()
                if native.subject and native.subject.strip()
                else "Email message",
                sender=_address(native.sender),
                from_address=_address(native.from_),
                conversation_id=native.conversationId,
                to_recipients=_addresses(native.toRecipients),
                cc_recipients=_addresses(native.ccRecipients),
                bcc_recipients=_addresses(native.bccRecipients),
                reply_to=_addresses(native.replyTo),
                sent_date=_date(native.sentDateTime),
                received_date=_date(native.receivedDateTime),
                internet_message_id=native.internetMessageId,
                is_draft=native.isDraft,
                has_attachments=native.hasAttachments,
                web_url_override=native.webLink,
                url=native.webLink or "https://outlook.office.com/mail/",
                size=len(content),
                file_type="html",
                mime_type="text/html",
                local_path=str(local),
            ),
        )


async def map_outlook(
    record: SourceRecord, storage: StorageBackend, directory: Path
) -> ProjectionInputs:
    """Project schema-one mailbox messages without provider calls or body-preview fallback."""
    if (
        record.deleted_at is not None
        or record.content_access != "available"
        or record.payload_schema_version != 1
        or record.identity.record_type != "message"
        or record.identity.container_id is not None
        or record.parent is not None
        or record.completeness not in {"partial", "metadata_only"}
    ):
        raise ValueError("Outlook projection requires an active schema-one mailbox message")
    native = OutlookMessage.model_validate(record.payload)
    if native.id != record.identity.native_id:
        raise ValueError("Outlook projection identity differs from retained native metadata")
    if len(record.blobs) > 1 or any(
        ref.role is not None or ref.source_path is not None or ref.media_type != "message/rfc822"
        for ref in record.blobs
    ):
        raise ValueError("Outlook projection requires one top-level RFC822 original")
    if not record.blobs:
        if record.completeness != "metadata_only":
            raise ValueError("Missing Outlook MIME requires an explicit metadata-only capture")
        return ProjectionInputs(
            parts=(
                ProjectionInput(
                    part=ExtractionPart(
                        part_index=0, key="/mime", kind="body", media_type="message/rfc822"
                    ),
                    entity=None,
                ),
            )
        )
    if record.completeness != "partial":
        raise ValueError("Outlook MIME capture must report pending attachment inventory")
    message = _parse(await read_blob(record, record.blobs[0], storage))
    projection = OutlookProjection(record, directory, native)
    await projection.render(message, "/mime")
    body = await projection.body()
    if not projection.fragments and any(part.omission for part in projection.parts):
        body = ProjectionInput(part=body.part, entity=None, omission="unsupported_format")
    # MIME cannot prove native reference/item attachments were represented in the response.
    inventory = ProjectionInput(
        part=ExtractionPart(
            part_index=len(projection.parts) + 1, key="/attachment_inventory", kind="record"
        ),
        entity=None,
    )
    return ProjectionInputs(parts=(body, *projection.parts, inventory))
