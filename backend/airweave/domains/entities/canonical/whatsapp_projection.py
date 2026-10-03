"""Offline WhatsApp text and original media from committed v2 JSON and blobs."""

import json
import mimetypes
import re
from datetime import datetime
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, ConfigDict, TypeAdapter

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.entities.canonical.requests import parent_container_key
from airweave.domains.storage.protocols import StorageBackend
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.entities._base import Breadcrumb
from airweave.platform.entities.whatsapp import WhatsAppAttachmentEntity, WhatsAppMessageEntity
from airweave.platform.sources.records.whatsapp_collections import (
    WhatsAppParticipantCollection,
    WhatsAppReactionCollection,
)
from airweave.platform.sources.records.whatsapp_models import (
    WhatsAppAttachment,
    WhatsAppChat,
    WhatsAppMessage,
    WhatsAppUser,
)


class _ContactLabel(BaseModel):
    """Provider profile details and owner-assigned contact labels are different facts."""

    model_config = ConfigDict(extra="ignore", strict=True)
    contact_name: str | None = None


def _key(chat_id: str, message_id: str, part: str) -> str:
    return json.dumps([chat_id, message_id, part], ensure_ascii=False, separators=(",", ":"))


def _body_entity(
    message: WhatsAppMessage,
    *,
    native_id: str,
    source_path: str,
    text: str,
    sender: WhatsAppUser | None,
    sender_id: str | None,
    is_sender: bool | None,
    timestamp: datetime | None,
) -> WhatsAppMessageEntity:
    entity = WhatsAppMessageEntity(
        projection_key=_key(message.chat_id, message.id, source_path),
        title="Quoted WhatsApp message" if source_path == "/quoted" else "WhatsApp message",
        text=text,
        message_id=native_id,
        owner_message_id=message.id,
        chat_id=message.chat_id,
        source_path=source_path,
        sender_id=sender_id,
        is_sender=is_sender,
        sender_display_name=sender.display_name if sender else None,
        sender_contact_name=_ContactLabel.model_validate(sender.specifics or {}).contact_name
        if sender
        else None,
        sender_public_identifier=sender.public_identifier if sender else None,
        sent_at=timestamp,
        breadcrumbs=[],
    )
    populate_base_fields(entity)
    return entity


def _text_part(index: int, path: str, entity: WhatsAppMessageEntity) -> ProjectionInput:
    return ProjectionInput(
        part=ExtractionPart(part_index=index, key=path, kind="body", media_type="text/plain"),
        entity=entity,
        native_body=NativeTextBody(text=entity.text, metadata_fields=("text",)),
    )


def _suffix(attachment: WhatsAppAttachment) -> str:
    suffix = Path(attachment.filename or "").suffix.lower()
    if not re.fullmatch(r"\.[a-z0-9]{1,12}", suffix):
        suffix = mimetypes.guess_extension(attachment.mimetype or "") or ".bin"
    return suffix


def _validate_blobs(record: SourceRecord, message: WhatsAppMessage) -> None:
    """Bind every original blob to an unchanged native attachment position."""
    paths = {f"/attachments/{index}" for index in range(len(message.attachments))}
    if message.quoted:
        paths |= {
            f"/quoted/attachments/{index}" for index in range(len(message.quoted.attachments))
        }
    if any(blob.role is not None or blob.source_path not in paths for blob in record.blobs):
        raise ValueError("WhatsApp blob does not identify a current native attachment")
    if len({blob.source_path for blob in record.blobs}) != len(record.blobs):
        raise ValueError("WhatsApp attachment has ambiguous retained originals")
    if len({item.id for item in message.attachments}) != len(message.attachments):
        raise ValueError("WhatsApp message contains duplicate native attachment IDs")


def _text_parts(
    message: WhatsAppMessage, timestamp: datetime, *, restricted: bool
) -> list[ProjectionInput]:
    """Keep authored body and quoted snapshot as distinct native text inputs."""
    # Ephemeral WhatsApp content is phone-only. Never expose even embedded quote
    # text or accidentally captured media through this offline search projection.
    parts: list[ProjectionInput] = []
    if restricted or message.text is None:
        parts.append(
            ProjectionInput(
                part=ExtractionPart(part_index=0, key="/text", kind="body"), entity=None
            )
        )
    else:
        body = _body_entity(
            message,
            native_id=message.id,
            source_path="/text",
            text=message.text,
            sender=message.sender,
            sender_id=message.sender_id,
            is_sender=message.is_sender,
            timestamp=timestamp,
        )
        parts.append(_text_part(0, "/text", body))
    quote = message.quoted
    if quote is not None:
        if restricted or quote.text is None:
            parts.append(
                ProjectionInput(
                    part=ExtractionPart(part_index=len(parts), key="/quoted", kind="body"),
                    entity=None,
                )
            )
        else:
            entity = _body_entity(
                message,
                native_id=quote.id,
                source_path="/quoted",
                text=quote.text,
                sender=quote.sender,
                sender_id=quote.sender.id if quote.sender else None,
                is_sender=None,
                timestamp=None,
            )
            parts.append(_text_part(len(parts), "/quoted", entity))
    return parts


def _captured_message(record: SourceRecord) -> WhatsAppMessage:
    """Parse only a readable, account/chat-scoped retained native message."""
    if record.identity.record_type != "whatsapp_message" or record.payload_schema_version != 1:
        raise ValueError("WhatsApp projection requires a schema1 message original")
    if record.deleted_at is not None or record.content_access != "available":
        raise ValueError("Unavailable WhatsApp records cannot be projected")
    message = WhatsAppMessage.model_validate(record.payload)
    if message.id != record.identity.native_id or message.chat_id != record.identity.container_id:
        raise ValueError("WhatsApp message identity differs from its retained original")
    if message.sender and message.sender.id != message.sender_id:
        raise ValueError("WhatsApp sender profile disagrees with native sender ID")
    TypeAdapter(AwareDatetime).validate_python(message.timestamp)
    return message


def _excluded_message(message: WhatsAppMessage) -> bool:
    return bool(message.is_deleted or message.is_hidden or message.is_event)


def excluded_whatsapp(record: SourceRecord) -> bool:
    """Validate authority records before allowing a zero-document publication."""
    if record.identity.record_type == "whatsapp_message_reactions":
        parent = record.parent
        if (
            record.payload_schema_version != 1
            or parent is None
            or parent.record_type != "whatsapp_message"
            or parent.container_id is None
            or record.identity.native_id != parent.native_id
            or record.identity.container_id != parent_container_key(parent)
            or record.blobs
            or record.completeness != "complete"
            or record.source_created_at is not None
            or record.source_updated_at is not None
        ):
            raise ValueError("WhatsApp reactions require a complete exact message-owned collection")
        collection = WhatsAppReactionCollection.model_validate(record.payload)
        if collection.message_id != parent.native_id or collection.chat_id != parent.container_id:
            raise ValueError("WhatsApp reaction collection identity disagrees")
        return True
    if record.identity.record_type == "whatsapp_chat_participants":
        parent = record.parent
        if (
            record.payload_schema_version != 1
            or parent is None
            or parent.record_type != "whatsapp_chat"
            or parent.container_id is not None
            or record.identity.native_id != parent.native_id
            or record.identity.container_id != parent_container_key(parent)
            or record.blobs
            or record.completeness != "complete"
            or record.source_created_at is not None
            or record.source_updated_at is not None
        ):
            raise ValueError("WhatsApp participants require a complete exact chat-owned collection")
        collection = WhatsAppParticipantCollection.model_validate(record.payload)
        if collection.chat_id != parent.native_id:
            raise ValueError("WhatsApp participant collection identity disagrees")
        return True
    if record.identity.record_type == "whatsapp_chat":
        if (
            record.payload_schema_version != 1
            or record.identity.container_id is not None
            or record.parent is not None
        ):
            raise ValueError("WhatsApp chat requires a schema1 authority root")
        chat = WhatsAppChat.model_validate(record.payload)
        if chat.id != record.identity.native_id or chat.is_channel:
            raise ValueError("WhatsApp chat identity or supported scope disagrees")
        return True
    message = _captured_message(record)
    return _excluded_message(message)


async def map_whatsapp_message(
    record: SourceRecord, storage: StorageBackend, directory: Path
) -> ProjectionInputs:
    """Never fetch providers, infer transcripts, alter native IDs or recover restricted media."""
    message = _captured_message(record)
    if _excluded_message(message):
        return ProjectionInputs(parts=())
    timestamp = TypeAdapter(AwareDatetime).validate_python(message.timestamp)
    _validate_blobs(record, message)
    restricted = message.view_mode is not None
    parts = _text_parts(message, timestamp, restricted=restricted)
    quote = message.quoted
    attachments = [
        (item, f"/attachments/{index}") for index, item in enumerate(message.attachments)
    ]
    if quote is not None:
        attachments.extend(
            (item, f"/quoted/attachments/{index}") for index, item in enumerate(quote.attachments)
        )
    for attachment, path in attachments:
        descriptor = ExtractionPart(
            part_index=len(parts),
            key=path,
            kind="file",
            media_type=attachment.mimetype,
            extension=_suffix(attachment),
        )
        ref = next((blob for blob in record.blobs if blob.source_path == path), None)
        if restricted or attachment.is_unavailable or ref is None:
            if (
                not restricted
                and not attachment.is_unavailable
                and ref is None
                and path.startswith("/attachments/")
                and record.completeness == "complete"
            ):
                raise ValueError("Complete WhatsApp message lacks a retained attachment")
            parts.append(ProjectionInput(part=descriptor, entity=None))
            continue
        if attachment.type not in {"audio", "img", "video", "file"}:
            parts.append(
                ProjectionInput(part=descriptor, entity=None, omission="unsupported_format")
            )
            continue
        content = await read_blob(record, ref, storage)
        local = await write_blob(content, directory, suffix=descriptor.extension)
        entity = WhatsAppAttachmentEntity(
            attachment_key=_key(message.chat_id, message.id, path),
            filename=attachment.filename or attachment.id,
            attachment_id=attachment.id,
            message_id=message.id,
            chat_id=message.chat_id,
            source_path=path,
            attachment_type=attachment.type,
            voice_note=attachment.voice_note,
            sticker=attachment.sticker,
            url="",
            size=len(content),
            mime_type=attachment.mimetype,
            file_type=descriptor.extension.lstrip("."),
            local_path=str(local),
            breadcrumbs=[
                Breadcrumb(
                    entity_id=_key(message.chat_id, message.id, "/text"),
                    name="WhatsApp message",
                    entity_type="WhatsAppMessageEntity",
                )
            ],
        )
        populate_base_fields(entity)
        parts.append(ProjectionInput(part=descriptor, entity=entity))
    return ProjectionInputs(parts=tuple(parts))
