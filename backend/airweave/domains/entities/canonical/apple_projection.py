"""Offline native Apple text projection with explicit undecoded/unavailable parts."""

import base64
import mimetypes
import re
from pathlib import Path

from pydantic import JsonValue

from airweave.domains.entities.canonical.apple_payloads import (
    BinaryField,
    DeviceOriginalEnvelope,
    NativeMessage,
    NativeNote,
    NativeRow,
    NoteRow,
    native_integer,
    native_text,
)
from airweave.domains.entities.canonical.apple_preparation import (
    PreparationLimits,
    prepare_message_body,
    prepare_notes_body,
)
from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.storage.protocols import StorageBackend
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.entities.apple import AppleAttachmentEntity, AppleRecordEntity

# A single device page is bounded to 2MiB before admission. Expansion gets a separate
# ceiling; expensive or malformed content stays pending rather than truncating text.
_BODY_LIMITS = PreparationLimits(
    maximum_input_bytes=2 * 1024 * 1024,
    maximum_decompressed_bytes=16 * 1024 * 1024,
    maximum_text_bytes=8 * 1024 * 1024,
    cpu_seconds=5,
    wall_seconds=10,
    memory_bytes=256 * 1024 * 1024,
)


async def _attachments(
    record: SourceRecord,
    rows: tuple[NativeRow, ...] | tuple[NoteRow, ...],
    parts: list[ProjectionInput],
    storage: StorageBackend | None,
    directory: Path | None,
) -> None:
    """Native paths are labels only; read/materialize solely committed canonical references."""
    if len(record.blobs) > 64 or sum(blob.size_bytes for blob in record.blobs) > 64 * 1024 * 1024:
        raise ValueError("Apple attachments exceed admitted run byte budget")
    paths = {f"/original/attachments/{index}" for index in range(len(rows))}
    if any(blob.role is not None or blob.source_path not in paths for blob in record.blobs):
        raise ValueError("Apple blob does not identify an original attachment")
    for index, row in enumerate(rows):
        fields = row.fields
        identity = (
            native_text(fields, "guid")
            if isinstance(row, NativeRow)
            else native_text(fields, "ZIDENTIFIER")
        )
        identity = (
            identity or f"row:{row.row_id if isinstance(row, NativeRow) else row.primary_key}"
        )
        refs = [
            blob for blob in record.blobs if blob.source_path == f"/original/attachments/{index}"
        ]
        if len(refs) > 1:
            raise ValueError("Apple attachment has ambiguous original references")
        filename = (
            native_text(fields, "transfer_name")
            or native_text(fields, "filename")
            or native_text(fields, "ZFILENAME")
            or identity
        )
        filename = filename.replace("\\", "/").rsplit("/", 1)[-1] or identity
        media_type = refs[0].media_type if refs else native_text(fields, "mime_type")
        suffix = Path(filename).suffix.lower()
        if not re.fullmatch(r"\.[a-z0-9]{1,12}", suffix):
            suffix = mimetypes.guess_extension(media_type or "") or ".bin"
        descriptor = ExtractionPart(
            part_index=len(parts),
            key="attachment:" + identity,
            kind="file",
            media_type=media_type,
            extension=suffix,
        )
        if not refs:
            parts.append(ProjectionInput(part=descriptor, entity=None))
            continue
        if storage is None or directory is None:
            raise ValueError("Retained Apple attachments require scoped projection storage")
        content = await read_blob(record, refs[0], storage)
        local_path = await write_blob(content, directory, suffix=suffix)
        entity = AppleAttachmentEntity(
            attachment_key=f"{record.id}:{identity}",
            filename=filename,
            breadcrumbs=[],
            url="",
            size=len(content),
            mime_type=media_type,
            file_type=suffix.lstrip("."),
            local_path=str(local_path),
        )
        populate_base_fields(entity)
        parts.append(ProjectionInput(part=descriptor, entity=entity))


def _body(record: SourceRecord, title: str, text: str) -> ProjectionInput:
    entity = AppleRecordEntity(
        native_id=record.identity.native_id,
        breadcrumbs=[],
        title=title,
        content=text,
        created_at=record.source_created_at,
        modified_at=record.source_updated_at,
    )
    populate_base_fields(entity)
    return ProjectionInput(
        part=ExtractionPart(part_index=0, key="body", kind="body"),
        entity=entity,
        native_body=NativeTextBody(text=text, metadata_fields=("content",)),
    )


def _gap(index: int, key: str, *, retained: bool = False) -> ProjectionInput:
    return ProjectionInput(
        part=ExtractionPart(part_index=index, key=key, kind="file"),
        entity=None,
        omission="unsupported_format" if retained else None,
    )


async def _message_parts(
    record: SourceRecord,
    original: dict[str, JsonValue],
    storage: StorageBackend | None,
    directory: Path | None,
) -> list[ProjectionInput]:
    parts: list[ProjectionInput] = []
    message = NativeMessage.model_validate(original)
    if message.guid != record.identity.native_id:
        raise ValueError("Retained Messages identity differs from original")
    text = native_text(message.message.fields, "text")
    attributed = message.message.fields.get("attributedBody")
    if attributed is not None and isinstance(attributed.root, BinaryField):
        prepared = await prepare_message_body(attributed.root.blob.bytes(), limits=_BODY_LIMITS)
        text = prepared.text
    if text is not None:
        parts.append(_body(record, "Message", text))
    if "attributedBodyUndecoded" in message.body_fidelity:
        # Acquisition fidelity remains unchanged; this is a separately prepared view.
        parts.append(_gap(len(parts), "rich-message-content", retained=True))
    elif text is None:
        parts.append(_gap(len(parts), "body"))
    await _attachments(record, message.attachments, parts, storage, directory)
    return parts


async def _note_parts(
    record: SourceRecord,
    original: dict[str, JsonValue],
    storage: StorageBackend | None,
    directory: Path | None,
) -> list[ProjectionInput]:
    parts: list[ProjectionInput] = []
    note = NativeNote.model_validate(original)
    if note.native_id != record.identity.native_id:
        raise ValueError("Retained Notes identity differs from original")
    if native_integer(note.note.fields, "ZISPASSWORDPROTECTED") or native_integer(
        note.note.fields, "ZMARKEDFORDELETION"
    ):
        raise ValueError("Locked or removed Notes content cannot be projected")
    if note.compressed_body is None:
        parts.append(_gap(0, "body"))
    else:
        prepared = await prepare_notes_body(
            base64.b64decode(note.compressed_body, validate=True),
            locked=False,
            limits=_BODY_LIMITS,
        )
        parts.append(
            _body(record, native_text(note.note.fields, "ZTITLE1") or "Note", prepared.text)
        )
        # Text extraction is useful, but must not report complete rich-note coverage.
        parts.append(_gap(len(parts), "rich-note-content", retained=True))
    await _attachments(record, note.attachments, parts, storage, directory)
    return parts


def _contact_parts(record: SourceRecord) -> list[ProjectionInput]:
    from airweave.domains.entities.canonical.contact_preparation import prepare_contact

    prepared = prepare_contact(record)
    body = _body(record, prepared.title, prepared.text)
    return [
        body.model_copy(
            update={
                "native_body": NativeTextBody(
                    text=prepared.text,
                    kind="extracted_text",
                    preparation=prepared.preparation,
                    metadata_fields=("content",),
                )
            }
        )
    ]


async def map_apple(
    record: SourceRecord,
    source_name: str,
    storage: StorageBackend | None = None,
    directory: Path | None = None,
) -> ProjectionInputs:
    """Derive views only from retained payloads; never fetch a provider or native path."""
    if record.deleted_at is not None or record.content_access != "available":
        raise ValueError("Unavailable Apple records cannot be projected")
    envelope = DeviceOriginalEnvelope.model_validate(record.payload)
    if envelope.source_kind != source_name:
        raise ValueError("Apple source kind differs from admitted envelope")
    if source_name == "imessage" and record.identity.record_type == "imessage_message":
        parts = await _message_parts(record, envelope.original, storage, directory)
    elif source_name == "apple_notes" and record.identity.record_type == "apple_note":
        parts = await _note_parts(record, envelope.original, storage, directory)
    elif source_name == "apple_contacts" and record.identity.record_type == "apple_contact":
        if record.blobs:
            raise ValueError("Contacts originals do not admit attachments")
        parts = _contact_parts(record)
    else:
        raise ValueError("Unsupported Apple projection source or record type")
    if len({part.part.key for part in parts}) != len(parts):
        raise ValueError("Duplicate native projection parts")
    return ProjectionInputs(parts=tuple(parts))
