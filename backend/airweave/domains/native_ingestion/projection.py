"""Offline views of original Almanac records, sessions and protocol messages."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StrictBool

from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.native_ingestion.knowledge_fields import knowledge_details
from airweave.domains.native_ingestion.models import NativeSnapshot
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.entities._airweave_field import AirweaveField
from airweave.platform.entities._base import BaseEntity


class _Original(BaseModel):
    """Read only declared projection fields; all other original JSON stays retained."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(min_length=1)
    created_at: AwareDatetime


class _Knowledge(_Original):
    type: Literal[
        "person",
        "organisation",
        "place",
        "event",
        "creative_work",
        "topic",
        "page",
        "task",
        "project",
    ]
    revision: int = Field(strict=True, ge=1)
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    path: str = Field(min_length=1)
    body: str
    updated_at: AwareDatetime
    archived_at: AwareDatetime | None
    user_notes: str | None = None


class _Session(_Original):
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    revision: int = Field(strict=True, ge=1)
    content_revision: int = Field(strict=True, ge=0)
    archived: StrictBool
    updated_at: AwareDatetime


class _ImportProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid")
    legacy_row_id: int = Field(strict=True, gt=0)
    active: StrictBool
    compacted: StrictBool


class _TextBlock(BaseModel):
    model_config = ConfigDict(extra="ignore")
    type: Literal["text"]
    text: str


class _Block(BaseModel):
    model_config = ConfigDict(extra="allow")
    type: str = Field(min_length=1)


class _Payload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    role: Literal["user", "assistant", "tool", "system"]
    content: str | list[dict[str, JsonValue]] | None


class _Message(_Original):
    ordinal: int = Field(strict=True, ge=0)
    import_provenance: _ImportProvenance | None = None
    payload: _Payload


class NativeContentEntity(BaseEntity):
    """Explicit searchable metadata; original payloads never enter entity serialization."""

    native_key: str = AirweaveField(..., is_entity_id=True, embeddable=False)
    title: str = AirweaveField(..., is_name=True, embeddable=True)
    description: str | None = AirweaveField(None, embeddable=True)
    user_notes: str | None = AirweaveField(None, embeddable=True)
    details: str | None = AirweaveField(None, embeddable=True)
    native_type: str = AirweaveField(..., embeddable=False)
    path: str | None = AirweaveField(None, embeddable=False)
    session_id: str | None = AirweaveField(None, embeddable=False)
    message_id: str | None = AirweaveField(None, embeddable=False)
    message_ordinal: int | None = AirweaveField(None, embeddable=False)
    block_index: int | None = AirweaveField(None, embeddable=False)
    role: str | None = AirweaveField(None, embeddable=False)
    archived: bool | None = AirweaveField(None, embeddable=False)


def _read(record: SourceRecord) -> tuple[NativeSnapshot, _Knowledge | _Session | _Message]:
    snapshot = NativeSnapshot.model_validate(record.payload)
    if snapshot.identity != record.identity or snapshot.parent != record.parent:
        raise ValueError("Native snapshot identity differs from retained record")
    if snapshot.operation != "upsert":
        raise ValueError("Native tombstone cannot project content")
    match snapshot.identity.record_type:
        case "knowledge":
            original = _Knowledge.model_validate(snapshot.original)
            if snapshot.version.kind != "record" or original.revision != snapshot.version.revision:
                raise ValueError("Knowledge version differs from original")
        case "session":
            original = _Session.model_validate(snapshot.original)
            if (
                snapshot.version.kind != "session"
                or original.revision != snapshot.version.revision
                or original.content_revision != snapshot.version.content_revision
            ):
                raise ValueError("Session version differs from original")
        case "message":
            original = _Message.model_validate(snapshot.original)
        case _:
            raise ValueError("Unsupported native projection type")
    _validate_identity(record, snapshot, original)
    return snapshot, original


def _validate_identity(
    record: SourceRecord, snapshot: NativeSnapshot, original: _Knowledge | _Session | _Message
) -> None:
    if original.id != snapshot.identity.native_id:
        raise ValueError("Native original ID differs from snapshot")
    if (
        original.created_at != record.source_created_at
        or original.created_at != snapshot.source_created_at
    ):
        raise ValueError("Native original creation timestamp differs from snapshot")
    if isinstance(original, (_Knowledge, _Session)) and (
        original.updated_at != record.source_updated_at
        or original.updated_at != snapshot.source_updated_at
    ):
        raise ValueError("Native original update timestamp differs from snapshot")


def excluded_native(record: SourceRecord) -> bool:
    """Follow current knowledge and session search visibility policy."""
    _, original = _read(record)
    if isinstance(original, _Knowledge):
        return original.archived_at is not None
    return (
        isinstance(original, _Message)
        and original.import_provenance is not None
        and not original.import_provenance.active
    )


def _part(index: int, key: str, entity: NativeContentEntity, text: str) -> ProjectionInput:
    populate_base_fields(entity)
    return ProjectionInput(
        part=ExtractionPart(part_index=index, key=key, kind="body", media_type="text/plain"),
        entity=entity,
        native_body=NativeTextBody(text=text),
    )


def map_native(record: SourceRecord) -> ProjectionInputs:
    """Select verbatim bodies and individual text blocks without flattening protocol JSON."""
    snapshot, original = _read(record)
    key = snapshot.identity.native_id
    common = {
        "created_at": record.source_created_at,
        "updated_at": record.source_updated_at,
        "breadcrumbs": [],
    }
    if isinstance(original, _Knowledge):
        entity = NativeContentEntity(
            native_key=key,
            title=original.title,
            description=original.description,
            user_notes=original.user_notes,
            details=knowledge_details(original.type, snapshot.original),
            path=original.path,
            native_type=original.type,
            **common,
        )
        return ProjectionInputs(parts=(_part(0, key, entity, original.body),))
    if isinstance(original, _Session):
        entity = NativeContentEntity(
            native_key=key,
            title=original.title,
            native_type="session",
            session_id=key,
            archived=original.archived,
            **common,
        )
        return ProjectionInputs(parts=(_part(0, key, entity, original.description),))
    content = original.payload.content
    blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content
    # Null or an empty list is retained nontext protocol content, not invented text.
    blocks = blocks or [{"type": "nontext"}]
    parts = []
    for index, block in enumerate(blocks):
        kind = _Block.model_validate(block).type
        part_key = f"{snapshot.identity.entity_key}:block:{index}"
        if kind != "text":
            parts.append(
                ProjectionInput(
                    part=ExtractionPart(part_index=index, key=part_key, kind="record"),
                    entity=None,
                    omission="unsupported_format",
                )
            )
            continue
        text = _TextBlock.model_validate(block).text
        entity = NativeContentEntity(
            native_key=part_key,
            title=f"{original.payload.role} message",
            native_type="message",
            session_id=snapshot.identity.container_id,
            message_id=original.id,
            message_ordinal=original.ordinal,
            block_index=index,
            role=original.payload.role,
            **common,
        )
        parts.append(_part(index, part_key, entity, text))
    return ProjectionInputs(parts=tuple(parts))
