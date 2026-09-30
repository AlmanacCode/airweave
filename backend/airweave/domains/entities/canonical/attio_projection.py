"""Offline search views of Attio originals; membership never owns CRM record content."""

import json
from uuid import UUID

from pydantic import BaseModel, ConfigDict, HttpUrl, JsonValue

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import ProjectionMappingError
from airweave.platform.entities.attio import AttioOriginalEntity


class _Native(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _Identity(_Native):
    workspace_id: UUID
    object_id: UUID | None = None
    record_id: UUID | None = None
    list_id: UUID | None = None
    entry_id: UUID | None = None
    note_id: UUID | None = None


class _Original(_Native):
    id: _Identity


class _Object(_Original):
    singular_noun: str
    plural_noun: str
    api_slug: str


class _List(_Original):
    name: str


class _Record(_Original):
    web_url: HttpUrl | None = None
    values: dict[str, JsonValue]


class _Entry(_Original):
    parent_record_id: UUID
    parent_object: str
    entry_values: dict[str, JsonValue]


class _Note(_Original):
    title: str
    parent_record_id: UUID
    parent_object: str
    content_plaintext: str
    content_markdown: str


class _Name(_Native):
    full_name: str | None = None
    value: str | None = None


def _attributes(values: dict[str, JsonValue]) -> str:
    # Arbitrary custom attributes are data, not guessed standard properties. Preserve
    # their names, values and references without fetching or joining other originals.
    return json.dumps(values, ensure_ascii=False, sort_keys=True, indent=2)


def _parent(record: SourceRecord, kind: str, native_id: UUID | None) -> None:
    if (
        native_id is None
        or record.identity.container_id != str(native_id)
        or record.parent is None
        or record.parent.record_type != kind
        or record.parent.native_id != str(native_id)
        or (kind in {"object", "list"} and record.parent.container_id is not None)
    ):
        raise ProjectionMappingError("Attio parent differs from captured identity")


def _url(url: HttpUrl | None) -> str | None:
    if url is None:
        return None
    if (
        url.scheme != "https"
        or url.host != "app.attio.com"
        or url.port not in (None, 443)
        or url.username
        or url.password
    ):
        raise ProjectionMappingError("Attio URL is outside the native application host")
    return str(url)


def _record_title(original: _Record, native_id: UUID) -> str:
    title = f"Record {native_id}"
    names = original.values.get("name")
    if isinstance(names, list) and names:
        name = _Name.model_validate(names[0])
        title = name.full_name or name.value or title
    return title


def map_attio(record: SourceRecord) -> tuple[AttioOriginalEntity, ...]:
    """Render each native identity separately, without inventing URLs or related content."""
    native = _Original.model_validate(record.payload)
    kind = record.identity.record_type
    native_url = None
    ids = native.id
    native_id = {
        "object": ids.object_id,
        "record": ids.record_id,
        "list": ids.list_id,
        "entry": ids.entry_id,
        "note": ids.note_id,
    }.get(kind)
    if native_id is None or str(native_id) != record.identity.native_id:
        raise ProjectionMappingError("Attio native identity differs from captured identity")
    if kind in {"object", "list"}:
        if record.parent is not None or record.identity.container_id is not None:
            raise ProjectionMappingError("Attio root has an unexpected capture parent")
        if kind == "object":
            obj = _Object.model_validate(record.payload)
            title, text = obj.plural_noun, f"{obj.singular_noun}\n{obj.api_slug}"
        else:
            title, text = _List.model_validate(record.payload).name, ""
        coverage = "Container metadata only; records and memberships are separate originals."
    elif kind == "record":
        _parent(record, "object", ids.object_id)
        original = _Record.model_validate(record.payload)
        native_url = _url(original.web_url)
        title = _record_title(original, native_id)
        text = _attributes(original.values)
        coverage = "Retained attributes only; notes, memberships and file contents are separate."
    elif kind == "entry":
        _parent(record, "list", ids.list_id)
        entry = _Entry.model_validate(record.payload)
        title = f"List entry {native_id}"
        text = f"Referenced {entry.parent_object} record: {entry.parent_record_id}\n"
        text += _attributes(entry.entry_values)
        coverage = "List membership and its attributes only; referenced CRM record is separate."
    elif kind == "note":
        note = _Note.model_validate(record.payload)
        _parent(record, "record", note.parent_record_id)
        title, text = note.title, note.content_plaintext
        coverage = "Retained note text only; embedded media bytes are not included."
    else:
        raise ProjectionMappingError("Unsupported Attio original kind")
    return (
        AttioOriginalEntity(
            native_id=str(native_id),
            original_kind=kind,
            title=title or kind.capitalize(),
            text=text,
            content_coverage=coverage,
            native_url=native_url,
            breadcrumbs=[],
            created_at=record.source_created_at,
            updated_at=record.source_updated_at,
        ),
    )
