"""Offline rendering of complete, bounded native Notion property archives."""

from typing import Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    JsonValue,
    StrictBool,
    StrictInt,
    TypeAdapter,
)

from airweave.domains.entities.canonical.blob_materializer import read_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.entities.canonical.projection_mappers import ProjectionMappingError
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.entities.notion import NotionOriginalEntity

MAX_PROPERTY_PROJECTION_BYTES = 16 * 1024 * 1024


class _Header(BaseModel):
    model_config = ConfigDict(extra="forbid")
    format_version: Literal[1]
    page_id: UUID
    property_id: str = Field(min_length=1)
    notion_version: Literal["2026-03-11"]
    page_last_edited_time: AwareDatetime


class _Archive(_Header):
    responses: list[dict[str, JsonValue]] = Field(min_length=1)


class _Manifest(_Header):
    name: str
    property: dict[str, JsonValue]
    response_count: int = Field(ge=1)
    value_status: Literal["available", "unsupported"]


class _Native(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _Value(_Native):
    id: str
    type: str


class _Item(_Value):
    object: Literal["property_item"]


class _Status(_Native):
    type: Literal["complete"]


class _Page(_Native):
    object: Literal["list"]
    type: Literal["property_item"]
    results: list[dict[str, JsonValue]]
    property_item: dict[str, JsonValue]
    has_more: StrictBool
    next_cursor: str | None
    request_status: _Status | None = None


class _Label(_Native):
    name: str


class _Reference(_Native):
    id: UUID
    name: str | None = None


class _Date(_Native):
    start: str
    end: str | None = None
    time_zone: str | None = None


class _Rich(_Native):
    type: Literal["text", "mention", "equation"]
    plain_text: str


class _Unique(_Native):
    number: StrictInt | None
    prefix: str | None


class _Unsupported(Exception):
    """Known native values whose searchable interpretation is not implemented."""


_LIST_TYPES = {"title", "rich_text", "people", "relation", "rollup"}
_SUPPORTED = _LIST_TYPES | {
    "select",
    "status",
    "multi_select",
    "files",
    "created_by",
    "last_edited_by",
    "date",
    "checkbox",
    "boolean",
    "number",
    "url",
    "email",
    "phone_number",
    "string",
    "created_time",
    "last_edited_time",
    "unique_id",
    "formula",
}


def _known(kind: str) -> None:
    if kind not in _SUPPORTED:
        if kind in {"verification", "button", "place"}:
            raise _Unsupported
        raise ProjectionMappingError("Unknown Notion property value type")


def _render(kind: str, value: JsonValue) -> str:  # noqa: C901
    _known(kind)
    if value is None:
        if kind in {"number", "date", "select", "status", "url", "email", "phone_number", "string"}:
            return ""
        raise ProjectionMappingError("Notion property has an invalid null value")
    if kind in {"title", "rich_text"}:
        return _Rich.model_validate(value).plain_text
    if kind in {"select", "status"}:
        return _Label.model_validate(value).name
    if kind in {"multi_select", "files"}:
        return ", ".join(item.name for item in TypeAdapter(list[_Label]).validate_python(value))
    if kind in {"people", "relation", "created_by", "last_edited_by"}:
        ref = _Reference.model_validate(value)
        return f"{ref.name} ({ref.id})" if ref.name else str(ref.id)
    if kind == "date":
        date = _Date.model_validate(value)
        return (
            date.start
            + (f" – {date.end}" if date.end else "")
            + (f" ({date.time_zone})" if date.time_zone else "")
        )
    if kind in {"checkbox", "boolean"}:
        return "true" if TypeAdapter(StrictBool).validate_python(value) else "false"
    if kind == "number":
        return str(TypeAdapter(StrictInt | FiniteFloat).validate_python(value, strict=True))
    if kind in {"url", "email", "phone_number", "string", "created_time", "last_edited_time"}:
        return TypeAdapter(str).validate_python(value, strict=True)
    if kind == "unique_id":
        unique = _Unique.model_validate(value)
        return (f"{unique.prefix}-" if unique.prefix else "") + (
            str(unique.number) if unique.number is not None else ""
        )
    if kind in {"formula", "rollup"}:
        result = TypeAdapter(dict[str, JsonValue]).validate_python(value)
        subtype = TypeAdapter(str).validate_python(result.get("type"))
        if subtype == "incomplete":
            raise ProjectionMappingError("Notion property calculation is incomplete")
        if subtype == "unsupported":
            raise _Unsupported
        if subtype not in {"string", "number", "boolean", "date"}:
            raise _Unsupported
        if subtype not in result:
            raise ProjectionMappingError("Notion calculation result is missing")
        return _render(subtype, result[subtype])
    if kind in {"verification", "button", "place"}:
        raise _Unsupported
    raise ProjectionMappingError("Unknown Notion property value type")


def _pagination(page: _Page, last: bool, cursors: set[str]) -> None:
    if page.has_more == last or (page.next_cursor is None) != last:
        raise ProjectionMappingError("Notion property archive pagination is incomplete")
    if page.next_cursor is not None:
        if not page.next_cursor or page.next_cursor in cursors:
            raise ProjectionMappingError("Notion property archive repeats a cursor")
        cursors.add(page.next_cursor)
    if last and page.property_item.get("next_url") is not None:
        raise ProjectionMappingError("Notion terminal property retains continuation")


def _values(
    archive: _Archive, prop: _Value
) -> tuple[list[dict[str, JsonValue]], dict[str, JsonValue]]:
    values: list[dict[str, JsonValue]] = []
    metadata = {}
    cursors: set[str] = set()
    for index, raw in enumerate(archive.responses):
        last = index == len(archive.responses) - 1
        if raw.get("object") == "property_item":
            if len(archive.responses) != 1 or prop.type in _LIST_TYPES:
                raise ProjectionMappingError("Notion single property has multiple responses")
            item = _Item.model_validate(raw)
            metadata = raw
            values.append(raw)
        else:
            if prop.type not in _LIST_TYPES:
                raise ProjectionMappingError("Notion scalar property returned a list")
            page = _Page.model_validate(raw)
            metadata = page.property_item
            item = _Value.model_validate(metadata)
            _pagination(page, last, cursors)
            for raw_item in page.results:
                parsed = _Item.model_validate(raw_item)
                if prop.type != "rollup" and (parsed.id != prop.id or parsed.type != prop.type):
                    raise ProjectionMappingError("Notion property contains another identity")
            values.extend(page.results)
        if item.id != prop.id or item.type != prop.type:
            raise ProjectionMappingError("Notion property metadata identity mismatch")
    return values, metadata


def _text(archive: _Archive, prop: _Value, status: str) -> str:
    _known(prop.type)
    values, metadata = _values(archive, prop)
    if prop.type in {"formula", "rollup"}:
        calculation = TypeAdapter(dict[str, JsonValue]).validate_python(metadata.get(prop.type))
        subtype = calculation.get("type")
        if (subtype == "unsupported") != (status == "unsupported"):
            raise ProjectionMappingError("Notion property calculation status mismatch")
        if prop.type == "rollup" and subtype == "array":
            # show_original returns target values in results, not the metadata array.
            return "\n".join(
                _render(_Item.model_validate(v).type, v[_Item.model_validate(v).type])
                for v in values
            )
        return _render(prop.type, calculation)
    if status != "available":
        raise ProjectionMappingError("Notion non-calculation property has invalid status")
    rendered = [_render(prop.type, value[prop.type]) for value in values]
    return ("" if prop.type in {"title", "rich_text"} else "\n").join(rendered)


async def map_notion_property(record: SourceRecord, storage: StorageBackend) -> ProjectionInputs:
    """Validate retained authority and render values without fetching referenced objects."""
    manifest = _Manifest.model_validate(record.payload)
    prop = _Value.model_validate(manifest.property)
    parent = record.parent
    if (
        record.payload_schema_version != 2
        or parent is None
        or parent.record_type != "page"
        or parent.container_id is not None
        or parent.native_id != str(manifest.page_id)
        or record.identity.container_id != str(manifest.page_id)
        or record.identity.native_id != manifest.property_id
        or prop.id != manifest.property_id
        or len(record.blobs) != 1
    ):
        raise ProjectionMappingError("Notion property archive authority mismatch")
    blob = record.blobs[0]
    if (
        blob.media_type != "application/json"
        or blob.role is not None
        or blob.source_path is not None
    ):
        raise ProjectionMappingError("Notion property archive descriptor mismatch")
    if blob.size_bytes > MAX_PROPERTY_PROJECTION_BYTES:
        raise ProjectionMappingError("Notion property exceeds the 16 MiB projection budget")
    archive = _Archive.model_validate_json(await read_blob(record, blob, storage))
    header = tuple(_Header.model_fields)
    if (
        archive.model_dump(include=set(header)) != manifest.model_dump(include=set(header))
        or len(archive.responses) != manifest.response_count
    ):
        raise ProjectionMappingError("Notion property archive provenance mismatch")
    part = ExtractionPart(part_index=0, key=prop.id, kind="record", media_type="application/json")
    try:
        text = _text(archive, prop, manifest.value_status)
    except _Unsupported:
        return ProjectionInputs(
            parts=(ProjectionInput(part=part, entity=None, omission="unsupported_format"),)
        )
    entity = NotionOriginalEntity(
        native_id=prop.id,
        original_kind="page_property",
        title=manifest.name,
        text=text,
        breadcrumbs=[],
        native_url=None,
        created_at=None,
        updated_at=None,
        content_coverage="This property only. References and file names exclude linked bodies and "
        "file bytes. Related values are not a transactional snapshot.",
    )
    return ProjectionInputs(parts=(ProjectionInput(part=part, entity=entity),))
