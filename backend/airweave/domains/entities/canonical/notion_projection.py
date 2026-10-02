"""Offline, record-local Notion search text; no page assembly or provider access."""

from typing import Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    HttpUrl,
    JsonValue,
    StrictBool,
    TypeAdapter,
    field_validator,
)

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import ProjectionMappingError
from airweave.platform.entities.notion import NotionOriginalEntity


class _Native(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _RichText(_Native):
    type: Literal["text", "mention", "equation"]
    plain_text: str


class _Original(_Native):
    id: UUID
    object: Literal["page", "database", "data_source", "block"]
    created_time: AwareDatetime
    last_edited_time: AwareDatetime
    in_trash: StrictBool
    url: str | None = None

    @field_validator("url")
    @classmethod
    def native_url(cls, value: str | None) -> str | None:
        if value is not None:
            parsed = TypeAdapter(HttpUrl).validate_python(value)
            if (
                parsed.scheme != "https"
                or parsed.host not in {"app.notion.com", "notion.so", "www.notion.so"}
                or parsed.username is not None
                or parsed.password is not None
                or parsed.port not in (None, 443)
            ):
                raise ProjectionMappingError("Notion original URL is outside audited native hosts")
        return value


class _Property(_Native):
    type: str
    title: list[_RichText] | None = None


class _Page(_Original):
    properties: dict[str, _Property]


class _Database(_Original):
    title: list[_RichText]
    description: list[_RichText]


class _SchemaProperty(_Native):
    type: str


class _DataSource(_Database):
    properties: dict[str, _SchemaProperty]


class _Parent(_Native):
    type: Literal["page_id", "block_id"]
    page_id: UUID | None = None
    block_id: UUID | None = None


class _Block(_Original):
    type: str
    parent: _Parent
    has_children: StrictBool


class _Text(_Native):
    rich_text: list[_RichText]


class _Code(_Text):
    language: str
    caption: list[_RichText]


class _Todo(_Text):
    checked: StrictBool


class _TableRow(_Native):
    cells: list[list[_RichText]]


class _Reference(_Native):
    title: str


class _Equation(_Native):
    expression: str


class _Table(_Native):
    table_width: int
    has_column_header: StrictBool
    has_row_header: StrictBool


class _Synced(_Native):
    synced_from: dict[str, str] | None


def _text(items: list[_RichText]) -> str:
    # Native fragments include their own spacing. Mentions use only retained labels.
    return "".join(item.plain_text for item in items)


def _block_text(record: SourceRecord, block: _Block) -> tuple[str, str, str]:
    parent = record.parent
    native_parent = (
        block.parent.page_id if block.parent.type == "page_id" else block.parent.block_id
    )
    expected_kind = "page" if block.parent.type == "page_id" else "block"
    if (
        parent is None
        or parent.record_type != expected_kind
        or native_parent is None
        or parent.native_id != str(native_parent)
        or parent.container_id is not None
    ):
        raise ProjectionMappingError("Notion block parent differs from captured authority")
    kind = block.type
    content = record.payload.get(kind)
    coverage = "This block only; child blocks, comments and file contents are not included."
    if kind in {
        "paragraph",
        "heading_1",
        "heading_2",
        "heading_3",
        "bulleted_list_item",
        "numbered_list_item",
        "quote",
        "toggle",
        "callout",
    }:
        text = _text(_Text.model_validate(content).rich_text)
    elif kind == "to_do":
        todo = _Todo.model_validate(content)
        text = ("[x] " if todo.checked else "[ ] ") + _text(todo.rich_text)
    elif kind == "code":
        code = _Code.model_validate(content)
        text = "\n".join(
            part
            for part in (
                code.language,
                _text(code.rich_text),
                _text(code.caption),
            )
            if part
        )
    elif kind == "table_row":
        text = " | ".join(_text(cell) for cell in _TableRow.model_validate(content).cells)
    elif kind == "equation":
        text = _Equation.model_validate(content).expression
    else:
        return _structure(kind, content)
    title = text.splitlines()[0][:160] if text else kind.replace("_", " ").capitalize()
    return title, text, coverage


def _structure(kind: str, content: JsonValue) -> tuple[str, str, str]:
    """Render known structure/reference labels without inventing their body."""
    if kind in {"child_page", "child_database"}:
        label = _Reference.model_validate(content).title
        return (
            label or "Untitled reference",
            "",
            "Reference label only; linked content is separate.",
        )
    elif kind == "synced_block":
        synced = _Synced.model_validate(content)
        if synced.synced_from is not None:
            raise ProjectionMappingError(
                "Synced block reference content is not independently retained"
            )
        return "Synced block", "", "Container only; child blocks are separate."
    elif kind == "table":
        _Table.model_validate(content)
        return "Table", "", "Table container only; rows are separate."
    elif kind in {"column", "column_list", "divider", "table_of_contents"}:
        _Native.model_validate(content)
        return kind.replace("_", " ").capitalize(), "", "Structure only; no body text."
    else:
        raise ProjectionMappingError("Notion block type has no supported text projection")


def map_notion(record: SourceRecord) -> tuple[NotionOriginalEntity, ...]:
    """Map one retained native observation; missing/unknown content stays pending."""
    native = _Original.model_validate(record.payload)
    kind = record.identity.record_type
    if (
        str(native.id) != record.identity.native_id
        or native.object != kind
        or record.identity.container_id is not None
    ):
        raise ProjectionMappingError("Notion original identity differs from captured identity")
    if native.in_trash:
        raise ProjectionMappingError("Trashed Notion content cannot be projected")
    if kind == "block":
        title, text, coverage = _block_text(record, _Block.model_validate(record.payload))
    else:
        if record.parent is not None:
            raise ProjectionMappingError("Notion root has an unexpected capture parent")
        coverage = (
            "Metadata only; page body, full properties, comments "
            "and file contents are not included."
        )
        if kind == "page":
            page = _Page.model_validate(record.payload)
            titles = [prop.title for prop in page.properties.values() if prop.type == "title"]
            if len(titles) != 1 or titles[0] is None:
                raise ProjectionMappingError("Notion page requires its retained title property")
            title, text = _text(titles[0]), ""
        elif kind == "database":
            database = _Database.model_validate(record.payload)
            title, text = _text(database.title), _text(database.description)
        elif kind == "data_source":
            data = _DataSource.model_validate(record.payload)
            title = _text(data.title)
            schema = "\n".join(f"{name} ({prop.type})" for name, prop in data.properties.items())
            text = "\n".join(part for part in (_text(data.description), schema) if part)
        else:
            raise ProjectionMappingError("Unsupported Notion original kind")
        title = title or "Untitled " + kind.replace("_", " ")
    return (
        NotionOriginalEntity(
            native_id=record.identity.native_id,
            original_kind=kind,
            title=title,
            text=text,
            native_url=native.url,
            content_coverage=coverage,
            breadcrumbs=[],
            created_at=native.created_time,
            updated_at=native.last_edited_time,
        ),
    )
