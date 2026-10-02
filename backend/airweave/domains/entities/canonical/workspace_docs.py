"""Read retained native Docs content; no provider access or independent identity."""

from __future__ import annotations

import json

from pydantic import BaseModel, ConfigDict, JsonValue

from airweave.domains.entities.canonical.blob_materializer import read_blob
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.sources.records.workspace_manifest import (
    WorkspaceManifestV1,
    parse_manifest,
    validate_document,
)


class DocumentReadError(ValueError):
    """The retained document cannot supply the requested representation."""


class CapturedDocument(BaseModel):
    """Original native structure with validated capture provenance, never reconstructed JSON."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    manifest: WorkspaceManifestV1
    document: dict[str, JsonValue]

    def tab(self, tab_id: str) -> dict[str, JsonValue]:
        """Return the unchanged native subtree for one exact captured tab ID."""
        for tab in _tabs(self.document["tabs"]):
            properties = _object(tab.get("tabProperties"))
            if properties.get("tabId") == tab_id:
                return tab
        raise DocumentReadError("Requested tab is not present in this captured document")

    def text(self) -> str:
        """Project supported authored text across all tabs and ancillary content."""
        if self.manifest.native.status != "complete":
            raise DocumentReadError("Native document text capture is incomplete")
        sections = []
        for tab in _tabs(self.document["tabs"]):
            properties = _object(tab.get("tabProperties"))
            title = properties.get("title")
            if isinstance(title, str) and title:
                sections.append(title + "\n")
            document = _object(tab.get("documentTab"))
            sections.append(_content(_object(document.get("body")).get("content", [])))
            for field in ("headers", "footers", "footnotes"):
                for value in _object(document.get(field, {})).values():
                    sections.append(_content(_object(value).get("content", [])))
        return "\n".join(part for part in sections if part)


def _object(value: JsonValue) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise DocumentReadError("Captured Docs structure requires an object")
    return value


def _array(value: JsonValue) -> list[JsonValue]:
    if not isinstance(value, list):
        raise DocumentReadError("Captured Docs structure requires an array")
    return value


def _tabs(value: JsonValue):
    for item in _array(value):
        tab = _object(item)
        yield tab
        yield from _tabs(tab.get("childTabs", []))


def _content(value: JsonValue) -> str:
    sections = []
    for item in _array(value):
        element = _object(item)
        if "paragraph" in element:
            sections.append(_paragraph(_object(element["paragraph"])))
        elif "table" in element:
            table = _object(element["table"])
            for row in _array(table.get("tableRows", [])):
                cells = _array(_object(row).get("tableCells", []))
                sections.append("\t".join(_content(_object(c).get("content", [])) for c in cells))
        elif "tableOfContents" in element:
            sections.append(_content(_object(element["tableOfContents"]).get("content", [])))
        elif "sectionBreak" not in element:
            raise DocumentReadError("Unsupported captured Docs structural element")
    return "\n".join(sections)


def _paragraph(paragraph: dict[str, JsonValue]) -> str:
    pieces = []
    for item in _array(paragraph.get("elements", [])):
        element = _object(item)
        if "textRun" in element:
            text = _object(element["textRun"]).get("content")
            if not isinstance(text, str):
                raise DocumentReadError("Captured Docs text run has no text")
            pieces.append(text)
        elif "person" in element:
            properties = _object(_object(element["person"]).get("personProperties"))
            pieces.append(_display_text(properties.get("name") or properties.get("email")))
        elif "richLink" in element:
            properties = _object(_object(element["richLink"]).get("richLinkProperties"))
            pieces.append(_display_text(properties.get("title")))
        elif "equation" in element:
            raise DocumentReadError("Google Docs does not expose equation text for projection")
        elif any(
            key in element
            for key in (
                "inlineObjectElement",
                "pageBreak",
                "columnBreak",
                "footnoteReference",
                "horizontalRule",
                "autoText",
            )
        ):
            # Object bytes and automatic page numbers are not authored paragraph text.
            # Footnote text is included separately from its documentTab collection.
            continue
        else:
            raise DocumentReadError("Unsupported captured Docs paragraph element")
    return "".join(pieces)


def _display_text(value: JsonValue) -> str:
    if not isinstance(value, str) or not value:
        raise DocumentReadError("Captured Docs smart chip has no display text")
    return value


async def read_document(record: SourceRecord, storage: StorageBackend) -> CapturedDocument:
    """Resolve only the current record's committed manifest and document bytes."""
    if record.content_access != "available" or record.deleted_at is not None:
        raise DocumentReadError("Captured document is unavailable")
    if record.identity.record_type != "file" or record.payload.get("mimeType") != (
        "application/vnd.google-apps.document"
    ):
        raise DocumentReadError("Captured file is not a Google document")
    manifests = [blob for blob in record.blobs if blob.role == "representation_manifest"]
    if len(manifests) != 1:
        raise DocumentReadError("Captured document requires one representation manifest")
    version = record.payload.get("version")
    if not isinstance(version, str):
        raise DocumentReadError("Captured document has no Drive version")
    manifest = parse_manifest(
        await read_blob(record, manifests[0], storage),
        file_id=record.identity.native_id,
        drive_version=version,
        blobs=record.blobs,
    )
    digest = manifest.native.document_blob
    if digest is None:
        raise DocumentReadError("Native document content was not captured")
    matches = [blob for blob in record.blobs if blob.sha256 == digest]
    if len(matches) != 1:
        raise DocumentReadError("Native document blob reference is ambiguous or absent")
    document = json.loads(await read_blob(record, matches[0], storage))
    if not isinstance(document, dict):
        raise DocumentReadError("Native document response is not an object")
    validate_document(document, file_id=record.identity.native_id)
    return CapturedDocument(manifest=manifest, document=document)
