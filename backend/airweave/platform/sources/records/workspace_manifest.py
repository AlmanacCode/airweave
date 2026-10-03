"""Versioned Docs capture provenance. Drive remains the sole record/access owner."""

import hashlib
import json
from typing import Annotated, Literal

from airweave.domains.entities.canonical.requests import BlobReference
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

Sha256 = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
DOCS_MIME = "application/vnd.google-apps.document"
MANIFEST_MIME = "application/vnd.almanac.workspace-manifest+json"


class ExportState(BaseModel):
    """A retained export or a specific supported acquisition limitation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["retained", "unavailable"]
    blob: Sha256 | None = None
    reason: (
        Literal["export_size_limit", "unsupported", "read_size_limit", "download_not_permitted"]
        | None
    ) = None

    @model_validator(mode="after")
    def coverage(self):
        """Require explicit, internally consistent representation coverage."""
        if self.status == "retained":
            if self.blob is None or self.reason is not None:
                raise ValueError("Retained export requires only a blob")
        elif self.blob is not None or self.reason is None:
            raise ValueError("Unavailable export requires only a reason")
        return self


class DocsGap(BaseModel):
    """An explicit missing portion of this acquisition policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_path: str | None = Field(default=None, pattern=r"^/")
    reason: Literal["read_size_limit", "unsupported_media", "capture_budget"]


class DocsMediaPart(BaseModel):
    """A native response location backed by retained immutable bytes."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_path: str = Field(pattern=r"^/")
    blob: Sha256


class DocsState(BaseModel):
    """Declared request policy and native response availability, not revision history."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["docs"] = "docs"
    status: Literal["complete", "partial", "unavailable"]
    document_blob: Sha256 | None = None
    include_tabs_content: Literal[True] = True
    suggestions_view: Literal["DEFAULT_FOR_CURRENT_ACCESS"] = "DEFAULT_FOR_CURRENT_ACCESS"
    comments_view: Literal["COMMENTS_VIEW_MODE_OMITTED"] = "COMMENTS_VIEW_MODE_OMITTED"
    embedded_media: Literal["not_retained", "retained", "partial"]
    media: tuple[DocsMediaPart, ...] = ()
    missing: tuple[DocsGap, ...] = ()

    @model_validator(mode="after")
    def coverage(self):
        """Require explicit, internally consistent representation coverage."""
        if (self.status == "unavailable") != (self.document_blob is None):
            raise ValueError("Docs response availability must match its blob")
        if self.status == "unavailable" and not self.missing:
            raise ValueError("Unavailable Docs response requires an explicit gap")
        if self.status == "complete" and any(g.reason != "unsupported_media" for g in self.missing):
            raise ValueError("Complete Docs fields cannot contain non-media gaps")
        if self.embedded_media == "not_retained" and self.media:
            raise ValueError("Unretained Docs media cannot reference bytes")
        if self.embedded_media == "retained" and any(
            g.reason == "unsupported_media" for g in self.missing
        ):
            raise ValueError("Retained Docs media cannot contain missing media")
        if len({p.source_path for p in self.media}) != len(self.media):
            raise ValueError("Docs media locations must be unique")
        return self


class WorkspaceManifestV1(BaseModel):
    """One file/version's supported representations; native Sheets is not implemented."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    file_id: str = Field(min_length=1)
    drive_version: str = Field(pattern=r"^[0-9]+$")
    export: ExportState
    native: DocsState


def canonical_json(value: JsonValue) -> bytes:
    """Stable JSON preserves all native values, independent of object key order."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def validate_document(document: dict[str, JsonValue], *, file_id: str) -> None:
    """Validate identity and all-tabs shape without dropping unknown native fields."""
    if document.get("documentId") != file_id:
        raise ValueError("Docs response belongs to another file")
    tabs = document.get("tabs")
    if not isinstance(tabs, list) or not tabs:
        raise ValueError("Docs response is missing all-tabs content")
    seen: set[str] = set()
    pending = list(tabs)
    while pending:
        tab = pending.pop()
        if not isinstance(tab, dict) or not isinstance(tab.get("documentTab"), dict):
            raise ValueError("Docs response has an unsupported tab structure")
        properties = tab.get("tabProperties")
        tab_id = properties.get("tabId") if isinstance(properties, dict) else None
        if not isinstance(tab_id, str) or not tab_id or tab_id in seen:
            raise ValueError("Docs tab identity is missing or duplicated")
        seen.add(tab_id)
        children = tab.get("childTabs", [])
        if not isinstance(children, list):
            raise ValueError("Docs child tabs are malformed")
        pending.extend(children)


def document_media_gaps(document: dict[str, JsonValue]) -> tuple[DocsGap, ...]:
    """Embedded object bytes are not downloaded through unaudited content URLs."""
    gaps = []
    pending: list[tuple[str, JsonValue]] = [("", document)]
    while pending:
        path, value = pending.pop()
        if isinstance(value, dict):
            for key, child in value.items():
                location = path + "/" + key.replace("~", "~0").replace("/", "~1")
                if key == "embeddedObject":
                    gaps.append(DocsGap(source_path=location, reason="unsupported_media"))
                else:
                    pending.append((location, child))
        elif isinstance(value, list):
            pending.extend((f"{path}/{index}", child) for index, child in enumerate(value))
    return tuple(sorted(gaps, key=lambda gap: gap.source_path or ""))


def parse_manifest(
    content: bytes, *, file_id: str, drive_version: str, blobs: tuple[BlobReference, ...]
) -> WorkspaceManifestV1:
    """Reject foreign/missing/recursive references; Workspace alone deduplicates by digest."""
    manifest = WorkspaceManifestV1.model_validate_json(content)
    if manifest.file_id != file_id or manifest.drive_version != drive_version:
        raise ValueError("Workspace manifest identity/version does not match its original")
    marked = [blob for blob in blobs if blob.role == "representation_manifest"]
    digest = hashlib.sha256(content).hexdigest()
    if len(marked) != 1 or marked[0].sha256 != digest or marked[0].size_bytes != len(content):
        raise ValueError("Workspace manifest is not the uniquely retained manifest")
    by_digest: dict[str, BlobReference] = {}
    for blob in blobs:
        if blob.sha256 in by_digest:
            raise ValueError("Workspace descriptors must be unambiguous by digest")
        if blob.source_path is not None:
            raise ValueError("Workspace parts cannot claim locations in native Drive metadata")
        by_digest[blob.sha256] = blob
    references = {part.blob for part in manifest.native.media}
    references.update(
        value for value in (manifest.export.blob, manifest.native.document_blob) if value
    )
    if digest in references or references != set(by_digest) - {digest}:
        raise ValueError("Workspace manifest does not exactly account for its retained parts")
    return manifest
