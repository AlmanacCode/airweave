"""Versioned native spreadsheet coverage under a single Drive file identity."""

import hashlib
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from airweave.domains.entities.canonical.requests import BlobReference
from airweave.platform.sources.records.workspace_manifest import ExportState, Sha256

SHEETS_MIME = "application/vnd.google-apps.spreadsheet"


class GridBounds(BaseModel):
    """Zero-based, exclusive bounds; missing native cells inside a read range are blank."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sheet_id: int = Field(ge=0)
    start_row: int = Field(ge=0)
    end_row: int = Field(gt=0)
    start_column: int = Field(ge=0)
    end_column: int = Field(gt=0)

    @model_validator(mode="after")
    def nonempty(self):
        """Reject reversed or empty native ranges."""
        if self.end_row <= self.start_row or self.end_column <= self.start_column:
            raise ValueError("Grid bounds must be nonempty")
        return self


class GridPart(BaseModel):
    """One successfully retained response for a precise requested rectangle."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    bounds: GridBounds
    blob: Sha256


class GridGap(BaseModel):
    """An explicitly unretained range or unsupported native sheet."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sheet_id: int = Field(ge=0)
    bounds: GridBounds | None = None
    reason: Literal["capture_budget", "read_size_limit", "unsupported_sheet_type"]

    @model_validator(mode="after")
    def identity(self):
        """Bind a missing range to its stated sheet."""
        if self.reason == "unsupported_sheet_type":
            if self.bounds is not None:
                raise ValueError("Unsupported sheet has no grid range")
        elif self.bounds is None or self.bounds.sheet_id != self.sheet_id:
            raise ValueError("Missing range must belong to its sheet")
        return self


class SheetsState(BaseModel):
    """Structured coverage independent of exported-file availability."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["sheets"] = "sheets"
    status: Literal["complete", "partial"]
    metadata_blob: Sha256
    parts: tuple[GridPart, ...] = ()
    missing: tuple[GridGap, ...] = ()
    comments: Literal["omitted"] = "omitted"
    embedded_media: Literal["not_retained"] = "not_retained"
    calculated_values: Literal["observed_at_capture"] = "observed_at_capture"

    @model_validator(mode="after")
    def coverage(self):
        """Require explicit gaps for every partial capture."""
        if (self.status == "partial") != bool(self.missing):
            raise ValueError("Grid status must agree with missing ranges")
        return self


class WorkspaceManifestV2(BaseModel):
    """Sheets extension; immutable Docs v1 and export-only originals remain readable."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[2] = 2
    file_id: str = Field(min_length=1)
    drive_version: str = Field(pattern=r"^[0-9]+$")
    export: ExportState
    native: SheetsState


def parse_sheet_manifest(
    content: bytes, *, file_id: str, drive_version: str, blobs: tuple[BlobReference, ...]
) -> WorkspaceManifestV2:
    """All semantic descriptors must resolve to exactly the owning record's bytes."""
    manifest = WorkspaceManifestV2.model_validate_json(content)
    if (manifest.file_id, manifest.drive_version) != (file_id, drive_version):
        raise ValueError("Spreadsheet manifest identity/version mismatch")
    marked = [blob for blob in blobs if blob.role == "representation_manifest"]
    digest = hashlib.sha256(content).hexdigest()
    if len(marked) != 1 or (marked[0].sha256, marked[0].size_bytes) != (digest, len(content)):
        raise ValueError("Spreadsheet manifest is not the uniquely retained manifest")
    by_digest = {blob.sha256: blob for blob in blobs}
    if len(by_digest) != len(blobs) or any(blob.source_path is not None for blob in blobs):
        raise ValueError("Spreadsheet descriptors are ambiguous")
    refs = {manifest.native.metadata_blob, *(part.blob for part in manifest.native.parts)}
    if manifest.export.blob:
        refs.add(manifest.export.blob)
    if digest in refs or refs != set(by_digest) - {digest}:
        raise ValueError("Spreadsheet manifest does not account for every retained part")
    return manifest
