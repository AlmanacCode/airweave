"""Export-only acquisition evidence under the existing Drive blob authority."""

import hashlib
from typing import Literal

from airweave.domains.entities.canonical.requests import BlobReference
from airweave.platform.sources.records.workspace_manifest import ExportState
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExportManifestV3(BaseModel):
    """A file version's export coverage, without invented native document structure."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[3] = 3
    file_id: str = Field(min_length=1)
    drive_version: str = Field(pattern=r"^[0-9]+$")
    media_type: str = Field(min_length=1)
    export: ExportState

    @model_validator(mode="after")
    def omission_only(self):
        """Available export-only bytes keep the existing single-blob contract."""
        if self.export.status != "unavailable":
            raise ValueError("Export omission manifest requires an unavailable representation")
        return self


def parse_export_manifest(
    content: bytes,
    *,
    file_id: str,
    drive_version: str,
    media_type: str,
    blobs: tuple[BlobReference, ...],
) -> ExportManifestV3:
    """Bind coverage to exact metadata and every retained immutable descriptor."""
    manifest = ExportManifestV3.model_validate_json(content)
    if (manifest.file_id, manifest.drive_version, manifest.media_type) != (
        file_id,
        drive_version,
        media_type,
    ):
        raise ValueError("Export manifest identity/version/type mismatch")
    digest = hashlib.sha256(content).hexdigest()
    marked = [blob for blob in blobs if blob.role == "representation_manifest"]
    if len(marked) != 1 or (marked[0].sha256, marked[0].size_bytes) != (digest, len(content)):
        raise ValueError("Export manifest is not the uniquely retained manifest")
    by_digest = {blob.sha256: blob for blob in blobs}
    if len(by_digest) != len(blobs) or any(blob.source_path is not None for blob in blobs):
        raise ValueError("Export descriptors are ambiguous")
    refs = {manifest.export.blob} if manifest.export.blob else set()
    if digest in refs or refs != set(by_digest) - {digest}:
        raise ValueError("Export manifest does not account for every retained part")
    return manifest
