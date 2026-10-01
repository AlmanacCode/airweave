"""Offline Slack file inputs from retained message JSON and account-owned blobs."""

import mimetypes
import re
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.storage.protocols import StorageBackend
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.entities._base import BaseEntity, Breadcrumb
from airweave.platform.entities.slack import SlackAttachmentEntity


class SlackFile(BaseModel):
    """Only projection fields are parsed; the retained native payload stays unchanged."""

    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(pattern=r"^F[A-Z0-9]+$", max_length=128)
    name: str | None = Field(default=None, max_length=4096)
    mimetype: str | None = Field(default=None, max_length=256)


class SlackFiles(BaseModel):
    """A missing file list means no attachments; malformed lists never mean empty."""

    model_config = ConfigDict(extra="ignore", strict=True)
    files: list[SlackFile] = Field(default_factory=list)


async def map_slack_files(
    record: SourceRecord, body: BaseEntity, storage: StorageBackend, directory: Path
) -> ProjectionInputs:
    """Retain one body and one expected part per file, including uncaptured originals."""
    try:
        files = SlackFiles.model_validate(record.payload).files
    except ValidationError:
        raise ValueError("Slack file metadata is malformed") from None
    if len({file.id for file in files}) != len(files):
        raise ValueError("Slack message contains duplicate native file identities")
    paths = {f"/files/{index}" for index in range(len(files))}
    if any(blob.role is None and blob.source_path not in paths for blob in record.blobs):
        raise ValueError("Slack blob does not identify a current message file")
    populate_base_fields(body)
    parts = [
        ProjectionInput(part=ExtractionPart(part_index=0, key="body", kind="body"), entity=body)
    ]
    for index, file in enumerate(files):
        filename = file.name or file.id
        suffix = Path(filename).suffix.lower()
        if not re.fullmatch(r"\.[a-z0-9]{1,12}", suffix):
            suffix = mimetypes.guess_extension(file.mimetype or "") or ".bin"
        descriptor = ExtractionPart(
            part_index=index + 1,
            key=f"file:{file.id}",
            kind="file",
            media_type=file.mimetype,
            extension=suffix,
        )
        references = [blob for blob in record.blobs if blob.source_path == f"/files/{index}"]
        if len(references) > 1:
            raise ValueError("Slack file has ambiguous original blob references")
        if not references:
            if record.completeness == "complete":
                raise ValueError("Complete Slack message lacks a retained file")
            parts.append(ProjectionInput(part=descriptor, entity=None))
            continue
        content = await read_blob(record, references[0], storage)
        path = await write_blob(content, directory, suffix=suffix)
        entity = SlackAttachmentEntity(
            attachment_key=f"{record.identity.container_id}:{record.identity.native_id}:{file.id}",
            filename=filename,
            breadcrumbs=[
                Breadcrumb(
                    entity_id=body.entity_id, name="Slack message", entity_type=type(body).__name__
                )
            ],
            # Projection never dereferences URLs; the original is identified by its parent.
            url="",
            size=len(content),
            file_type=suffix.lstrip("."),
            mime_type=file.mimetype,
            local_path=str(path),
        )
        populate_base_fields(entity)
        parts.append(ProjectionInput(part=descriptor, entity=entity))
    return ProjectionInputs(parts=tuple(parts))
