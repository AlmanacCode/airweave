"""Offline Slack file inputs from retained message JSON and account-owned blobs."""

import mimetypes
import re
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.entities.canonical.requests import BlobReference, parent_container_key
from airweave.domains.entities.canonical.slack_files import SlackFileManifest
from airweave.domains.storage.protocols import StorageBackend
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
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


async def _file_inventory(
    record: SourceRecord, storage: StorageBackend
) -> tuple[list[SlackFile], SlackFileManifest | None]:
    """Verify optional acquisition evidence against unchanged native file positions."""
    if record.payload_schema_version not in (1, 2):
        raise ValueError("Unsupported Slack message capture schema")
    if record.payload_schema_version == 2 and record.blobs:
        raise ValueError("Child-owned Slack message cannot retain inline file blobs")
    try:
        files = SlackFiles.model_validate(record.payload).files
    except ValidationError:
        raise ValueError("Slack file metadata is malformed") from None
    if len({file.id for file in files}) != len(files):
        raise ValueError("Slack message contains duplicate native file identities")
    manifests = [blob for blob in record.blobs if blob.role == "representation_manifest"]
    if len(manifests) > 1:
        raise ValueError("Slack message has multiple file acquisition manifests")
    manifest = None
    if manifests:
        content = await read_blob(record, manifests[0], storage)
        try:
            manifest = SlackFileManifest.model_validate_json(content)
            if [entry.native_id for entry in manifest.files] != [file.id for file in files]:
                raise ValueError("Slack file manifest does not match native message identities")
            files = [
                SlackFile.model_validate(entry.file) if entry.file is not None else file
                for file, entry in zip(files, manifest.files, strict=True)
            ]
        except ValidationError:
            raise ValueError("Slack file acquisition manifest is malformed") from None
    paths = {f"/files/{index}" for index in range(len(files))}
    if any(blob.role is None and blob.source_path not in paths for blob in record.blobs):
        raise ValueError("Slack blob does not identify a current message file")
    return files, manifest


async def map_slack_files(
    record: SourceRecord, body: BaseEntity, storage: StorageBackend, directory: Path
) -> ProjectionInputs:
    """Retain one body and one expected part per file, including uncaptured originals."""
    files, manifest = await _file_inventory(record, storage)
    populate_base_fields(body)
    native_body = None
    if "text" in record.payload:
        native_body = NativeTextBody(text=record.payload["text"], metadata_fields=("text",))
    parts = [
        ProjectionInput(
            part=ExtractionPart(part_index=0, key="body", kind="body"),
            entity=body,
            native_body=native_body,
        )
    ]
    if record.payload_schema_version == 2:
        return ProjectionInputs(parts=tuple(parts))
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
        if manifest is not None:
            outcome = manifest.files[index]
            if (outcome.outcome == "captured") != bool(references):
                raise ValueError("Slack file manifest contradicts retained original blobs")
        if not references:
            if record.completeness == "complete":
                raise ValueError("Complete Slack message lacks a retained file")
            parts.append(ProjectionInput(part=descriptor, entity=None))
            continue
        entity = await _materialize_file(
            record,
            file,
            references[0],
            storage,
            directory,
            suffix,
            parent_id=body.entity_id,
        )
        parts.append(ProjectionInput(part=descriptor, entity=entity))
    return ProjectionInputs(parts=tuple(parts))


async def _materialize_file(
    record: SourceRecord,
    file: SlackFile,
    reference: BlobReference,
    storage: StorageBackend,
    directory: Path,
    suffix: str,
    *,
    parent_id: str,
) -> SlackAttachmentEntity:
    """Verify only this record's bytes and materialize a disposable converter input."""
    content = await read_blob(record, reference, storage)
    path = await write_blob(content, directory, suffix=suffix)
    entity = SlackAttachmentEntity(
        attachment_key=f"{record.identity.container_id}:{record.identity.native_id}:{file.id}",
        filename=file.name or file.id,
        breadcrumbs=[
            Breadcrumb(entity_id=parent_id, name="Slack message", entity_type="SlackMessageEntity")
        ],
        url="",
        size=len(content),
        file_type=suffix.lstrip("."),
        mime_type=file.mimetype,
        local_path=str(path),
    )
    populate_base_fields(entity)
    return entity


async def map_slack_file(
    record: SourceRecord, storage: StorageBackend, directory: Path
) -> ProjectionInputs:
    """A durable child is the only extraction owner of its message/file occurrence."""
    parent = record.parent
    if (
        record.payload_schema_version != 1
        or parent is None
        or parent.record_type != "message"
        or not parent.container_id
        or record.identity.container_id != parent_container_key(parent)
    ):
        raise ValueError("Slack file requires an exact message parent")
    try:
        file = SlackFile.model_validate(record.payload)
    except ValidationError:
        raise ValueError("Slack child file metadata is malformed") from None
    if file.id != record.identity.native_id:
        raise ValueError("Slack child native file identity disagrees")
    manifests = [blob for blob in record.blobs if blob.role == "representation_manifest"]
    references = [blob for blob in record.blobs if blob.role is None]
    if (
        len(manifests) != 1
        or len(references) > 1
        or any(blob.source_path != "" for blob in references)
    ):
        raise ValueError("Slack child requires one acquisition manifest and a root original")
    content = await read_blob(record, manifests[0], storage)
    try:
        manifest = SlackFileManifest.model_validate_json(content)
        if len(manifest.files) != 1 or manifest.files[0].native_id != file.id:
            raise ValueError("Slack child acquisition manifest identity disagrees")
        outcome = manifest.files[0]
        if outcome.file is not None:
            file = SlackFile.model_validate(outcome.file)
    except ValidationError:
        raise ValueError("Slack child acquisition manifest is malformed") from None
    captured = outcome.outcome == "captured"
    if captured != bool(references) or (not captured and record.completeness == "complete"):
        raise ValueError("Slack child acquisition evidence contradicts retained bytes")
    suffix = Path(file.name or "").suffix.lower()
    if not re.fullmatch(r"\.[a-z0-9]{1,12}", suffix):
        suffix = mimetypes.guess_extension(file.mimetype or "") or ".bin"
    descriptor = ExtractionPart(
        part_index=0, key=f"file:{file.id}", kind="file", media_type=file.mimetype, extension=suffix
    )
    entity = (
        await _materialize_file(
            record, file, references[0], storage, directory, suffix, parent_id=parent.native_id
        )
        if captured
        else None
    )
    return ProjectionInputs(parts=(ProjectionInput(part=descriptor, entity=entity),))
