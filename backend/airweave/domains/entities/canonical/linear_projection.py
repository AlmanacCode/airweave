"""Offline Linear views of native originals and verified immutable files."""

import mimetypes
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.entities._base import BaseEntity, Breadcrumb
from airweave.platform.entities.linear import (
    LinearAttachmentEntity,
    LinearCommentEntity,
    LinearIssueEntity,
    LinearLinkedAttachmentEntity,
)


class _Named(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: UUID
    name: str


class _IssueContext(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: UUID
    identifier: str = Field(min_length=1)
    title: str
    url: str
    team: _Named
    project: _Named | None = None


class _Original(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: UUID
    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    url: str


class _Child(_Original):
    issue: _IssueContext


class _Comment(_Child):
    body: str


class _Attachment(_Child):
    title: str


def _breadcrumbs(issue: _IssueContext) -> list[Breadcrumb]:
    return [
        Breadcrumb(entity_id=str(issue.id), name=issue.identifier, entity_type="LinearIssueEntity")
    ]


async def map_linear(
    record: SourceRecord, storage: StorageBackend, directory: Path
) -> tuple[BaseEntity, ...]:
    """Map only retained observations, rejecting identity drift or missing native context."""
    original = _Original.model_validate(record.payload)
    if str(original.id) != record.identity.native_id:
        raise ValueError("Linear record identity differs from captured original")
    kind = record.identity.record_type
    if kind == "issue":
        _IssueContext.model_validate(record.payload)
        if record.identity.container_id is not None:
            raise ValueError("Linear issue unexpectedly has a parent container")
        issue = LinearIssueEntity.from_api(record.payload)
        issue.web_url_value = original.url
        return (issue,)
    if kind not in {"comment", "attachment"}:
        raise ValueError("Unsupported Linear original kind")
    child = _Child.model_validate(record.payload)
    context = child.issue
    if str(context.id) != record.identity.container_id:
        raise ValueError("Linear child parent differs from canonical container")
    breadcrumbs = _breadcrumbs(context)
    if kind == "comment":
        _Comment.model_validate(record.payload)
        entity = LinearCommentEntity.from_api(
            record.payload,
            issue_id=str(context.id),
            issue_identifier=context.identifier,
            breadcrumbs=breadcrumbs,
            team_id=str(context.team.id),
            team_name=context.team.name,
            project_id=str(context.project.id) if context.project else None,
            project_name=context.project.name if context.project else None,
        )
        entity.web_url_value = original.url
        return (entity,)
    return await _attachment(record, storage, directory)


async def _attachment(
    record: SourceRecord,
    storage: StorageBackend,
    directory: Path,
) -> tuple[BaseEntity, ...]:
    attachment = _Attachment.model_validate(record.payload)
    context = attachment.issue
    breadcrumbs = _breadcrumbs(context)
    if not record.blobs:
        if record.completeness == "complete":
            raise ValueError("Linear attachment claims complete content without retained bytes")
        return (
            LinearLinkedAttachmentEntity(
                attachment_id=str(attachment.id),
                title=attachment.title,
                issue_identifier=context.identifier,
                target_url=attachment.url,
                content_coverage="Link metadata only; file content is not retained",
                breadcrumbs=breadcrumbs,
                created_at=attachment.createdAt,
                updated_at=attachment.updatedAt,
            ),
        )
    if len(record.blobs) != 1 or record.blobs[0].source_path != "/url":
        raise ValueError("Linear attachment requires one exact URL blob reference")
    ref = record.blobs[0]
    content = await read_blob(record, ref, storage)
    suffix = mimetypes.guess_extension(ref.media_type or "")
    if not suffix:
        suffix = Path(urlsplit(attachment.url).path).suffix.lower()
    if not suffix:
        raise ValueError("Retained Linear file has no known format for projection")
    local_path = await write_blob(content, directory, suffix=suffix)
    return (
        LinearAttachmentEntity(
            attachment_id=str(attachment.id),
            issue_id=str(context.id),
            issue_identifier=context.identifier,
            title=attachment.title,
            url=attachment.url,
            size=len(content),
            file_type=suffix.lstrip("."),
            mime_type=ref.media_type,
            local_path=str(local_path),
            breadcrumbs=breadcrumbs,
            web_url_value=attachment.url,
            created_at=attachment.createdAt,
            updated_at=attachment.updatedAt,
        ),
    )
