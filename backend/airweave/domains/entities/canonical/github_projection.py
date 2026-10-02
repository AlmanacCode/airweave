"""Offline GitHub views; search never fetches uncaptured provider content."""

from pathlib import Path
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, JsonValue

from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.entities._base import BaseEntity
from airweave.platform.entities.github import GitHubCodeFileEntity, GitHubTextEntity
from airweave.platform.sources.github_models import (
    BranchPayload,
    IssuePayload,
    Repository,
    RepositoryContext,
    TreeEntry,
)
from airweave.platform.utils.file_extensions import get_language_for_extension


class _Text(BaseModel):
    model_config = ConfigDict(extra="ignore")
    title: str | None = None
    name: str | None = None
    body: str | None = None
    description: str | None = None
    patch: str | None = None
    filename: str | None = None
    state: str | None = None


class _Child(BaseModel):
    model_config = ConfigDict(extra="forbid")
    native: dict[str, JsonValue]
    repository: RepositoryContext
    issue_number: int
    head_sha: str | None = None
    base_sha: str | None = None


class _File(BaseModel):
    model_config = ConfigDict(extra="forbid")
    entry: TreeEntry
    path: str
    ref: str
    commit_sha: str
    tree_sha: str
    repository: RepositoryContext


async def map_github(
    record: SourceRecord, storage: StorageBackend, directory: Path
) -> tuple[BaseEntity, ...]:
    """Map only current captured originals and verified immutable bytes."""
    kind = record.identity.record_type
    if kind == "file":
        return await _file(record, storage, directory)
    if kind == "repository":
        repository = Repository.model_validate(record.payload)
        if str(repository.id) != record.identity.native_id:
            raise ValueError("GitHub repository identity differs from captured payload")
        data, full_name = record.payload, repository.full_name
    elif kind == "branch":
        branch = BranchPayload.model_validate(record.payload)
        data, full_name = branch.branch, branch.repository.full_name
    elif kind == "issue":
        issue = IssuePayload.model_validate(record.payload)
        data, full_name = issue.issue, issue.repository.full_name
    elif kind in {"comment", "review", "review_comment", "pull_file"}:
        child = _Child.model_validate(record.payload)
        data, full_name = child.native, child.repository.full_name
    else:
        raise ValueError("Unsupported GitHub original kind")
    native = _Text.model_validate(data)
    return (
        GitHubTextEntity(
            original_id=str(record.id),
            title=native.title
            or native.name
            or native.filename
            or f"{kind} {record.identity.native_id}",
            resource_kind=kind,
            repository=full_name,
            text=native.body or native.description or native.patch or "",
            state=native.state,
            content_coverage=record.completeness,
            breadcrumbs=[],
            created_at=record.source_created_at,
            updated_at=record.source_updated_at,
        ),
    )


async def _file(
    record: SourceRecord, storage: StorageBackend, directory: Path
) -> tuple[BaseEntity, ...]:
    data = _File.model_validate(record.payload)
    if data.path != record.identity.native_id:
        raise ValueError("GitHub file path differs from captured identity")
    if record.completeness != "complete":
        return (
            GitHubTextEntity(
                original_id=str(record.id),
                title=data.path,
                resource_kind="file",
                repository=data.repository.full_name,
                text="",
                content_coverage=record.completeness,
                breadcrumbs=[],
            ),
        )
    if len(record.blobs) != 1 or data.entry.type != "blob":
        raise ValueError("Complete GitHub file requires one retained blob")
    content = await read_blob(record, record.blobs[0], storage)
    suffix = Path(data.path).suffix.lower()
    if not suffix:
        content.decode("utf-8")  # Extensionless text is supported; arbitrary binary is not.
        suffix = ".txt"
    local = await write_blob(content, directory, suffix=suffix)
    owner, repo = data.repository.full_name.split("/", 1)
    url = (
        f"https://github.com/{data.repository.full_name}/blob/{data.commit_sha}/"
        f"{quote(data.path, safe='/')}"
    )
    return (
        GitHubCodeFileEntity(
            full_path=data.path,
            name=Path(data.path).name,
            branch=data.ref,
            sha=data.entry.sha,
            repo_name=repo,
            repo_owner=owner,
            path_in_repo=data.path,
            language=get_language_for_extension(suffix),
            commit_id=data.commit_sha,
            url=url,
            html_url=url,
            size=len(content),
            file_type=suffix,
            mime_type=record.blobs[0].media_type,
            local_path=str(local),
            breadcrumbs=[],
        ),
    )
