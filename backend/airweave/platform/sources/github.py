"""Selected GitHub repositories captured through the existing durable page engine."""

from __future__ import annotations

import hashlib
import json
import mimetypes
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from urllib.parse import quote

from pydantic import JsonValue

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    InvalidScanContinuation,
    ScopeAccessLost,
)
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    parent_container_key,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.protocol import SourceAuthProvider
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.auth import GitHubAuthConfig
from airweave.platform.configs.config import GitHubConfig, GitHubRepositorySelection
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.github_http import GitHubReader, GitHubUnavailable
from airweave.platform.sources.github_models import (
    Branch,
    BranchPayload,
    Dates,
    FileProgress,
    GitCommit,
    Identity,
    Issue,
    IssuePayload,
    ListProgress,
    PullRequest,
    Repository,
    RepositoryContext,
    RootProgress,
    Tree,
    TreeEntry,
    TreeFrame,
)
from airweave.schemas.source_connection import AuthenticationMethod

FIELD_SET_VERSION = 1
MAX_CODE_BYTES = 10 * 1024 * 1024


@source(
    name="GitHub",
    short_name="github",
    auth_methods=[AuthenticationMethod.DIRECT, AuthenticationMethod.AUTH_PROVIDER],
    oauth_type=None,
    auth_config_class=GitHubAuthConfig,
    config_class=GitHubConfig,
    labels=["Code"],
    supports_continuous=False,
    rate_limit_level=RateLimitLevel.ORG,
)
class GitHubSource(BaseSource):
    """Full observed sweeps; no claim of an upstream transactional snapshot or event log."""

    canonical_record_types = (
        "repository",
        "branch",
        "file",
        "issue",
        "comment",
        "review",
        "review_comment",
        "pull_file",
    )
    canonical_container_parents = {
        "branch": "repository",
        "file": "branch",
        "issue": "repository",
        "comment": "issue",
        "review": "issue",
        "review_comment": "issue",
        "pull_file": "issue",
    }

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Bind selections and field policy to existing cycle restart semantics."""
        return self._cycle_configuration

    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: GitHubConfig,
    ) -> GitHubSource:
        """Do not fetch or extract managed provider credentials."""
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance.config = config
        instance.reader = GitHubReader(http_client, auth)
        instance.selections = {item.repository_id: item for item in config.repositories}
        fingerprint = {"config": config.model_dump(mode="json"), "fields": FIELD_SET_VERSION}
        instance._cycle_configuration = CycleConfiguration.from_source(
            fingerprint=hashlib.sha256(
                json.dumps(fingerprint, sort_keys=True).encode()
            ).hexdigest(),
            record_types=cls.canonical_record_types,
            container_parents=cls.canonical_container_parents,
        )
        return instance

    async def validate(self) -> None:
        """Verify authentication independently from permission to every selected repository."""
        Identity.model_validate(await self.reader.object("/user"))

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Use one bounded, stable parent identity policy at every hierarchy depth."""
        if self.canonical_container_parents.get(record_type) != parent.identity.record_type:
            raise ValueError("GitHub child scope does not belong to this parent")
        return CompletedScope(
            record_type=record_type,
            container_id=parent_container_key(parent.identity),
            parent=parent.identity,
        )

    @staticmethod
    def _record(
        scope: CompletedScope, native_id: str, payload: dict[str, JsonValue]
    ) -> CaptureRecord:
        native = payload.get("issue", payload.get("native", payload))
        dates = Dates.model_validate(native)
        body = native.get("body") if isinstance(native, dict) else None
        linked_attachment = isinstance(body, str) and any(
            host in body
            for host in ("github.com/user-attachments/", "user-images.githubusercontent.com/")
        )
        return CaptureRecord(
            identity=RecordIdentity(
                record_type=scope.record_type, native_id=native_id, container_id=scope.container_id
            ),
            parent=scope.parent,
            payload=payload,
            payload_schema_version=FIELD_SET_VERSION,
            completeness="partial" if linked_attachment else "complete",
            observed_at=datetime.now(timezone.utc),
            source_created_at=dates.created_at,
            source_updated_at=dates.updated_at,
        )

    @staticmethod
    def _page(
        records: list[CaptureRecord],
        progress: RootProgress | ListProgress | FileProgress,
        *,
        final: bool,
    ) -> CapturePage:
        return CapturePage(
            records=tuple(records),
            final=final,
            continuation=ScanContinuation(value=progress.model_dump(mode="json")),
        )

    @staticmethod
    def _context(repository: Repository) -> RepositoryContext:
        return RepositoryContext(
            repository_id=repository.id,
            owner_id=repository.owner.id,
            full_name=repository.full_name,
        )

    async def _repository(
        self, selection: GitHubRepositorySelection
    ) -> dict[str, JsonValue] | None:
        try:
            payload = await self.reader.object("/repos/" + selection.full_name, redirects=True)
        except GitHubUnavailable:
            return None
        repository = Repository.model_validate(payload)
        # Numeric owner selection is an authorization constraint, not repository identity.
        if repository.id != selection.repository_id or repository.owner.id != selection.owner_id:
            return None
        return payload

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Provider I/O happens outside SQL; the driver commits returned progress atomically."""
        if scope.record_type == "repository":
            if parent is not None or scope.container_id is not None:
                raise ValueError("GitHub repository inventory must be a root scope")
            return await self._repositories(scope, continuation)
        if parent is None or scope != self.child_scope(parent, scope.record_type):
            raise ValueError("GitHub page requires its exact captured parent")
        file_progress = None
        if scope.record_type == "file":
            file_progress = FileProgress.model_validate(continuation.value)
            branch = Branch.model_validate(BranchPayload.model_validate(parent.payload).branch)
            if file_progress.initialized and (
                file_progress.ref != branch.name or file_progress.commit_sha != branch.commit.sha
            ):
                raise InvalidScanContinuation(
                    "GitHub selected branch changed since the pinned scan"
                )
        try:
            await self._verify_parent_repository(parent)
            if scope.record_type == "branch":
                return await self._branches(scope, parent)
            if scope.record_type == "file":
                assert file_progress is not None
                return await self._files(scope, file_progress, parent, files)
            if scope.record_type == "issue":
                return await self._issues(scope, continuation, parent)
            return await self._conversation(scope, continuation, parent)
        except GitHubUnavailable as error:
            raise ScopeAccessLost(
                "GitHub parent is no longer readable", removal_reason="scope_removed"
            ) from error

    async def _verify_parent_repository(self, parent: SourceRecord) -> None:
        """A captured path is routing context, never continuing authorization after rename/reuse."""
        if parent.identity.record_type == "repository":
            context = self._context(Repository.model_validate(parent.payload))
        elif parent.identity.record_type == "branch":
            context = BranchPayload.model_validate(parent.payload).repository
        else:
            context = IssuePayload.model_validate(parent.payload).repository
        current = Repository.model_validate(await self.reader.object("/repos/" + context.full_name))
        if current.id != context.repository_id or current.owner.id != context.owner_id:
            raise GitHubUnavailable("GitHub repository no longer matches the authorized selection")
        if parent.identity.record_type == "issue":
            issue = Issue.model_validate(IssuePayload.model_validate(parent.payload).issue)
            current_issue = Issue.model_validate(
                await self.reader.object(f"/repos/{context.full_name}/issues/{issue.number}")
            )
            if current_issue.id != issue.id:
                raise GitHubUnavailable("GitHub issue no longer matches the captured parent")

    async def _repositories(
        self, scope: CompletedScope, continuation: ScanContinuation
    ) -> CapturePage:
        progress = RootProgress.model_validate(continuation.value)
        if progress.repository_index > len(self.config.repositories):
            raise InvalidScanContinuation("GitHub repository selection changed")
        records = []
        if progress.repository_index < len(self.config.repositories):
            selection = self.config.repositories[progress.repository_index]
            payload = await self._repository(selection)
            if payload is not None:
                records.append(self._record(scope, str(selection.repository_id), payload))
            progress.repository_index += 1
        return self._page(
            records, progress, final=progress.repository_index == len(self.config.repositories)
        )

    async def _branches(self, scope: CompletedScope, parent: SourceRecord) -> CapturePage:
        repository = Repository.model_validate(parent.payload)
        selection = self.selections[repository.id]
        if not self.config.include_code:
            return self._page([], RootProgress(), final=True)
        ref = selection.ref or repository.default_branch
        try:
            payload = await self.reader.object(
                f"/repos/{repository.full_name}/branches/{quote(ref, safe='')}"
            )
        except GitHubUnavailable:
            if await self._repository(selection) is None:
                raise
            # A missing branch is not proof that repository issues also became private.
            return self._page([], RootProgress(), final=True)
        branch = Branch.model_validate(payload)
        if branch.name != ref:
            raise ValueError("GitHub returned a different selected branch")
        body = BranchPayload(branch=payload, repository=self._context(repository))
        return self._page(
            [self._record(scope, branch.name, body.model_dump(mode="json"))],
            RootProgress(),
            final=True,
        )

    async def _issues(
        self, scope: CompletedScope, continuation: ScanContinuation, parent: SourceRecord
    ) -> CapturePage:
        progress = ListProgress.model_validate(continuation.value)
        if not self.config.include_conversations:
            return self._page([], progress, final=True)
        repository = Repository.model_validate(parent.payload)
        path = f"/repos/{repository.full_name}"
        payloads, final = await self.reader.page(
            path + "/issues", progress.page, extra="state=all&sort=created&direction=asc"
        )
        records = []
        for payload in payloads:
            issue = Issue.model_validate(payload)
            if issue.repository_url != "https://api.github.com" + path:
                raise ValueError("GitHub issue belongs to a different repository")
            detail = None
            if issue.pull_request is not None:
                detail = await self.reader.object(f"{path}/pulls/{issue.number}")
                if PullRequest.model_validate(detail).number != issue.number:
                    raise ValueError("GitHub returned a different pull request")
            body = IssuePayload(
                issue=payload, repository=self._context(repository), pull_request_detail=detail
            )
            records.append(self._record(scope, str(issue.id), body.model_dump(mode="json")))
        progress.page += 1
        progress.count += len(records)
        return self._page(records, progress, final=final)

    async def _conversation(
        self, scope: CompletedScope, continuation: ScanContinuation, parent: SourceRecord
    ) -> CapturePage:
        progress = ListProgress.model_validate(continuation.value)
        body = IssuePayload.model_validate(parent.payload)
        issue = Issue.model_validate(body.issue)
        base = f"/repos/{body.repository.full_name}"
        if scope.record_type != "comment" and issue.pull_request is None:
            return self._page([], progress, final=True)
        suffix = {
            "comment": f"issues/{issue.number}/comments",
            "review": f"pulls/{issue.number}/reviews",
            "review_comment": f"pulls/{issue.number}/comments",
            "pull_file": f"pulls/{issue.number}/files",
        }[scope.record_type]
        before = None
        if scope.record_type == "pull_file":
            before = PullRequest.model_validate(
                await self.reader.object(f"{base}/pulls/{issue.number}")
            )
            if progress.head_sha is not None and (
                progress.head_sha != before.head.sha or progress.base_sha != before.base.sha
            ):
                raise InvalidScanContinuation("GitHub pull request changed while listing files")
            progress.head_sha, progress.base_sha = before.head.sha, before.base.sha
        payloads, final = await self.reader.page(f"{base}/{suffix}", progress.page)
        if before is not None:
            await self._verify_pull_files(
                base, issue.number, before, progress.count + len(payloads), final
            )
        records = []
        for payload in payloads:
            if scope.record_type == "pull_file":
                native_id = payload.get("filename")
                if not isinstance(native_id, str) or not native_id:
                    raise ValueError("GitHub pull request file has no native path")
            else:
                native_id = str(Identity.model_validate(payload).id)
            captured = {
                "native": payload,
                "repository": body.repository.model_dump(mode="json"),
                "issue_number": issue.number,
            }
            if before is not None:
                captured.update({"head_sha": before.head.sha, "base_sha": before.base.sha})
            record = self._record(scope, native_id, captured)
            # PR file API is metadata/patch coverage; original bytes live in selected branch files.
            if scope.record_type == "pull_file":
                record = record.model_copy(update={"completeness": "metadata_only"})
            records.append(record)
        progress.page += 1
        progress.count += len(records)
        return self._page(records, progress, final=final)

    async def _verify_pull_files(
        self, base: str, number: int, before: PullRequest, count: int, final: bool
    ) -> None:
        after = PullRequest.model_validate(await self.reader.object(f"{base}/pulls/{number}"))
        if (after.head.sha, after.base.sha) != (before.head.sha, before.base.sha):
            raise InvalidScanContinuation("GitHub pull request changed while listing files")
        if count >= 3000 and (not final or before.changed_files > count):
            raise ValueError("GitHub pull request file listing exceeds its complete API window")
        if final and count != before.changed_files:
            raise ValueError("GitHub pull request file count was not completely enumerated")

    async def _files(
        self,
        scope: CompletedScope,
        progress: FileProgress,
        parent: SourceRecord,
        files: FileService,
    ) -> CapturePage:
        body = BranchPayload.model_validate(parent.payload)
        branch = Branch.model_validate(body.branch)
        base = f"/repos/{body.repository.full_name}/git"
        try:
            progress = await self._pin_code(progress, branch, base)
            records = []
            stack = list(progress.stack)
            # One tree response per page bounds work for enormous empty directory trees.
            if stack:
                frame = stack[-1]
                tree = Tree.model_validate(await self.reader.object(f"{base}/trees/{frame.sha}"))
                if tree.sha != frame.sha or tree.truncated or frame.offset > len(tree.tree):
                    raise ValueError("GitHub tree cannot be completely and consistently enumerated")
                while frame.offset < len(tree.tree) and len(records) < 1:
                    raw = tree.tree[frame.offset]
                    entry = TreeEntry.model_validate(raw)
                    if entry.path in {".", ".."} or "/" in entry.path or "\x00" in entry.path:
                        raise ValueError("GitHub tree entry is not a single native path component")
                    frame.offset += 1
                    if entry.type == "tree":
                        stack.append(TreeFrame(sha=entry.sha, component=entry.path))
                        break
                    path = "/".join(
                        [item.component for item in stack if item.component] + [entry.path]
                    )
                    payload = {
                        "entry": raw,
                        "path": path,
                        "ref": progress.ref,
                        "commit_sha": progress.commit_sha,
                        "tree_sha": progress.tree_sha,
                        "repository": body.repository.model_dump(mode="json"),
                    }
                    record = self._record(scope, path, payload)
                    record = await self._retain_blob(record, entry, base, path, files)
                    records.append(record)
                if stack[-1] is frame and frame.offset == len(tree.tree):
                    stack.pop()
            progress.stack = tuple(stack)
            return self._page(records, progress, final=not stack)
        except GitHubUnavailable as error:
            # An unreadable branch withdraws this code scope. A still-readable branch
            # with a missing immutable snapshot requires restart, never absence cleanup.
            await self.reader.object(
                f"/repos/{body.repository.full_name}/branches/{quote(branch.name, safe='')}"
            )
            raise InvalidScanContinuation("GitHub pinned code snapshot is unavailable") from error

    async def _pin_code(self, progress: FileProgress, branch: Branch, base: str) -> FileProgress:
        if progress.initialized:
            return progress
        commit = GitCommit.model_validate(
            await self.reader.object(f"{base}/commits/{branch.commit.sha}")
        )
        if commit.sha != branch.commit.sha:
            raise ValueError("GitHub returned a different pinned commit")
        return FileProgress(
            ref=branch.name,
            commit_sha=commit.sha,
            tree_sha=commit.tree.sha,
            stack=(TreeFrame(sha=commit.tree.sha, component=""),),
            initialized=True,
        )

    async def _retain_blob(
        self, record: CaptureRecord, entry: TreeEntry, base: str, path: str, files: FileService
    ) -> CaptureRecord:
        if entry.type == "commit":
            return record.model_copy(update={"completeness": "metadata_only"})
        if entry.size is None or entry.size > MAX_CODE_BYTES:
            return record.model_copy(update={"completeness": "partial"})
        content = await self.reader.blob(
            f"{base}/blobs/{entry.sha}", max_bytes=min(entry.size, MAX_CODE_BYTES)
        )
        if len(content) != entry.size or len(content) > MAX_CODE_BYTES:
            raise ValueError("GitHub blob size differs from its immutable tree entry")
        digest = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
        if digest != entry.sha:
            raise ValueError("GitHub blob does not match its immutable object identity")
        blob = await files.store_canonical_blob(content, media_type=mimetypes.guess_type(path)[0])
        lfs_pointer = content.startswith(b"version https://git-lfs.github.com/spec/v1\n")
        return record.model_copy(
            update={"blobs": (blob,), "completeness": "partial" if lfs_pointer else "complete"}
        )

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Confirm omitted inventory owners before descendants lose visibility."""
        kind = record.identity.record_type
        if kind == "repository":
            selection = self.selections.get(int(record.identity.native_id))
            if selection is None or await self._repository(selection) is None:
                return
        elif kind == "branch":
            if await self._branch_absent(record):
                return
        elif kind == "issue":
            if not self.config.include_conversations:
                return
            body = IssuePayload.model_validate(record.payload)
            issue = Issue.model_validate(body.issue)
            try:
                current = Issue.model_validate(
                    await self.reader.object(
                        f"/repos/{body.repository.full_name}/issues/{issue.number}"
                    )
                )
            except GitHubUnavailable:
                return
            if current.id != issue.id:
                return
        else:
            raise ValueError("GitHub absence confirmation is only valid for inventory owners")
        raise ValueError("GitHub omitted an inventory record that remains readable in selection")

    async def _branch_absent(self, record: SourceRecord) -> bool:
        body = BranchPayload.model_validate(record.payload)
        selection = self.selections.get(body.repository.repository_id)
        if selection is None or not self.config.include_code:
            return True
        repository = await self._repository(selection)
        if repository is None:
            return True
        ref = selection.ref or Repository.model_validate(repository).default_branch
        if ref != record.identity.native_id:
            return True
        try:
            await self.reader.object(
                f"/repos/{body.repository.full_name}/branches/{quote(ref, safe='')}"
            )
        except GitHubUnavailable:
            return True
        return False

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Replace the legacy lossy traversal; the registry chooses canonical capture."""
        raise NotImplementedError("GitHub requires canonical page capture")
        yield  # pragma: no cover
