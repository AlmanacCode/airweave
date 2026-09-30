"""Synthetic native GitHub fixtures; no provider credentials or writes."""

import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import InvalidScanContinuation, ScopeAccessLost
from airweave.domains.entities.canonical.requests import BlobReference, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceError, SourceRateLimitError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.platform.configs.config import GitHubConfig
from airweave.platform.sources.github import GitHubSource
from airweave.platform.sources.github_http import GitHubReader

REPO = {"id": 100, "owner": {"id": 200}, "full_name": "team/repo", "default_branch": "main"}
ISSUE = {
    "id": 300,
    "number": 7,
    "repository_url": "https://api.github.com/repos/team/repo",
    "title": "Original",
    "unknown": {"preserved": True},
}
COMMIT, TREE = "1" * 40, "2" * 40
BRANCH = {"name": "main", "commit": {"sha": COMMIT}}
CONFIG = {"repositories": [{"repository_id": 100, "owner_id": 200, "full_name": "team/repo"}]}


def response(value=None, *, status=200, headers=None, content=None):
    return httpx.Response(
        status,
        json=value if content is None else None,
        content=content,
        headers=headers,
        request=httpx.Request("GET", "https://api.github.com/test"),
    )


async def source(*responses, config=None):
    client = AsyncMock()
    client.get.side_effect = responses

    @asynccontextmanager
    async def stream(method, url, **kwargs):
        original = await client.get(url, **kwargs)
        if original.is_stream_consumed:
            original = httpx.Response(
                original.status_code,
                headers=original.headers,
                stream=httpx.ByteStream(original.content),
            )
        try:
            yield original
        finally:
            await original.aclose()

    client.stream = MagicMock(side_effect=stream)
    auth = ManagedAuthProvider(
        api_key="synthetic",
        connected_account_id="ca_synthetic",
        allowed_hosts=frozenset({"api.github.com"}),
    )
    capture = await GitHubSource.create(
        auth=auth,
        logger=MagicMock(),
        http_client=client,
        config=GitHubConfig.model_validate(config or CONFIG),
    )
    return capture, client


def saved(record):
    return SourceRecord(
        id=UUID(int=1),
        sync_id=UUID(int=2),
        identity=record.identity,
        parent=record.parent,
        revision=1,
        payload=record.payload,
        payload_schema_version=record.payload_schema_version,
        capture_hash="a" * 64,
        content_hash=None,
        completeness=record.completeness,
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=record.blobs,
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


async def repository(capture):
    return saved(
        (
            await capture.capture_page(
                CompletedScope(record_type="repository"), ScanContinuation(), files=MagicMock()
            )
        ).records[0]
    )


@pytest.mark.asyncio
async def test_managed_auth_and_raw_root_fields():
    capture, client = await source(response({**REPO, "private": True, "extra": [1, 2]}))
    root = await repository(capture)
    assert root.payload["extra"] == [1, 2]
    assert root.identity.native_id == "100"
    assert "Authorization" not in client.get.call_args.kwargs["headers"]
    assert client.get.call_args.kwargs["follow_redirects"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [{"id": 101}, {"owner": {"id": 201}}])
async def test_name_reuse_or_owner_transfer_not_captured(changed):
    capture, _ = await source(response({**REPO, **changed}))
    result = await capture.capture_page(
        CompletedScope(record_type="repository"), ScanContinuation(), files=MagicMock()
    )
    assert result.final and not result.records


@pytest.mark.asyncio
async def test_issue_payload_and_native_pagination():
    next_link = (
        "<https://api.github.com/repos/team/repo/issues?per_page=100&page=2"
        '&state=all&sort=created&direction=asc>; rel="next"'
    )
    capture, _ = await source(
        response(REPO),
        response(REPO),
        response([ISSUE], headers={"Link": next_link}),
        response(REPO),
        response([]),
    )
    root = await repository(capture)
    scope = capture.child_scope(root, "issue")
    first = await capture.capture_page(scope, ScanContinuation(), files=MagicMock(), parent=root)
    assert first.records[0].payload["issue"] == ISSUE
    assert first.records[0].parent == root.identity
    assert not first.final
    final = await capture.capture_page(scope, first.continuation, files=MagicMock(), parent=root)
    assert final.final


@pytest.mark.asyncio
async def test_child_route_revalidated_against_repository_name_reuse():
    capture, client = await source(response(REPO), response({**REPO, "id": 999}))
    root = await repository(capture)
    with pytest.raises(ScopeAccessLost) as error:
        await capture.capture_page(
            capture.child_scope(root, "issue"), ScanContinuation(), files=MagicMock(), parent=root
        )
    assert error.value.removal_reason == "scope_removed"
    assert client.get.call_count == 2


@pytest.mark.asyncio
async def test_issue_number_reuse_does_not_read_another_issues_comments():
    capture, client = await source(
        response(REPO),
        response(REPO),
        response([ISSUE]),
        response(REPO),
        response({**ISSUE, "id": 999}),
    )
    root = await repository(capture)
    issue = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "issue"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    with pytest.raises(ScopeAccessLost):
        await capture.capture_page(
            capture.child_scope(issue, "comment"),
            ScanContinuation(),
            files=MagicMock(),
            parent=issue,
        )
    assert client.get.call_count == 5


@pytest.mark.asyncio
async def test_pinned_code_blob_retained_and_resume_descends_tree():
    content = b"print('hello')\n"
    blob_sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
    child_tree = "3" * 40
    root_tree = {
        "sha": TREE,
        "truncated": False,
        "tree": [{"path": "src", "mode": "040000", "type": "tree", "sha": child_tree}],
    }
    capture, client = await source(
        response(REPO),
        response(REPO),
        response(BRANCH),
        response(REPO),
        response({"sha": COMMIT, "tree": {"sha": TREE}}),
        response(root_tree),
        response(REPO),
        response(
            {
                "sha": child_tree,
                "truncated": False,
                "tree": [
                    {
                        "path": "hello.py",
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob_sha,
                        "size": len(content),
                    }
                ],
            }
        ),
        response(content=content),
        response(REPO),
        response(root_tree),
    )
    root = await repository(capture)
    branch = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "branch"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    scope = capture.child_scope(branch, "file")
    files = MagicMock()
    files.store_canonical_blob = AsyncMock(
        return_value=BlobReference(
            key="canonical/test/blob",
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )
    )
    first = await capture.capture_page(scope, ScanContinuation(), files=files, parent=branch)
    assert not first.final and not first.records
    second = await capture.capture_page(scope, first.continuation, files=files, parent=branch)
    assert second.records[0].identity.native_id == "src/hello.py"
    assert second.records[0].payload["commit_sha"] == COMMIT
    assert second.records[0].blobs
    assert files.store_canonical_blob.call_args.args == (content,)
    final = await capture.capture_page(scope, second.continuation, files=files, parent=branch)
    assert final.final
    assert sum("/git/commits/" in str(call.args[0]) for call in client.get.call_args_list) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tree_response,exception",
    [
        (response({"sha": TREE, "truncated": True, "tree": []}), ValueError),
        (response({}, status=404), InvalidScanContinuation),
    ],
)
async def test_unfinished_tree_never_completes(tree_response, exception):
    capture, _ = await source(
        response(REPO),
        response(REPO),
        response(BRANCH),
        response(REPO),
        response({"sha": COMMIT, "tree": {"sha": TREE}}),
        tree_response,
        response(BRANCH),
    )
    root = await repository(capture)
    branch = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "branch"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    with pytest.raises(exception):
        await capture.capture_page(
            capture.child_scope(branch, "file"),
            ScanContinuation(),
            files=MagicMock(),
            parent=branch,
        )


@pytest.mark.asyncio
async def test_missing_branch_does_not_withdraw_repository_issues():
    capture, _ = await source(
        response(REPO), response(REPO), response({}, status=404), response(REPO)
    )
    root = await repository(capture)
    result = await capture.capture_page(
        capture.child_scope(root, "branch"), ScanContinuation(), files=MagicMock(), parent=root
    )
    assert result.final and not result.records


@pytest.mark.asyncio
async def test_external_redirect_rejected_before_credentials_forwarded():
    capture, client = await source(
        response({}, status=301, headers={"Location": "https://evil.invalid/steal"})
    )
    with pytest.raises(SourceError, match="untrusted"):
        await repository(capture)
    assert client.get.call_count == 1


@pytest.mark.asyncio
async def test_foreign_or_stuck_page_link_rejected():
    capture, _ = await source(
        response(REPO),
        response(REPO),
        response(
            [ISSUE],
            headers={"Link": '<https://api.github.com/repos/other/repo/issues?page=2>; rel="next"'},
        ),
    )
    root = await repository(capture)
    with pytest.raises(ValueError, match="out-of-scope"):
        await capture.capture_page(
            capture.child_scope(root, "issue"), ScanContinuation(), files=MagicMock(), parent=root
        )


def test_rate_limit_never_classified_as_scope_loss():
    reader = GitHubReader(MagicMock(), MagicMock())
    with pytest.raises(SourceRateLimitError):
        reader._status(response({"message": "API rate limit exceeded"}, status=403))


def test_legacy_and_ambiguous_selections_rejected():
    with pytest.raises(ValidationError, match="Refresh GitHub"):
        GitHubConfig(repo_name="team/repo")
    with pytest.raises(ValidationError):
        GitHubConfig.model_validate({"repositories": CONFIG["repositories"] * 2})


@pytest.mark.asyncio
async def test_reviews_comments_and_pr_files_keep_original_fields():
    pr_issue = {**ISSUE, "pull_request": {"url": "https://api.github.com/repos/team/repo/pulls/7"}}
    pr = {
        "id": 400,
        "number": 7,
        "head": {"sha": "a" * 40},
        "base": {"sha": "b" * 40},
        "changed_files": 1,
    }
    review = {
        "id": 500,
        "state": "APPROVED",
        "body": "Looks good",
        "submitted_at": "2026-09-30T00:00:00Z",
    }
    file = {"filename": "README.md", "patch": "@@ ...", "status": "modified"}
    capture, _ = await source(
        response(REPO),
        response(REPO),
        response([pr_issue]),
        response(pr),
        response(REPO),
        response(pr_issue),
        response([review]),
        response(REPO),
        response(pr_issue),
        response(pr),
        response([file]),
        response(pr),
    )
    root = await repository(capture)
    issue = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "issue"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    assert issue.payload["pull_request_detail"] == pr
    result = await capture.capture_page(
        capture.child_scope(issue, "review"), ScanContinuation(), files=MagicMock(), parent=issue
    )
    assert result.records[0].payload["native"] == review
    result = await capture.capture_page(
        capture.child_scope(issue, "pull_file"), ScanContinuation(), files=MagicMock(), parent=issue
    )
    assert result.records[0].payload["native"] == file
    assert result.records[0].completeness == "metadata_only"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["head", "cap"])
async def test_pr_diff_changed_or_truncated_does_not_complete(change):
    pr_issue = {**ISSUE, "pull_request": {}}
    pr = {
        "id": 400,
        "number": 7,
        "head": {"sha": "a" * 40},
        "base": {"sha": "b" * 40},
        "changed_files": 3001 if change == "cap" else 1,
    }
    after = {**pr, "head": {"sha": "c" * 40}} if change == "head" else pr
    capture, _ = await source(
        response(REPO),
        response(REPO),
        response([pr_issue]),
        response(pr),
        response(REPO),
        response(pr_issue),
        response(pr),
        response([{"filename": "one"}]),
        response(after),
    )
    root = await repository(capture)
    issue = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "issue"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    with pytest.raises(InvalidScanContinuation if change == "head" else ValueError):
        await capture.capture_page(
            capture.child_scope(issue, "pull_file"),
            ScanContinuation(),
            files=MagicMock(),
            parent=issue,
        )


@pytest.mark.asyncio
async def test_streamed_blob_enforces_incremental_bound():
    class Chunks(httpx.AsyncByteStream):
        def __init__(self):
            self.parts = iter((b"small", b"oversized"))

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.parts)
            except StopIteration:
                raise AssertionError("Should stop consuming at the size limit") from None

    native = httpx.Response(200, stream=Chunks())
    capture, _ = await source(native)
    with pytest.raises(ValueError, match="byte bound"):
        await capture.reader.blob("/repos/team/repo/git/blobs/" + "a" * 40, max_bytes=6)


def test_corrupt_pinned_progress_rejected():
    from airweave.platform.sources.github_models import FileProgress

    with pytest.raises(ValidationError, match="complete snapshot"):
        FileProgress(initialized=True)
    with pytest.raises(ValidationError, match="cannot contain"):
        FileProgress(commit_sha=COMMIT)


@pytest.mark.asyncio
async def test_composio_capability_binds_only_selected_github_account_and_api_host():
    from airweave.domains.auth_provider.exceptions import AuthProviderConfigError
    from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider

    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "synthetic"},
        config={"auth_config_id": "cfg", "account_id": "ca_synthetic"},
    )
    provider._get_with_auth = AsyncMock(
        return_value={
            "id": "ca_synthetic",
            "toolkit": {"slug": "github"},
            "auth_config": {"id": "cfg"},
            "status": "ACTIVE",
        }
    )
    result = await provider.get_auth_result("github", [])
    assert result.managed_auth.connected_account_id == "ca_synthetic"
    assert result.managed_auth.allowed_hosts == frozenset({"api.github.com"})
    assert provider._get_with_auth.call_args.args[1].endswith("/ca_synthetic")
    provider._get_with_auth.return_value["toolkit"] = {"slug": "slack"}
    with pytest.raises(AuthProviderConfigError, match="toolkit"):
        await provider.get_auth_result("github", [])


@pytest.mark.asyncio
async def test_bad_saved_snapshot_fails_before_provider_request():
    capture, client = await source(response(REPO), response(REPO), response(BRANCH))
    root = await repository(capture)
    branch = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "branch"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    with pytest.raises(ValidationError):
        await capture.capture_page(
            capture.child_scope(branch, "file"),
            ScanContinuation(value={"initialized": True}),
            files=MagicMock(),
            parent=branch,
        )
    assert client.get.call_count == 3
    with pytest.raises(InvalidScanContinuation, match="branch changed"):
        await capture.capture_page(
            capture.child_scope(branch, "file"),
            ScanContinuation(
                value={"initialized": True, "ref": "main", "commit_sha": "f" * 40, "tree_sha": TREE}
            ),
            files=MagicMock(),
            parent=branch,
        )
    assert client.get.call_count == 3


@pytest.mark.asyncio
async def test_linked_issue_upload_is_explicitly_partial():
    issue = {**ISSUE, "body": "![screenshot](https://github.com/user-attachments/assets/example)"}
    capture, _ = await source(response(REPO), response(REPO), response([issue]))
    root = await repository(capture)
    page = await capture.capture_page(
        capture.child_scope(root, "issue"), ScanContinuation(), files=MagicMock(), parent=root
    )
    assert page.records[0].completeness == "partial"
    assert page.records[0].payload["issue"] == issue


@pytest.mark.asyncio
async def test_native_times_feed_existing_canonical_projection_metadata():
    issue = {**ISSUE, "created_at": "2025-01-01T10:00:00Z", "updated_at": "2026-09-30T10:00:00Z"}
    capture, _ = await source(response(REPO), response(REPO), response([issue]))
    root = await repository(capture)
    assert root.source_created_at is None and root.source_updated_at is None
    page = await capture.capture_page(
        capture.child_scope(root, "issue"), ScanContinuation(), files=MagicMock(), parent=root
    )
    item = page.records[0]
    assert item.source_created_at.isoformat() == "2025-01-01T10:00:00+00:00"
    assert item.source_updated_at.isoformat() == "2026-09-30T10:00:00+00:00"
    assert item.source_created_at != item.observed_at


@pytest.mark.asyncio
async def test_existing_direct_pat_auth_remains_supported():
    from airweave.domains.sources.token_providers.credential import DirectCredentialProvider
    from airweave.platform.configs.auth import GitHubAuthConfig

    client = AsyncMock()
    client.get.return_value = response(REPO)
    auth = DirectCredentialProvider(
        GitHubAuthConfig(personal_access_token="ghp_synthetic_for_test")
    )
    reader = GitHubReader(client, auth)
    assert await reader.object("/repos/team/repo") == REPO
    assert (
        client.get.call_args.kwargs["headers"]["Authorization"] == "Bearer ghp_synthetic_for_test"
    )


def test_review_submission_is_not_invented_as_creation_time():
    item = GitHubSource._record(
        CompletedScope(record_type="review"),
        "1",
        {"native": {"id": 1, "submitted_at": "2026-09-30T10:00:00Z"}},
    )
    assert item.source_created_at is None and item.source_updated_at is None
    assert item.payload["native"]["submitted_at"] == "2026-09-30T10:00:00Z"


@pytest.mark.asyncio
async def test_transferred_issue_withdraws_old_scope_without_reading_new_comments():
    target = "https://api.github.com/repos/other/repo/issues/9"
    moved = {**ISSUE, "number": 9, "repository_url": "https://api.github.com/repos/other/repo"}
    capture, client = await source(
        response(REPO),
        response(REPO),
        response([ISSUE]),
        response(REPO),
        response({}, status=301, headers={"Location": target}),
        response(moved),
        response({}, status=301, headers={"Location": target}),
        response(moved),
    )
    root = await repository(capture)
    issue = saved(
        (
            await capture.capture_page(
                capture.child_scope(root, "issue"),
                ScanContinuation(),
                files=MagicMock(),
                parent=root,
            )
        ).records[0]
    )
    with pytest.raises(ScopeAccessLost) as error:
        await capture.capture_page(
            capture.child_scope(issue, "comment"),
            ScanContinuation(),
            files=MagicMock(),
            parent=issue,
        )
    assert error.value.removal_reason == "scope_removed"
    await capture.confirm_absent(issue)
    assert all("/comments" not in call.args[0] for call in client.get.call_args_list)
