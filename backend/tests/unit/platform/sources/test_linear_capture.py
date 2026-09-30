"""Credential-free tests of exact Linear originals and whole-scope page authority."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.page_source import ScopeAccessLost
from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceError, SourceRateLimitError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.platform.configs.config import LinearConfig
from airweave.platform.sources.linear import LinearSource

WORKSPACE = UUID(int=1)
TEAM = UUID(int=2)
ISSUE = UUID(int=3)
COMMENT = UUID(int=4)
STAMP = "2026-09-30T00:00:00Z"


def connection(nodes, cursor=None):
    return {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}


def node(identity=ISSUE):
    return {
        "id": str(identity),
        "team": {"id": str(TEAM)},
        "createdAt": STAMP,
        "updatedAt": STAMP,
        "description": "Original **Markdown**",
        "unknown": {"kept": True},
    }


def response(data, *, errors=None, status=200, headers=None):
    return httpx.Response(
        status,
        json={"data": data, **({"errors": errors} if errors else {})},
        headers=headers,
        request=httpx.Request("POST", "https://api.linear.app/graphql"),
    )


async def source(*pages, workspace=WORKSPACE, config=None):
    client = AsyncMock()
    client.post.side_effect = [
        response({"organization": {"id": str(workspace)}, "viewer": {"id": str(UUID(int=9))}}),
        response({"teams": connection([{"id": str(TEAM)}])}),
        *pages,
    ]
    auth = ManagedAuthProvider(
        api_key="test-only",
        connected_account_id="ca_test",
        allowed_hosts=frozenset({"api.linear.app"}),
    )
    result = await LinearSource.create(
        auth=auth,
        logger=MagicMock(),
        http_client=client,
        config=config or LinearConfig(workspace_id=WORKSPACE, team_ids=(TEAM,)),
    )
    return result, client


@pytest.mark.asyncio
async def test_native_payload_pagination_and_managed_auth():
    original = node()
    capture, client = await source(
        response({"issues": connection([original], "next")}), response({"issues": connection([])})
    )
    first = await capture.capture_page(
        CompletedScope(record_type="issue"), ScanContinuation(), files=MagicMock()
    )
    assert not first.final
    assert first.records[0].payload == original
    assert first.records[0].identity.native_id == str(ISSUE)
    second = await capture.capture_page(
        CompletedScope(record_type="issue"), first.continuation, files=MagicMock()
    )
    assert second.final
    assert client.post.call_args.kwargs["json"]["variables"]["after"] == "next"
    assert client.post.call_args.kwargs["headers"] == {}


@pytest.mark.asyncio
async def test_children_preserve_reply_and_independent_edit_without_parent_timestamp():
    original = {
        **node(COMMENT),
        "issue": {"id": str(ISSUE)},
        "parent": {"id": str(UUID(int=8))},
        "body": "edited",
        "updatedAt": "2026-09-30T01:00:00Z",
    }
    capture, _ = await source(
        response(
            {
                "issue": {
                    "id": str(ISSUE),
                    "team": {"id": str(TEAM)},
                    "comments": connection([original]),
                }
            }
        )
    )
    page = await capture.capture_page(
        CompletedScope(record_type="comment", container_id=str(ISSUE)),
        ScanContinuation(),
        files=MagicMock(),
    )
    assert page.final and page.records[0].payload == original
    assert page.records[0].identity.container_id == str(ISSUE)
    assert page.records[0].source_updated_at.hour == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("issue", {"id": str(UUID(int=99))}), ("issue", None)])
async def test_wrong_or_null_comment_parent_fails(field, value):
    original = {**node(COMMENT), field: value}
    capture, _ = await source(
        response(
            {
                "issue": {
                    "id": str(ISSUE),
                    "team": {"id": str(TEAM)},
                    "comments": connection([original]),
                }
            }
        )
    )
    with pytest.raises(ValueError):
        await capture.capture_page(
            CompletedScope(record_type="comment", container_id=str(ISSUE)),
            ScanContinuation(),
            files=MagicMock(),
        )


@pytest.mark.asyncio
async def test_partial_graphql_response_never_returns_a_page():
    capture, _ = await source(
        response(
            {"issues": connection([node()])},
            errors=[{"extensions": {"code": "INTERNAL_SERVER_ERROR"}}],
        )
    )
    with pytest.raises(SourceError):
        await capture.capture_page(
            CompletedScope(record_type="issue"), ScanContinuation(), files=MagicMock()
        )


@pytest.mark.asyncio
async def test_moved_child_scope_reports_scope_removed():
    capture, _ = await source(
        response(
            {
                "issue": {
                    "id": str(ISSUE),
                    "team": {"id": str(UUID(int=99))},
                    "comments": connection([]),
                }
            }
        )
    )
    with pytest.raises(ScopeAccessLost) as error:
        await capture.capture_page(
            CompletedScope(record_type="comment", container_id=str(ISSUE)),
            ScanContinuation(),
            files=MagicMock(),
        )
    assert error.value.removal_reason == "scope_removed"


@pytest.mark.asyncio
async def test_missing_issue_is_not_access_revoked_or_empty_success():
    capture, _ = await source(response({"issue": None}))
    with pytest.raises(ValidationError):
        await capture.capture_page(
            CompletedScope(record_type="comment", container_id=str(ISSUE)),
            ScanContinuation(),
            files=MagicMock(),
        )


@pytest.mark.asyncio
async def test_omitted_readable_selected_root_blocks_reconciliation():
    capture, _ = await source(
        response({"issue": node()}),
        response({"issue": {**node(), "team": {"id": str(UUID(int=99))}}}),
    )
    with pytest.raises(ValueError, match="still readable"):
        await capture.confirm_root_absent(str(ISSUE))
    await capture.confirm_root_absent(str(ISSUE))


@pytest.mark.asyncio
async def test_repeated_cursor_fails_without_final_page():
    capture, _ = await source(response({"issues": connection([node()], "same")}))
    with pytest.raises(ValueError, match="non-advancing"):
        await capture.capture_page(
            CompletedScope(record_type="issue"),
            ScanContinuation(value={"after": "same"}),
            files=MagicMock(),
        )


@pytest.mark.asyncio
async def test_workspace_mismatch_and_unavailable_team_fail_before_capture():
    with pytest.raises(ValueError, match="workspace"):
        await source(workspace=UUID(int=99))
    with pytest.raises(ValueError, match="teams are unavailable"):
        await source(config=LinearConfig(workspace_id=WORKSPACE, team_ids=(UUID(int=99),)))


def test_old_configuration_and_duplicates_fail_explicitly():
    with pytest.raises(ValidationError):
        LinearConfig()
    with pytest.raises(ValidationError):
        LinearConfig(workspace_id=WORKSPACE, team_ids=(TEAM, TEAM))


@pytest.mark.asyncio
async def test_config_fingerprint_and_binary_coverage():
    capture, _ = await source()
    original = {**node(), "description": "![image](https://uploads.linear.app/file)"}
    record = capture._capture(CompletedScope(record_type="issue"), original)
    assert record.completeness == "partial" and not record.blobs
    assert record.payload == original
    assert capture.canonical_container_parents == {"comment": "issue", "attachment": "issue"}
    assert len(capture.capture_cycle_configuration.fingerprint) == 64


@pytest.mark.asyncio
async def test_rate_limit_uses_provider_reset_without_accepting_partial_data():
    capture, _ = await source(
        response(
            None,
            errors=[{"extensions": {"code": "RATELIMITED"}}],
            status=400,
            headers={"X-RateLimit-Requests-Reset": "9999999999999"},
        )
    )
    # Exercise one attempt without sleeping or retrying an invented rate-limit response.
    with pytest.raises(SourceRateLimitError) as error:
        await LinearSource._query.__wrapped__(capture, "issues", {})
    assert error.value.retry_after > 60


def test_registry_detects_canonical_class_before_instance_creation():
    from airweave.domains.entities.canonical.page_source import CanonicalPageSource

    assert isinstance(LinearSource, CanonicalPageSource)


@pytest.mark.asyncio
async def test_managed_linear_binding_only_allows_graphql_origin(monkeypatch):
    from airweave.domains.auth_provider.exceptions import AuthProviderConfigError
    from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider

    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "test-only"},
        config={"account_id": "ca_test", "auth_config_id": "ac_test"},
    )
    fetch = AsyncMock(
        return_value={
            "toolkit": {"slug": "linear"},
            "status": "ACTIVE",
            "auth_config": {"id": "ac_test"},
        }
    )
    monkeypatch.setattr(provider, "_get_with_auth", fetch)
    result = await provider.get_auth_result("linear", ["access_token"])
    assert result.credentials is None
    assert result.managed_auth.allowed_hosts == {"api.linear.app", "uploads.linear.app"}
    fetch.return_value["toolkit"]["slug"] = "gmail"
    with pytest.raises(AuthProviderConfigError, match="toolkit"):
        await provider.get_auth_result("linear", ["access_token"])


@pytest.mark.asyncio
async def test_fresh_instance_resumes_exact_saved_cursor_with_same_scope_fingerprint():
    first_source, _ = await source(response({"issues": connection([node()], "saved-page")}))
    first = await first_source.capture_page(
        CompletedScope(record_type="issue"), ScanContinuation(), files=MagicMock()
    )
    fresh_source, client = await source(response({"issues": connection([])}))
    resumed = await fresh_source.capture_page(
        CompletedScope(record_type="issue"),
        ScanContinuation.model_validate_json(first.continuation.model_dump_json()),
        files=MagicMock(),
    )
    assert resumed.final
    assert client.post.call_args.kwargs["json"]["variables"]["after"] == "saved-page"
    assert fresh_source.capture_cycle_configuration == first_source.capture_cycle_configuration


@pytest.mark.asyncio
async def test_attachment_page_is_independent_and_does_not_claim_file_bytes():
    attachment = {
        **node(UUID(int=5)),
        "issue": {"id": str(ISSUE)},
        "url": "https://external.example/document",
        "metadata": {"native": True},
    }
    capture, _ = await source(
        response(
            {
                "issue": {
                    "id": str(ISSUE),
                    "team": {"id": str(TEAM)},
                    "attachments": connection([attachment]),
                }
            }
        )
    )
    page = await capture.capture_page(
        CompletedScope(record_type="attachment", container_id=str(ISSUE)),
        ScanContinuation(),
        files=MagicMock(),
    )
    assert page.records[0].payload == attachment
    assert page.records[0].completeness == "partial"
    assert page.records[0].blobs == ()


@pytest.mark.asyncio
async def test_retained_upload_is_stored_before_reference_without_mutating_original(
    monkeypatch, tmp_path
):
    import hashlib
    from contextlib import asynccontextmanager

    from airweave.domains.storage.file_service import FileService

    @asynccontextmanager
    async def stream(*args, **kwargs):
        assert args == ("GET", "https://uploads.linear.app/workspace/object")
        assert kwargs["follow_redirects"] is False
        assert kwargs["headers"] == {}
        yield httpx.Response(
            200,
            content=b"retained",
            headers={"content-type": "application/pdf"},
            request=httpx.Request("GET", args[1]),
        )

    capture, client = await source()
    client.stream = MagicMock(side_effect=stream)
    storage = AsyncMock()
    monkeypatch.setattr(FileService, "_ensure_base_dir", lambda self: None)
    files = FileService(sync_job_id=UUID(int=10), sync_id=UUID(int=11), storage_backend=storage)
    original = {**node(UUID(int=5)), "url": "https://uploads.linear.app/workspace/object"}
    retained = await capture._retain(
        CompletedScope(record_type="attachment", container_id=str(ISSUE)), original, files
    )
    assert retained.payload == original and retained.completeness == "complete"
    ref = retained.blobs[0]
    assert ref.sha256 == hashlib.sha256(b"retained").hexdigest()
    assert ref.source_path == "/url"
    assert ref.key.startswith(f"canonical/{UUID(int=11)}/blobs/sha256/")
    storage.write_file.assert_awaited_once_with(ref.key, b"retained")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "https://external.example/a",
        "http://uploads.linear.app/a",
        "https://uploads.linear.app.evil.example/a",
        "https://user@uploads.linear.app/a",
        "https://uploads.linear.app:444/a",
        "https://uploads.linear.app/a#fragment",
    ],
)
async def test_non_audited_file_url_never_receives_credentials(url):
    capture, client = await source()
    files = MagicMock()
    original = {**node(UUID(int=5)), "url": url}
    retained = await capture._retain(
        CompletedScope(record_type="attachment", container_id=str(ISSUE)), original, files
    )
    assert retained.completeness == "partial" and not retained.blobs
    client.stream.assert_not_called()
    files.store_canonical_blob.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,content,length", [(302, b"", "0"), (200, b"12345", "1"), (200, b"", "5")]
)
async def test_redirect_or_oversize_never_publishes_a_blob(monkeypatch, status, content, length):
    from contextlib import asynccontextmanager

    import airweave.platform.sources.linear as linear_module

    @asynccontextmanager
    async def stream(*args, **kwargs):
        assert kwargs["follow_redirects"] is False
        yield httpx.Response(
            status,
            content=content,
            headers={"content-length": length, "location": "https://external.example/"},
            request=httpx.Request("GET", args[1]),
        )

    capture, client = await source()
    client.stream = MagicMock(side_effect=stream)
    monkeypatch.setattr(linear_module, "MAX_FILE_SIZE_BYTES", 4)
    files = AsyncMock()
    original = {**node(UUID(int=5)), "url": "https://uploads.linear.app/a"}
    if status == 302:
        with pytest.raises(SourceError):
            await capture._retain(
                CompletedScope(record_type="attachment", container_id=str(ISSUE)), original, files
            )
    else:
        retained = await capture._retain(
            CompletedScope(record_type="attachment", container_id=str(ISSUE)), original, files
        )
        assert retained.completeness == "partial" and not retained.blobs
    files.store_canonical_blob.assert_not_called()


@pytest.mark.asyncio
async def test_file_storage_failure_prevents_page_success():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def stream(*args, **kwargs):
        yield httpx.Response(200, content=b"bytes", request=httpx.Request("GET", args[1]))

    attachment = {
        **node(UUID(int=5)),
        "issue": {"id": str(ISSUE)},
        "url": "https://uploads.linear.app/a",
    }
    capture, client = await source(
        response(
            {
                "issue": {
                    "id": str(ISSUE),
                    "team": {"id": str(TEAM)},
                    "attachments": connection([attachment]),
                }
            }
        )
    )
    client.stream = MagicMock(side_effect=stream)
    files = AsyncMock()
    files.store_canonical_blob.side_effect = OSError("fixture storage failure")
    with pytest.raises(OSError):
        await capture.capture_page(
            CompletedScope(record_type="attachment", container_id=str(ISSUE)),
            ScanContinuation(),
            files=files,
        )
