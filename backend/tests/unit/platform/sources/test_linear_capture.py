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
    first = await capture.capture_page(CompletedScope(record_type="issue"), ScanContinuation())
    assert not first.final
    assert first.records[0].payload == original
    assert first.records[0].identity.native_id == str(ISSUE)
    second = await capture.capture_page(CompletedScope(record_type="issue"), first.continuation)
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
        CompletedScope(record_type="comment", container_id=str(ISSUE)), ScanContinuation()
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
            CompletedScope(record_type="comment", container_id=str(ISSUE)), ScanContinuation()
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
        await capture.capture_page(CompletedScope(record_type="issue"), ScanContinuation())


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
            CompletedScope(record_type="comment", container_id=str(ISSUE)), ScanContinuation()
        )
    assert error.value.removal_reason == "scope_removed"


@pytest.mark.asyncio
async def test_missing_issue_is_not_access_revoked_or_empty_success():
    capture, _ = await source(response({"issue": None}))
    with pytest.raises(ValidationError):
        await capture.capture_page(
            CompletedScope(record_type="comment", container_id=str(ISSUE)), ScanContinuation()
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
            CompletedScope(record_type="issue"), ScanContinuation(value={"after": "same"})
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
    assert result.managed_auth.allowed_hosts == {"api.linear.app"}
    fetch.return_value["toolkit"]["slug"] = "gmail"
    with pytest.raises(AuthProviderConfigError, match="toolkit"):
        await provider.get_auth_result("linear", ["access_token"])


@pytest.mark.asyncio
async def test_fresh_instance_resumes_exact_saved_cursor_with_same_scope_fingerprint():
    first_source, _ = await source(response({"issues": connection([node()], "saved-page")}))
    first = await first_source.capture_page(CompletedScope(record_type="issue"), ScanContinuation())
    fresh_source, client = await source(response({"issues": connection([])}))
    resumed = await fresh_source.capture_page(
        CompletedScope(record_type="issue"),
        ScanContinuation.model_validate_json(first.continuation.model_dump_json()),
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
        CompletedScope(record_type="attachment", container_id=str(ISSUE)), ScanContinuation()
    )
    assert page.records[0].payload == attachment
    assert page.records[0].completeness == "partial"
    assert page.records[0].blobs == ()
