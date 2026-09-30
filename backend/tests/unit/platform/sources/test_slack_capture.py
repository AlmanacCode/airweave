"""Slack native page contracts; durable recovery is tested separately against PostgreSQL."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.sources.slack import SlackApiError, SlackSource


def source():
    return SlackSource(
        auth=StaticTokenProvider("token"), logger=MagicMock(), http_client=MagicMock()
    )


@pytest.mark.asyncio
async def test_page_commits_history_before_threads_and_preserves_native_fields():
    connector = source()
    connector._get = AsyncMock(
        side_effect=[
            {"channels": [{"id": "C1", "unknown_native_field": "kept"}]},
            {
                "messages": [{"ts": "1", "reply_count": 1, "blocks": [{"type": "rich_text"}]}],
                "response_metadata": {"next_cursor": "next"},
                "has_more": True,
            },
            {"messages": [{"ts": "1.1", "thread_ts": "1", "text": "reply"}]},
            {"messages": [{"ts": "2", "files": [{"id": "F1"}]}]},
        ]
    )
    root = await connector.capture_page(
        CompletedScope(record_type="channel"), ScanContinuation(), files=MagicMock()
    )
    assert root.final and root.records[0].payload["unknown_native_field"] == "kept"
    scope = CompletedScope(record_type="message", container_id="C1")
    history = await connector.capture_page(scope, ScanContinuation(), files=MagicMock())
    assert history.records[0].payload["blocks"] == [{"type": "rich_text"}]
    assert history.continuation.value["pending_threads"] == ["1"] and not history.final
    replies = await connector.capture_page(scope, history.continuation, files=MagicMock())
    assert replies.records[0].payload["thread_ts"] == "1" and not replies.final
    last = await connector.capture_page(scope, replies.continuation, files=MagicMock())
    assert last.final and last.records[0].completeness == "partial"
    calls = connector._get.call_args_list
    assert calls[0].args[1]["types"] == "public_channel,private_channel,im,mpim"
    assert calls[2].args[0].endswith("conversations.replies")
    assert calls[3].args[1]["cursor"] == "next"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pages",
    [
        [{"messages": [], "has_more": True}],
        [{"messages": [], "response_metadata": {"next_cursor": "repeat"}}] * 2,
    ],
)
async def test_incomplete_pagination_fails(pages):
    connector = source()
    connector._get = AsyncMock(side_effect=pages)
    progress = ScanContinuation()
    with pytest.raises(ValueError):
        for _ in pages:
            result = await connector.capture_page(
                CompletedScope(record_type="message", container_id="C1"),
                progress,
                files=MagicMock(),
            )
            progress = result.continuation


@pytest.mark.asyncio
async def test_prior_channel_loss_requires_explicit_provider_confirmation():
    connector = source()
    connector._get = AsyncMock(side_effect=SlackApiError("channel_not_found"))
    await connector.confirm_absent(
        SourceRecord.model_construct(identity=RecordIdentity(record_type="channel", native_id="C1"))
    )
    connector._get = AsyncMock(return_value={"channel": {"id": "C1"}})
    with pytest.raises(ValueError, match="omitted"):
        await connector.confirm_absent(
            SourceRecord.model_construct(
                identity=RecordIdentity(record_type="channel", native_id="C1")
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["json", "http", "proxy"])
async def test_rate_limit_honors_entire_wait_and_preserves_proxy_type(kind):
    import httpx

    from airweave.domains.auth_provider.exceptions import AuthProviderRateLimitError

    connector = source()
    request = httpx.Request("GET", "https://slack.com/api/conversations.list")
    first = (
        AuthProviderRateLimitError(provider_name="composio", retry_after=240)
        if kind == "proxy"
        else httpx.Response(
            429 if kind == "http" else 200,
            headers={"Retry-After": "240"},
            json={"ok": False, "error": "ratelimited"},
            request=request,
        )
    )
    connector.http_client.get = AsyncMock(
        side_effect=[first, httpx.Response(200, json={"ok": True}, request=request)]
    )
    waits = AsyncMock()
    get = connector._get.retry_with(sleep=waits)
    assert await get(connector, str(request.url)) == {"ok": True}
    assert float(waits.call_args.args[0]) == 240
    assert connector.http_client.get.await_count == 2


@pytest.mark.asyncio
async def test_proxy_rate_limit_stops_after_five_attempts():
    from airweave.domains.auth_provider.exceptions import AuthProviderRateLimitError

    connector = source()
    connector.http_client.get = AsyncMock(
        side_effect=AuthProviderRateLimitError(provider_name="composio", retry_after=240)
    )
    waits = AsyncMock()
    with pytest.raises(AuthProviderRateLimitError):
        await connector._get.retry_with(sleep=waits)(connector, "https://slack.com/api/test")
    assert connector.http_client.get.await_count == 5
    assert waits.await_count == 4
    assert all(float(call.args[0]) == 240 for call in waits.call_args_list)
