"""Slack native page contracts; durable recovery is tested separately against PostgreSQL."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackApiError, SlackSource


async def source():
    connector = SlackSource(
        auth=StaticTokenProvider("token"), logger=MagicMock(), http_client=MagicMock()
    )
    connector.slack_config = SlackConfig(expected_team_id="T1", expected_user_id="U1")
    with patch.object(
        connector, "_get", AsyncMock(return_value={"ok": True, "team_id": "T1", "user_id": "U1"})
    ):
        await connector.validate()
    return connector


@pytest.mark.asyncio
async def test_page_commits_history_before_threads_and_preserves_native_fields():
    connector = await source()
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
    connector = await source()
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
    connector = await source()
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

    connector = await source()
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

    connector = await source()
    connector.http_client.get = AsyncMock(
        side_effect=AuthProviderRateLimitError(provider_name="composio", retry_after=240)
    )
    waits = AsyncMock()
    with pytest.raises(AuthProviderRateLimitError):
        await connector._get.retry_with(sleep=waits)(connector, "https://slack.com/api/test")
    assert connector.http_client.get.await_count == 5
    assert waits.await_count == 4
    assert all(float(call.args[0]) == 240 for call in waits.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code, expected",
    [
        ("thread_not_found", "restart"),
        ("channel_not_found", "access"),
        ("not_in_channel", "access"),
        ("missing_scope", "error"),
        ("internal_error", "error"),
    ],
)
async def test_queued_thread_failure_preserves_error_meaning(code, expected):
    from airweave.domains.entities.canonical.page_source import (
        InvalidScanContinuation,
        ScopeAccessLost,
    )

    connector = await source()
    connector._get = AsyncMock(side_effect=SlackApiError(code))
    error = {"restart": InvalidScanContinuation, "access": ScopeAccessLost, "error": SlackApiError}[
        expected
    ]
    with pytest.raises(error):
        await connector.capture_page(
            CompletedScope(record_type="message", container_id="C1"),
            ScanContinuation(value={"pending_threads": ["1"]}),
            files=MagicMock(),
        )
    assert connector._get.await_count == 1


@pytest.mark.asyncio
async def test_thread_not_found_outside_reply_fetch_does_not_invalidate_inventory():
    connector = await source()
    connector._get = AsyncMock(side_effect=SlackApiError("thread_not_found"))
    with pytest.raises(SlackApiError):
        await connector.capture_page(
            CompletedScope(record_type="message", container_id="C1"),
            ScanContinuation(),
            files=MagicMock(),
        )


@pytest.mark.parametrize(
    "native,created,updated",
    [
        (
            {"ts": "1355517523.000005", "edited": {"ts": "1355517536.000001"}},
            "2012-12-14T20:38:43.000005+00:00",
            "2012-12-14T20:38:56.000001+00:00",
        ),
        (
            {"ts": "1355517523.000005", "latest_reply": "1855517536.999999"},
            "2012-12-14T20:38:43.000005+00:00",
            None,
        ),
        ({"ts": "invalid", "edited": {"ts": "NaN"}}, None, None),
        ({"ts": "999999999999.123456", "edited": {"ts": True}}, None, None),
    ],
)
def test_message_native_dates_preserve_precision_and_unknowns(native, created, updated):
    record = SlackSource._capture_message(native, "C1")
    assert record.payload == native
    assert (record.source_created_at.isoformat() if record.source_created_at else None) == created
    assert (record.source_updated_at.isoformat() if record.source_updated_at else None) == updated


@pytest.mark.asyncio
async def test_conversation_dates_use_distinct_native_units():
    connector = await source()
    connector._get = AsyncMock(
        return_value={
            "channels": [
                {"id": "C1", "created": 1449252889, "updated": 1689965803820},
                {"id": "C2", "created": True, "updated": "1689965803820"},
                {"id": "C3"},
            ]
        }
    )
    result = await connector.capture_page(
        CompletedScope(record_type="channel"), ScanContinuation(), files=MagicMock()
    )
    first, malformed, absent = result.records
    assert first.source_created_at.isoformat() == "2015-12-04T18:14:49+00:00"
    assert first.source_updated_at.isoformat() == "2023-07-21T18:56:43.820000+00:00"
    assert malformed.source_created_at is malformed.source_updated_at is None
    assert absent.source_created_at is absent.source_updated_at is None
