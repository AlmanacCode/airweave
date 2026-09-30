"""Thread preservation and failure-safe Slack capture."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.sources.slack import SlackSource


def source():
    return SlackSource(
        auth=StaticTokenProvider("token"), logger=MagicMock(), http_client=MagicMock()
    )


@pytest.mark.asyncio
async def test_channel_pagination_threads_and_native_payload(monkeypatch):
    connector = source()
    calls = []

    async def get(url, params):
        calls.append((url, params))
        if url.endswith("conversations.list"):
            return {"channels": [{"id": "C1", "unknown_native_field": "kept"}]}
        if url.endswith("conversations.history"):
            if params.get("cursor"):
                return {"messages": [{"ts": "2", "text": "second", "files": [{"id": "F1"}]}]}
            return {
                "messages": [{"ts": "1", "reply_count": 1, "blocks": [{"type": "rich_text"}]}],
                "response_metadata": {"next_cursor": "next"},
                "has_more": True,
            }
        return {
            "messages": [
                {"ts": "1", "reply_count": 1},
                {"ts": "1.1", "thread_ts": "1", "text": "reply"},
            ]
        }

    monkeypatch.setattr(connector, "_get", get)
    observations = [x async for x in connector.generate_observations()]
    records = [x for x in observations if isinstance(x, CaptureRecord)]
    assert records[0].payload["unknown_native_field"] == "kept"
    assert any(x.identity.native_id == "1.1" and x.payload["thread_ts"] == "1" for x in records)
    assert records[-1].completeness == "partial"
    assert CompletedScope(record_type="message", container_id="C1") in observations
    assert observations[-1] == CompletedScope(record_type="channel")
    assert calls[0][1]["types"] == "public_channel,private_channel,im,mpim"


@pytest.mark.asyncio
async def test_failed_thread_never_completes_scope(monkeypatch):
    connector = source()
    get = AsyncMock(
        side_effect=[
            {"channels": [{"id": "C1"}]},
            {"messages": [{"ts": "1", "reply_count": 1}]},
            RuntimeError("permission lost"),
        ]
    )
    monkeypatch.setattr(connector, "_get", get)
    emitted = []
    with pytest.raises(RuntimeError, match="permission"):
        async for observation in connector.generate_observations():
            emitted.append(observation)
    assert not any(type(x) is CompletedScope for x in emitted)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pages",
    [
        [{"messages": [], "has_more": True}],
        [{"messages": [], "response_metadata": {"next_cursor": "repeat"}}] * 2,
    ],
)
async def test_incomplete_pagination_fails(monkeypatch, pages):
    connector = source()
    monkeypatch.setattr(connector, "_get", AsyncMock(side_effect=pages))
    with pytest.raises(ValueError):
        _ = [x async for x in connector._capture_pages("conversations.history", "messages", {})]


@pytest.mark.asyncio
async def test_prior_channel_loss_requires_explicit_provider_confirmation(monkeypatch):
    from uuid import uuid4

    from airweave.domains.entities.canonical.requests import RemovedScope
    from airweave.domains.syncs.cursors.cursor import SyncCursor
    from airweave.platform.sources.slack import SlackApiError

    connector = source()
    cursor = SyncCursor(uuid4(), cursor_data={"channel_ids": ["C1"]})
    monkeypatch.setattr(
        connector,
        "_get",
        AsyncMock(side_effect=[{"channels": []}, SlackApiError("channel_not_found")]),
    )
    observations = [x async for x in connector.generate_observations(cursor=cursor)]
    assert observations[1].kind == "delete"
    assert observations[1].removal_reason == "access_revoked"
    assert isinstance(observations[2], RemovedScope)
    assert cursor.get()["channel_ids"] == []


@pytest.mark.asyncio
async def test_omitted_but_accessible_channel_does_not_advance_cursor(monkeypatch):
    from uuid import uuid4

    from airweave.domains.syncs.cursors.cursor import SyncCursor

    connector = source()
    cursor = SyncCursor(uuid4(), cursor_data={"channel_ids": ["C1"]})
    monkeypatch.setattr(
        connector, "_get", AsyncMock(side_effect=[{"channels": []}, {"channel": {"id": "C1"}}])
    )
    with pytest.raises(ValueError, match="omitted"):
        _ = [x async for x in connector.generate_observations(cursor=cursor)]
    assert cursor.get()["channel_ids"] == ["C1"]
