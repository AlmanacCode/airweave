"""Wispr session binding, query caps, and exact transcript continuations."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.sources.token_providers.protocol import ManagedToolAuthProvider
from airweave.platform.sources.wispr import WisprSource


async def source():
    return await WisprSource.create(
        auth=ManagedToolAuthProvider(
            api_key="key", connected_account_id="ca_bound", user_id="user"
        ),
        logger=MagicMock(),
        http_client=MagicMock(),
    )


@pytest.mark.asyncio
async def test_session_is_bound_and_never_auto_connects(monkeypatch):
    connector = await source()
    post = AsyncMock(
        side_effect=[
            {"session_id": "session"},
            {"data": {"meetings": [], "has_more": False}, "error": None},
        ]
    )
    monkeypatch.setattr(connector, "_post", post)
    await connector.validate()
    payload = post.call_args_list[0].args[1]
    assert payload["connected_accounts"] == {"wispr_flow_mcp": ["ca_bound"]}
    assert payload["manage_connections"] == {"enable": False}
    assert "account" not in post.call_args_list[1].args[1]


@pytest.mark.asyncio
async def test_preserves_native_ranges_and_continues_exact_offset(monkeypatch):
    connector = await source()
    execute = AsyncMock(
        side_effect=[
            {
                "id": "m",
                "modified_at": "same",
                "content": "notes",
                "transcript": (
                    "abc\n(...truncated, 3 chars remaining; "
                    "continue with view_transcript.start_char=3...)\n\nProvider guidance follows."
                ),
                "native_extra": True,
            },
            {"id": "m", "modified_at": "same", "transcript": "def", "content": "notes"},
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    result = await connector._meeting("m")
    assert len(result["responses"]) == 2
    assert result["responses"][0]["response"]["native_extra"] is True
    assert execute.call_args.args[1]["view_transcript"]["start_char"] == 3


@pytest.mark.asyncio
async def test_changed_meeting_fails_instead_of_mixing_versions(monkeypatch):
    connector = await source()
    execute = AsyncMock(
        side_effect=[
            {
                "id": "m",
                "modified_at": "old",
                "content": "",
                "transcript": (
                    "(...truncated, 2 chars remaining; "
                    "continue with view_transcript.start_char=1...)"
                ),
            },
            {"id": "m", "modified_at": "new", "transcript": "new"},
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    with pytest.raises(ValueError, match="changed"):
        await connector._meeting("m")


@pytest.mark.asyncio
async def test_query_cap_partitions_by_meeting_start_not_modified(monkeypatch):
    connector = await source()
    row1 = {"id": "1", "start": "2026-01-01T00:00:00Z", "modified_at": "2026-09-01T00:00:00Z"}
    row2 = {"id": "2", "start": "2026-01-03T00:00:00Z"}
    listing = AsyncMock(side_effect=[([row1, row2], True), ([row2], False), ([row1], False)])
    monkeypatch.setattr(connector, "_list_window", listing)
    assert {row["id"] async for row in connector._list_all()} == {"1", "2"}
    assert listing.call_args_list[1].args[0].isoformat() == "2026-01-02T00:00:00+00:00"


@pytest.mark.asyncio
async def test_unpartitionable_cap_fails(monkeypatch):
    connector = await source()
    monkeypatch.setattr(
        connector,
        "_list_window",
        AsyncMock(return_value=([{"start": "2026-01-01T00:00:00Z"}], True)),
    )
    with pytest.raises(ValueError, match="cap"):
        _ = [row async for row in connector._list_all()]
