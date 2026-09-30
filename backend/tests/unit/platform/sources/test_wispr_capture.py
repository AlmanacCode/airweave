"""Wispr session binding, query caps, and exact transcript continuations."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.sources.exceptions import SourceServerError
from airweave.domains.sources.exceptions.classifier import classify_error
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
async def test_explicit_tool_failure_never_becomes_partial_success(monkeypatch):
    connector = await source()
    monkeypatch.setattr(
        connector,
        "_post",
        AsyncMock(side_effect=[{"session_id": "session"}, {"data": {}, "error": "failed"}]),
    )
    with pytest.raises(ValueError, match="tool execution failed; capture is incomplete"):
        await connector._meeting("m")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {"meetings": [], "has_more": False, "truncated": True},
        {"meetings": [], "has_more": True, "next_cursor": None},
        {"meetings": [{"id": "m"}, {"id": "m"}], "has_more": False},
    ],
)
async def test_listing_failure_never_commits_partial_final_page(monkeypatch, response):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    monkeypatch.setattr(connector, "_execute", AsyncMock(return_value=response))
    with pytest.raises(ValueError):
        await connector.capture_page(
            CompletedScope(record_type="meeting_listing"), ScanContinuation(), files=MagicMock()
        )


@pytest.mark.asyncio
async def test_listing_preserves_native_row_and_continuation(monkeypatch):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    row = {"id": "m", "title": "Meeting", "unknown_native": True, "start": "2026-01-01T00:00:00Z"}
    execute = AsyncMock(return_value={"meetings": [row], "has_more": True, "next_cursor": "next"})
    monkeypatch.setattr(connector, "_execute", execute)
    page = await connector.capture_page(
        CompletedScope(record_type="meeting_listing"), ScanContinuation(), files=MagicMock()
    )
    assert page.records[0].payload == row
    assert page.records[0].completeness == "metadata_only"
    assert page.records[0].source_created_at is None
    assert page.continuation.value["cursor"] == "next"
    assert page.final is False
    with pytest.raises(ValueError, match="repeated pagination"):
        await connector.capture_page(
            CompletedScope(record_type="meeting_listing"), page.continuation, files=MagicMock()
        )


@pytest.mark.asyncio
async def test_listing_projection_is_explicitly_empty():
    from types import SimpleNamespace

    from airweave.domains.entities.canonical.projection_mappers import _wispr

    assert _wispr(SimpleNamespace(identity=SimpleNamespace(record_type="meeting_listing"))) == ()


@pytest.mark.asyncio
async def test_partitioned_listing_resumes_both_half_open_windows(monkeypatch):
    from airweave.domains.entities.canonical.page_source import CanonicalPageSource
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    assert isinstance(connector, CanonicalPageSource)
    rows = [
        {"id": "a", "start": "2026-01-01T00:00:00Z"},
        {"id": "b", "start": "2026-01-03T00:00:00Z"},
    ]
    execute = AsyncMock(
        side_effect=[
            {"meetings": rows, "has_more": True, "truncated": True},
            {"meetings": rows[:1], "has_more": False},
            {"meetings": rows[1:], "has_more": False},
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    scope = CompletedScope(record_type="meeting_listing")
    first = await connector.capture_page(scope, ScanContinuation(), files=MagicMock())
    assert not first.final
    # Round-trip persisted progress into a newly constructed source.
    resumed = await source()
    monkeypatch.setattr(resumed, "_execute", execute)
    second = await resumed.capture_page(
        scope,
        ScanContinuation.model_validate_json(first.continuation.model_dump_json()),
        files=MagicMock(),
    )
    assert not second.final
    third = await resumed.capture_page(scope, second.continuation, files=MagicMock())
    assert third.final
    assert execute.call_args_list[1].args[1]["until"] == "2026-01-02T00:00:00+00:00"
    assert execute.call_args_list[2].args[1]["since"] == "2026-01-02T00:00:00+00:00"
    assert third.records[0].identity.native_id == "b"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [{"id": "a"}],
        [
            {"id": "a", "start": "2026-01-01T00:00:00Z"},
            {"id": "b", "start": "2026-01-01T00:00:00Z"},
        ],
    ],
)
async def test_capped_unsplittable_listing_fails_without_final_page(monkeypatch, rows):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    monkeypatch.setattr(
        connector,
        "_execute",
        AsyncMock(
            return_value={
                "meetings": rows,
                "has_more": False,
                "truncated": True,
            }
        ),
    )
    with pytest.raises(ValueError, match="cannot be partitioned safely"):
        await connector.capture_page(
            CompletedScope(record_type="meeting_listing"), ScanContinuation(), files=MagicMock()
        )


@pytest.mark.asyncio
async def test_uncapped_listing_does_not_invent_missing_dates(monkeypatch):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    monkeypatch.setattr(
        connector,
        "_execute",
        AsyncMock(
            return_value={
                "meetings": [{"id": "a"}],
                "has_more": False,
            }
        ),
    )
    page = await connector.capture_page(
        CompletedScope(record_type="meeting_listing"), ScanContinuation(), files=MagicMock()
    )
    assert page.final
    assert page.records[0].source_created_at is None
    assert page.records[0].source_updated_at is None


@pytest.mark.asyncio
async def test_short_listing_pages_are_not_limited_to_six_cursors(monkeypatch):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    execute = AsyncMock(
        side_effect=[
            {"meetings": [{"id": str(i)}], "has_more": i < 8, "next_cursor": str(i + 1)}
            for i in range(9)
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    continuation = ScanContinuation()
    for _ in range(9):
        page = await connector.capture_page(
            CompletedScope(record_type="meeting_listing"), continuation, files=MagicMock()
        )
        continuation = page.continuation
    assert page.final


@pytest.mark.asyncio
async def test_ambiguous_continuation_markers_do_not_choose_a_range(monkeypatch):
    connector = await source()
    transcript = (
        "Quoted example:\n(...truncated, 10 chars remaining; "
        "continue with view_transcript.start_char=10...)\n"
        "Actual meeting text follows.\n(...truncated, 20 chars remaining; "
        "continue with view_transcript.start_char=40000...)\nProvider guidance follows."
    )
    execute = AsyncMock(
        return_value={
            "id": "m",
            "modified_at": "same",
            "content": "notes",
            "transcript": transcript,
        }
    )
    monkeypatch.setattr(connector, "_execute", execute)
    with pytest.raises(ValueError, match="ambiguous continuation"):
        await connector._meeting("m")
    assert execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", ["Private meeting: rate limit exceeded", "Too many requests: private detail"]
)
async def test_rate_signal_stops_without_retry_or_invented_status(monkeypatch, error):
    connector = await source()
    post = AsyncMock(side_effect=[{"session_id": "session"}, {"data": {}, "error": error}])
    monkeypatch.setattr(connector, "_post", post)
    with pytest.raises(SourceServerError, match="rate-limit signal") as failure:
        await connector._meeting("m")
    assert post.await_count == 2
    assert failure.value.source_short_name == "wispr"
    assert failure.value.status_code is None
    assert "private" not in str(failure.value).lower()
    assert "retry delay are unknown" in str(failure.value)
    # Do not turn unstructured tool text into credential failure or a timed retry.
    assert classify_error(failure.value).category is None
