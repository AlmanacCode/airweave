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
            {"data": {"notes": [], "has_more": False}, "error": None},
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
    result = await connector._body("meeting", "m")
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
        await connector._body("meeting", "m")


@pytest.mark.asyncio
async def test_explicit_tool_failure_never_becomes_partial_success(monkeypatch):
    connector = await source()
    monkeypatch.setattr(
        connector,
        "_post",
        AsyncMock(side_effect=[{"session_id": "session"}, {"data": {}, "error": "failed"}]),
    )
    with pytest.raises(ValueError, match="tool execution failed; capture is incomplete"):
        await connector._body("meeting", "m")


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
        await connector._body("meeting", "m")
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
        await connector._body("meeting", "m")
    assert post.await_count == 2
    assert failure.value.source_short_name == "wispr"
    assert failure.value.status_code is None
    assert "private" not in str(failure.value).lower()
    assert "retry delay are unknown" in str(failure.value)
    # Do not turn unstructured tool text into credential failure or a timed retry.
    assert classify_error(failure.value).category is None


@pytest.mark.asyncio
async def test_scratchpad_ranges_preserve_normalized_text_and_native_fields(monkeypatch):
    connector = await source()
    execute = AsyncMock(
        side_effect=[
            {
                "id": "n",
                "title": "Note",
                "modified_at": "2026-09-30T00:00:00Z",
                "content": (
                    "abc\n(...truncated, 3 chars remaining; "
                    "continue with view_content.start_char=3...)"
                ),
                "future_native": {"retained": True},
            },
            {"id": "n", "title": "Note", "modified_at": "2026-09-30T00:00:00Z", "content": "def"},
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    result = await connector._body("scratchpad_note", "n")
    assert len(result["responses"]) == 2
    assert result["responses"][0]["response"]["future_native"] == {"retained": True}
    assert execute.call_args_list[0].args == (
        "WISPR_FLOW_MCP_GET_SCRATCHPAD_NOTE",
        {"note_id": "n", "view_content": {"char_limit": 40000, "start_char": 0}},
    )
    assert execute.call_args.args[1]["view_content"]["start_char"] == 3
    assert all("view_transcript" not in call.args[1] for call in execute.call_args_list)


@pytest.mark.asyncio
async def test_scratchpad_partitions_by_modified_time_and_keeps_native_listing(monkeypatch):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    rows = [
        {"id": "n1", "modified_at": "2026-01-01T00:00:00Z", "content_excerpt": "original"},
        {"id": "n2", "modified_at": "2026-01-03T00:00:00Z", "content_excerpt": "next"},
    ]
    execute = AsyncMock(
        side_effect=[
            {"notes": rows, "has_more": True, "truncated": True},
            {"notes": rows[:1], "has_more": False},
            {"notes": rows[1:], "has_more": False},
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    scope = CompletedScope(record_type="scratchpad_listing")
    first = await connector.capture_page(scope, ScanContinuation(), files=MagicMock())
    assert first.records[0].payload == rows[0] and not first.final
    assert first.records[0].identity.record_type == "scratchpad_listing"
    second = await connector.capture_page(scope, first.continuation, files=MagicMock())
    third = await connector.capture_page(scope, second.continuation, files=MagicMock())
    assert third.final
    assert execute.call_args_list[1].args[1]["until"] == "2026-01-02T00:00:00+00:00"
    assert execute.call_args_list[2].args[1]["since"] == "2026-01-02T00:00:00+00:00"
    assert connector.capture_cycle_configuration.policy("scratchpad_listing") == "discovery_only"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second",
    [
        {"id": "wrong", "modified_at": "same", "content": "def"},
        {"id": "n", "modified_at": "changed", "content": "def"},
        {"id": "n", "modified_at": "same", "content": None},
    ],
)
async def test_scratchpad_mismatched_or_changed_ranges_fail(monkeypatch, second):
    connector = await source()
    execute = AsyncMock(
        side_effect=[
            {
                "id": "n",
                "modified_at": "same",
                "content": (
                    "abc\n(...truncated, 3 chars remaining; "
                    "continue with view_content.start_char=3...)"
                ),
            },
            second,
        ]
    )
    monkeypatch.setattr(connector, "_execute", execute)
    with pytest.raises(ValueError):
        await connector._body("scratchpad_note", "n")


@pytest.mark.asyncio
async def test_scratchpad_cap_never_substitutes_meeting_start_for_modified_time(monkeypatch):
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import ScanContinuation

    connector = await source()
    monkeypatch.setattr(
        connector,
        "_execute",
        AsyncMock(
            return_value={
                "notes": [
                    {"id": "n1", "start": "2026-01-01T00:00:00Z"},
                    {"id": "n2", "start": "2026-01-03T00:00:00Z"},
                ],
                "has_more": True,
                "truncated": True,
            }
        ),
    )
    with pytest.raises(ValueError, match="cannot be partitioned safely"):
        await connector.capture_page(
            CompletedScope(record_type="scratchpad_listing"), ScanContinuation(), files=MagicMock()
        )


@pytest.mark.asyncio
async def test_explicit_no_transcript_preserves_notes_and_native_null(monkeypatch):
    connector = await source()
    native = {"id": "m", "content": "available notes", "has_transcript": False, "transcript": None}
    execute = AsyncMock(return_value=native)
    monkeypatch.setattr(connector, "_execute", execute)
    result = await connector._body("meeting", "m")
    assert result["responses"][0]["response"] == native
    assert execute.await_count == 1
    for ambiguous in (
        {**native, "has_transcript": True},
        {k: v for k, v in native.items() if k != "transcript"},
    ):
        execute.return_value = ambiguous
        with pytest.raises(ValueError, match="not a string"):
            await connector._body("meeting", "m")


@pytest.mark.asyncio
async def test_transcript_disappearing_after_first_range_stays_incomplete(monkeypatch):
    connector = await source()
    monkeypatch.setattr(
        connector,
        "_execute",
        AsyncMock(
            side_effect=[
                {
                    "id": "m",
                    "modified_at": "2026-10-01T00:00:00Z",
                    "content": "notes",
                    "has_transcript": True,
                    "transcript": (
                        "abc\n(...truncated, 3 chars remaining; "
                        "continue with view_transcript.start_char=3...)"
                    ),
                },
                {
                    "id": "m",
                    "modified_at": "2026-10-01T00:00:00Z",
                    "has_transcript": False,
                    "transcript": None,
                },
            ]
        ),
    )
    with pytest.raises(ValueError, match="not a string"):
        await connector._body("meeting", "m")


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [{}, {"modified_at": None}, {"modified_at": ""}])
async def test_multirange_body_requires_version_before_following_continuation(version):
    connector = await source()
    connector._execute = AsyncMock(
        return_value={
            "id": "m",
            **version,
            "content": "notes",
            "transcript": "abc\n(...truncated, 3 chars remaining; "
            "continue with view_transcript.start_char=3...)",
        }
    )
    with pytest.raises(ValueError, match="no usable version"):
        await connector._body("meeting", "m")
    connector._execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 400, 401, 429, 500])
async def test_streamed_response_limit_closes_success_and_error(monkeypatch, status):
    import httpx

    from airweave.domains.storage import FileSkippedException
    from airweave.platform.sources import wispr

    class Stream(httpx.AsyncByteStream):
        closed = False

        def __init__(self):
            self.iterator = self.chunks()

        async def chunks(self):
            yield b"x" * 16
            yield b"never needed"

        def __aiter__(self):
            return self.iterator

        async def aclose(self):
            await self.iterator.aclose()
            self.closed = True

    stream = Stream()
    monkeypatch.setattr(wispr, "MAX_RESPONSE_BYTES", 8)
    monkeypatch.setattr(wispr, "MAX_ERROR_BYTES", 8)
    connector = await source()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status, stream=stream))
    ) as client:
        connector._http_client = client
        with pytest.raises(Exception) as caught:
            await connector._post("session", {})
    assert not isinstance(caught.value, FileSkippedException)
    assert stream.closed
    expected = {
        200: "SourceError",
        400: "ComposioProxyError",
        401: "AuthProviderAuthError",
        429: "AuthProviderRateLimitError",
        500: "AuthProviderServerError",
    }
    assert type(caught.value).__name__ == expected[status]


@pytest.mark.asyncio
async def test_nonidentity_encoding_is_rejected_and_valid_unicode_preserved():
    import json

    import httpx

    from airweave.domains.sources.exceptions import SourceError

    connector = await source()
    value = {"data": {"text": "你好 café 🎵"}, "error": None}

    def respond(request):
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(200, content=json.dumps(value, ensure_ascii=False).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        connector._http_client = client
        assert await connector._post("session", {}) == value
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"content-encoding": "br"}, content=b"")
        )
    ) as client:
        connector._http_client = client
        with pytest.raises(SourceError, match="identity encoding"):
            await connector._post("session", {})


@pytest.mark.asyncio
async def test_aggregate_archive_limit_counts_native_metadata(monkeypatch):
    from airweave.domains.sources.exceptions import SourceError
    from airweave.platform.sources import wispr

    connector = await source()
    first = {
        "id": "m",
        "modified_at": "same",
        "content": "你好",
        "native_metadata": "x" * 250,
        "transcript": (
            "(...truncated, 3 chars remaining; continue with view_transcript.start_char=3...)"
        ),
    }
    second = {
        "id": "m",
        "modified_at": "same",
        "transcript": "🎵 café",
        "native_metadata": "y" * 250,
    }
    execute = AsyncMock(side_effect=[first, second])
    monkeypatch.setattr(connector, "_execute", execute)
    monkeypatch.setattr(wispr, "MAX_BODY_BYTES", 700)
    with pytest.raises(SourceError, match="body exceeds byte limit"):
        await connector._body("meeting", "m")
    assert execute.await_count == 2
    execute.side_effect = [first, second]
    monkeypatch.setattr(wispr, "MAX_BODY_BYTES", 2000)
    captured = await connector._body("meeting", "m")
    assert [part["response"] for part in captured["responses"]] == [first, second]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, expected", [(401, "AuthProviderAuthError"), (429, "AuthProviderRateLimitError")]
)
async def test_encoded_error_preserves_header_status_failure(status, expected):
    import httpx

    connector = await source()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status, headers={"content-encoding": "br"}, content=b"")
        )
    ) as client:
        connector._http_client = client
        with pytest.raises(Exception) as caught:
            await connector._post("session", {})
        assert type(caught.value).__name__ == expected
