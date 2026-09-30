"""Calendar races and resets, with no provider mutation or live credentials."""

from copy import deepcopy
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RemovedScope,
    StartedScope,
)
from airweave.domains.sources.exceptions import SourceGoneError, SourceServerError
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.cursors.google_calendar import GoogleCalendarCursor
from airweave.platform.sources.records.google_calendar import generate_calendar_observations


class ScriptedGet:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    async def __call__(self, url, params):
        self.calls.append((url, deepcopy(params)))
        value = next(self.replies)
        if isinstance(value, Exception):
            raise value
        return value


def cursor(tokens=None):
    return SyncCursor(uuid4(), GoogleCalendarCursor, {"calendar_tokens": tokens or {}})


async def test_full_pagination_retains_sparse_cancellation_and_container():
    get = ScriptedGet(
        [
            {"items": [{"id": "team/a@example.com"}]},
            {
                "items": [{"id": "series", "recurrence": ["RRULE:FREQ=WEEKLY"]}],
                "nextPageToken": "page2",
            },
            {
                "items": [
                    {
                        "id": "exception",
                        "status": "cancelled",
                        "recurringEventId": "series",
                        "originalStartTime": {"date": "2026-10-01"},
                    }
                ],
                "nextSyncToken": "boundary",
            },
        ]
    )
    state = cursor()
    results = [item async for item in generate_calendar_observations(get, state)]
    events = [
        item
        for item in results
        if isinstance(item, CaptureRecord) and item.identity.record_type == "event"
    ]
    assert all(item.identity.container_id == "team/a@example.com" for item in events)
    assert events[1].kind == "upsert" and events[1].payload["recurringEventId"] == "series"
    assert "%2F" in get.calls[1][0]
    assert get.calls[2][1]["pageToken"] == "page2"
    assert state.data["calendar_tokens"] == {"team/a@example.com": "boundary"}
    assert any(isinstance(item, CompletedScope) and item.record_type == "event" for item in results)


async def test_410_after_delta_page_resets_only_that_scope():
    get = ScriptedGet(
        [
            {"items": [{"id": "a"}, {"id": "b"}]},
            {"items": [{"id": "stale"}], "nextPageToken": "delta2"},
            SourceGoneError("expired"),
            {"items": [{"id": "current"}], "nextSyncToken": "a-new"},
            {"items": [], "nextSyncToken": "b-new"},
        ]
    )
    state = cursor({"a": "a-old", "b": "b-old"})
    results = [item async for item in generate_calendar_observations(get, state)]
    starts = [
        item.container_id
        for item in results
        if type(item) is StartedScope and item.record_type == "event"
    ]
    assert starts == ["a"]
    assert get.calls[2][1]["syncToken"] == "a-old"
    assert "syncToken" not in get.calls[3][1] and "pageToken" not in get.calls[3][1]
    assert get.calls[4][1]["syncToken"] == "b-old"
    assert state.data["calendar_tokens"] == {"a": "a-new", "b": "b-new"}


async def test_failure_keeps_original_cursor_and_does_not_complete_scope():
    get = ScriptedGet(
        [
            {"items": [{"id": "a"}]},
            {"items": [{"id": "one"}], "nextPageToken": "next"},
            SourceServerError("temporary", status_code=503),
        ]
    )
    state = cursor()
    observed = []
    with pytest.raises(SourceServerError):
        async for item in generate_calendar_observations(get, state):
            observed.append(item)
    assert state.data["calendar_tokens"] == {}
    assert not any(type(item) is CompletedScope for item in observed)


async def test_missing_calendar_removes_only_previous_calendar_children():
    get = ScriptedGet([{"items": [{"id": "kept"}]}, {"items": [], "nextSyncToken": "new"}])
    state = cursor({"kept": "old", "removed": "old-removed"})
    results = [item async for item in generate_calendar_observations(get, state)]
    removed = [item for item in results if type(item) is RemovedScope]
    assert [(item.record_type, item.container_id, item.removal_reason) for item in removed] == [
        ("event_occurrence", "removed", "scope_removed"),
        ("event", "removed", "scope_removed"),
    ]
    assert state.data["calendar_tokens"] == {"kept": "new"}


def test_only_recurring_cancellations_are_retained_and_reinstatement_keeps_identity():
    from airweave.platform.sources.records.google_calendar import record

    cancelled = {
        "id": "exception",
        "status": "cancelled",
        "recurringEventId": "series",
        "originalStartTime": {"dateTime": "2026-10-01T10:00:00Z"},
    }
    exclusion = record("event", cancelled, "calendar")
    active = record("event", {**cancelled, "status": "confirmed"}, "calendar")
    assert exclusion.kind == active.kind == "upsert"
    assert exclusion.identity == active.identity
    assert exclusion.payload == cancelled
    assert record("event", {"id": "single", "status": "cancelled"}, "calendar").kind == "delete"
    with pytest.raises(ValueError, match="original occurrence"):
        record(
            "event", {"id": "bad", "status": "cancelled", "recurringEventId": "series"}, "calendar"
        )


async def test_expanded_window_is_fixed_paged_and_separate_from_master_scope():
    from airweave.platform.configs.config import CalendarOccurrenceWindow

    window = CalendarOccurrenceWindow(start="2026-03-01T00:00:00Z", end="2026-04-01T00:00:00Z")
    get = ScriptedGet(
        [
            {"items": [{"id": "cal", "timeZone": "America/Los_Angeles"}]},
            {
                "items": [{"id": "master", "recurrence": ["RRULE:FREQ=WEEKLY"]}],
                "nextSyncToken": "master-token",
            },
            {
                "items": [
                    {
                        "id": "real-instance",
                        "recurringEventId": "master",
                        "originalStartTime": {"date": "2026-03-01"},
                    }
                ],
                "nextPageToken": "p2",
            },
            {"items": [{"id": "cancelled-instance", "status": "cancelled"}]},
        ]
    )
    state = cursor()
    observations = [item async for item in generate_calendar_observations(get, state, window)]
    occurrences = [
        item
        for item in observations
        if isinstance(item, CaptureRecord) and item.identity.record_type == "event_occurrence"
    ]
    assert [item.identity.native_id for item in occurrences] == [
        "real-instance",
        "cancelled-instance",
    ]
    assert occurrences[1].kind == "delete"
    assert get.calls[2][1]["timeMin"] == get.calls[3][1]["timeMin"]
    assert "syncToken" not in get.calls[2][1] and get.calls[2][1]["singleEvents"] == "true"
    assert state.data["occurrence_coverage"]["cal"]["timezone"] == "America/Los_Angeles"
    assert any(
        type(item) is CompletedScope and item.record_type == "event_occurrence"
        for item in observations
    )


async def test_failed_expanded_refresh_keeps_prior_coverage_and_does_not_reconcile():
    from airweave.platform.configs.config import CalendarOccurrenceWindow

    window = CalendarOccurrenceWindow(start="2026-03-01T00:00:00Z", end="2026-04-01T00:00:00Z")
    state = cursor({"cal": "old"})
    before = state.data
    get = ScriptedGet(
        [
            {"items": [{"id": "cal", "timeZone": "UTC"}]},
            {"items": [], "nextSyncToken": "new"},
            {"items": [{"id": "one"}], "nextPageToken": "p2"},
            SourceServerError("temporary", status_code=503),
        ]
    )
    observations = []
    with pytest.raises(SourceServerError):
        async for item in generate_calendar_observations(get, state, window):
            observations.append(item)
    assert state.data == before
    assert not any(
        type(item) is CompletedScope and item.record_type == "event_occurrence"
        for item in observations
    )


def test_explicit_historical_window_and_rolling_defaults_are_bounded():
    from datetime import timedelta

    from airweave.platform.configs.config import GoogleCalendarConfig

    default = GoogleCalendarConfig().resolved_window()
    assert default.end - default.start == timedelta(days=120)
    chosen = GoogleCalendarConfig(
        occurrence_window={"start": "2001-01-01T00:00:00Z", "end": "2001-02-01T00:00:00Z"}
    )
    assert chosen.resolved_window().start.year == 2001
    with pytest.raises(ValueError):
        GoogleCalendarConfig(
            occurrence_window={"start": "2001-01-01T00:00:00Z", "end": "2003-02-01T00:00:00Z"}
        )


async def test_expanded_repeated_pages_never_claim_completion():
    from airweave.platform.configs.config import CalendarOccurrenceWindow
    from airweave.platform.sources.records.google_calendar import capture_occurrences

    window = CalendarOccurrenceWindow(start="2026-01-01T00:00:00Z", end="2026-02-01T00:00:00Z")
    get = ScriptedGet(
        [{"items": [], "nextPageToken": "repeat"}, {"items": [], "nextPageToken": "repeat"}]
    )
    observed = []
    with pytest.raises(ValueError, match="repeated"):
        async for item in capture_occurrences(get, "cal", window):
            observed.append(item)
    assert not any(type(item) is CompletedScope for item in observed)


async def test_selection_withdraws_old_scope_and_readd_bootstraps():
    state = cursor({"old": "old-token", "keep": "keep-token"})
    get = ScriptedGet(
        [{"items": [{"id": "old"}, {"id": "keep"}]}, {"items": [], "nextSyncToken": "next"}]
    )
    result = [
        item async for item in generate_calendar_observations(get, state, calendar_ids=("keep",))
    ]
    assert len(get.calls) == 2 and "/keep/events" in get.calls[1][0]
    assert get.calls[1][1]["syncToken"] == "keep-token"
    assert state.data["calendar_tokens"] == {"keep": "next"}
    assert {item.record_type for item in result if isinstance(item, RemovedScope)} == {
        "event",
        "event_occurrence",
    }
    assert all(
        item.removal_reason == "scope_removed" for item in result if isinstance(item, RemovedScope)
    )
    readd = ScriptedGet([{"items": [{"id": "old"}]}, {"items": [], "nextSyncToken": "fresh"}])
    _ = [item async for item in generate_calendar_observations(readd, state, calendar_ids=("old",))]
    assert "syncToken" not in readd.calls[1][1]


async def test_empty_selection_makes_no_provider_requests_and_completes_membership():
    state = cursor({"old": "token"})
    get = ScriptedGet([])
    result = [item async for item in generate_calendar_observations(get, state, calendar_ids=())]
    assert get.calls == [] and state.data["calendar_tokens"] == {}
    assert any(type(item) is CompletedScope and item.record_type == "calendar" for item in result)


async def test_missing_requested_calendar_revokes_known_scope_without_completion():
    state = cursor({"missing": "token"})
    result = []
    with pytest.raises(ValueError, match="not accessible"):
        async for item in generate_calendar_observations(
            ScriptedGet([{"items": []}]), state, calendar_ids=("missing",)
        ):
            result.append(item)
    assert state.data["calendar_tokens"] == {"missing": "token"}
    assert any(
        isinstance(item, RemovedScope) and item.removal_reason == "access_revoked"
        for item in result
    )
    assert not any(type(item) is CompletedScope for item in result)


def test_calendar_selection_config_distinguishes_none_empty_and_invalid():
    from airweave.platform.configs.config import GoogleCalendarConfig

    assert GoogleCalendarConfig().calendar_ids is None
    assert GoogleCalendarConfig(calendar_ids=[]).calendar_ids == ()
    for value in (["primary"], ["a", "a"], [""], [" a"]):
        with pytest.raises(ValueError):
            GoogleCalendarConfig(calendar_ids=value)


async def test_selected_calendar_loses_access_between_membership_and_events():
    from airweave.domains.sources.exceptions import SourceEntityNotFoundError

    state = cursor({"selected": "old"})
    get = ScriptedGet([{"items": [{"id": "selected"}]}, SourceEntityNotFoundError("gone")])
    result = [
        item
        async for item in generate_calendar_observations(get, state, calendar_ids=("selected",))
    ]
    revoked = [item for item in result if isinstance(item, RemovedScope)]
    assert len(revoked) == 2 and all(item.removal_reason == "access_revoked" for item in revoked)
    assert state.data["calendar_tokens"] == {}


async def test_occurrence_access_loss_discards_master_token_before_restoration():
    from airweave.domains.sources.exceptions import SourceEntityNotFoundError
    from airweave.platform.configs.config import CalendarOccurrenceWindow

    window = CalendarOccurrenceWindow(start="2026-03-01T00:00:00Z", end="2026-04-01T00:00:00Z")
    state = cursor({"cal": "old"})
    get = ScriptedGet(
        [
            {"items": [{"id": "cal", "timeZone": "UTC"}]},
            {"items": [{"id": "unchanged"}], "nextSyncToken": "must-discard"},
            SourceEntityNotFoundError("synthetic access loss"),
        ]
    )
    lost = [item async for item in generate_calendar_observations(get, state, window)]
    assert state.data["calendar_tokens"] == {}
    assert state.data["occurrence_coverage"] == {}
    assert {item.record_type for item in lost if isinstance(item, RemovedScope)} == {
        "event",
        "event_occurrence",
    }
    restored_get = ScriptedGet(
        [
            {"items": [{"id": "cal", "timeZone": "UTC"}]},
            {"items": [{"id": "unchanged"}], "nextSyncToken": "restored"},
            {"items": []},
        ]
    )
    restored = [item async for item in generate_calendar_observations(restored_get, state, window)]
    assert "syncToken" not in restored_get.calls[1][1]
    assert any(isinstance(item, StartedScope) and item.record_type == "event" for item in restored)
    assert state.data["calendar_tokens"] == {"cal": "restored"}
    assert "cal" in state.data["occurrence_coverage"]
