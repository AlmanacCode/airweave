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
        ("event", "removed", "scope_removed")
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
