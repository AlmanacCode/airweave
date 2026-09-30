"""Calendar native pagination and resource rules; recovery is verified with real SQL separately."""

from copy import deepcopy

import pytest

from airweave.domains.entities.canonical.calendar import CalendarScopeContext
from airweave.domains.entities.canonical.page_source import InvalidScopeCheckpoint, ScopeAccessLost
from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.sources.exceptions import (
    SourceEntityNotFoundError,
    SourceGoneError,
    SourceServerError,
)
from airweave.platform.configs.config import GoogleCalendarConfig
from airweave.platform.sources.records.calendar_pages import CalendarPages, CalendarProgress
from airweave.platform.sources.records.google_calendar import record


class ScriptedGet:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    async def __call__(self, url, params=None):
        self.calls.append((url, deepcopy(params)))
        value = next(self.replies)
        if isinstance(value, Exception):
            raise value
        return value


def progress(kind="event", **updates):
    params = {"maxResults": 250, "showDeleted": "true", "singleEvents": "false"}
    if kind == "event_occurrence":
        params.update(
            singleEvents="true", timeMin="2026-03-01T00:00:00Z", timeMax="2026-04-01T00:00:00Z"
        )
    return CalendarProgress(
        mode="full", context=CalendarScopeContext(parameters=params), **updates
    ).continuation()


async def test_full_pagination_retains_sparse_cancellation_and_container():
    get = ScriptedGet(
        [
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
    pages = CalendarPages(get, GoogleCalendarConfig())
    scope = CompletedScope(record_type="event", container_id="team/a@example.com")
    first = await pages.page(scope, progress())
    assert not first.final and first.provider_checkpoint is None
    second = await pages.page(scope, first.continuation)
    assert second.final and second.provider_checkpoint.value == {"sync_token": "boundary"}
    assert (
        second.records[0].kind == "upsert"
        and second.records[0].identity.container_id == scope.container_id
    )
    assert "%2F" in get.calls[0][0] and get.calls[1][1]["pageToken"] == "page2"


async def test_410_only_delta_is_scope_reset_other_errors_fail():
    scope = CompletedScope(record_type="event", container_id="cal")
    pages = CalendarPages(ScriptedGet([SourceGoneError("expired")]), GoogleCalendarConfig())
    delta = CalendarProgress.model_validate(progress().value).model_copy(
        update={"mode": "changes", "sync_token": "old"}
    )
    with pytest.raises(InvalidScopeCheckpoint):
        await pages.page(scope, delta.continuation())
    for error in (SourceGoneError("full gone"), SourceServerError("temporary", status_code=503)):
        with pytest.raises(type(error)):
            await CalendarPages(ScriptedGet([error]), GoogleCalendarConfig()).page(
                scope, progress()
            )


def test_only_recurring_cancellations_are_retained_and_reinstatement_keeps_identity():
    cancelled = {
        "id": "exception",
        "status": "cancelled",
        "recurringEventId": "series",
        "originalStartTime": {"dateTime": "2026-10-01T10:00:00Z"},
    }
    exclusion = record("event", cancelled, "calendar")
    active = record("event", {**cancelled, "status": "confirmed"}, "calendar")
    assert exclusion.kind == active.kind == "upsert" and exclusion.identity == active.identity
    assert exclusion.payload == cancelled
    assert record("event", {"id": "single", "status": "cancelled"}, "calendar").kind == "delete"
    with pytest.raises(ValueError, match="original occurrence"):
        record(
            "event", {"id": "bad", "status": "cancelled", "recurringEventId": "series"}, "calendar"
        )


async def test_occurrence_window_and_budgets_survive_continuation():
    scope = CompletedScope(record_type="event_occurrence", container_id="cal")
    get = ScriptedGet(
        [
            {"items": [{"id": "a"}], "nextPageToken": "p2"},
            {"items": [{"id": "b", "status": "cancelled"}]},
        ]
    )
    pages = CalendarPages(get, GoogleCalendarConfig())
    first = await pages.page(scope, progress("event_occurrence"))
    second = await pages.page(scope, first.continuation)
    assert (
        second.final and second.provider_checkpoint is None and second.records[0].kind == "delete"
    )
    assert get.calls[0][1]["timeMin"] == get.calls[1][1]["timeMin"]
    assert second.continuation.value["records"] == 2
    with pytest.raises(ValueError, match="budget"):
        await CalendarPages(
            ScriptedGet([{"items": [{"id": "excess"}]}]), GoogleCalendarConfig()
        ).page(scope, progress("event_occurrence", records=10000))


async def test_repeated_pages_and_oversized_native_pages_fail():
    scope = CompletedScope(record_type="event_occurrence", container_id="cal")
    pages = CalendarPages(
        ScriptedGet([{"items": [], "nextPageToken": "same"}] * 2), GoogleCalendarConfig()
    )
    first = await pages.page(scope, progress("event_occurrence"))
    with pytest.raises(ValueError, match="repeated"):
        await pages.page(scope, first.continuation)
    with pytest.raises(ValueError, match="bounded page"):
        await CalendarPages(
            ScriptedGet([{"items": [{"id": str(i)} for i in range(251)]}]), GoogleCalendarConfig()
        ).page(scope, progress("event_occurrence"))


async def test_exact_missing_membership_and_accessible_omission_are_different():
    pages = CalendarPages(ScriptedGet([SourceEntityNotFoundError("gone")]), GoogleCalendarConfig())
    await pages.confirm_member_absent("missing")
    with pytest.raises(ValueError, match="omitted accessible"):
        await CalendarPages(
            ScriptedGet([{"id": "visible"}]), GoogleCalendarConfig()
        ).confirm_member_absent("visible")
    with pytest.raises(ScopeAccessLost):
        await CalendarPages(
            ScriptedGet([SourceEntityNotFoundError("gone")]), GoogleCalendarConfig()
        ).page(CompletedScope(record_type="event", container_id="cal"), progress())


def test_explicit_historical_window_rolling_defaults_and_selection_are_bounded():
    from datetime import timedelta

    assert (
        GoogleCalendarConfig().resolved_window().end
        - GoogleCalendarConfig().resolved_window().start
        == timedelta(days=120)
    )
    assert (
        GoogleCalendarConfig(
            occurrence_window={"start": "2001-01-01T00:00:00Z", "end": "2001-02-01T00:00:00Z"}
        )
        .resolved_window()
        .start.year
        == 2001
    )
    with pytest.raises(ValueError):
        GoogleCalendarConfig(
            occurrence_window={"start": "2001-01-01T00:00:00Z", "end": "2003-02-01T00:00:00Z"}
        )
    assert (
        GoogleCalendarConfig().calendar_ids is None
        and GoogleCalendarConfig(calendar_ids=[]).calendar_ids == ()
    )
    for value in (["primary"], ["a", "a"], [""], [" a"]):
        with pytest.raises(ValueError):
            GoogleCalendarConfig(calendar_ids=value)


def test_record_stamps_native_dates_without_inventing_capture_dates():
    native = {"id": "event", "created": "2026-03-01T00:00:00Z", "updated": "2026-03-02T00:00:00Z"}
    captured = record("event", native, "cal")
    assert captured.source_created_at.day == 1 and captured.source_updated_at.day == 2
    assert captured.payload == native
    assert record("calendar", {"id": "cal"}).source_created_at is None
