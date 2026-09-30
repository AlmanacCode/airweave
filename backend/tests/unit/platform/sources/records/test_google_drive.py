"""Drive bootstrap/delta fidelity, scope completeness and checkpoint failure behavior."""

from copy import deepcopy
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope, StartedScope
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.cursors.google_drive import GoogleDriveCursor
from airweave.platform.sources.records.google_drive import generate_drive_observations


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


def state(token=""):
    return SyncCursor(uuid4(), GoogleDriveCursor, {"canonical_page_token": token})


async def test_initial_boundary_precedes_scan_and_replay_updates_metadata():
    get = ScriptedGet(
        [
            {"startPageToken": "before"},
            {
                "files": [{"id": "one", "name": "old", "parents": ["folder-a"]}],
                "nextPageToken": "page2",
            },
            {"files": []},
            {
                "changes": [
                    {"file": {"id": "one", "name": "new", "parents": ["folder-b"], "trashed": True}}
                ],
                "newStartPageToken": "after",
            },
        ]
    )
    cursor = state()
    results = [item async for item in generate_drive_observations(get, cursor)]
    records = [item for item in results if isinstance(item, CaptureRecord)]
    assert get.calls[0][0].endswith("startPageToken")
    assert get.calls[-1][1]["pageToken"] == "before"
    assert [item.payload["name"] for item in records] == ["old", "new"]
    assert records[-1].kind == "upsert" and records[-1].payload["trashed"] is True
    assert all(item.completeness == "metadata_only" for item in records)
    assert cursor.data["canonical_page_token"] == "after"
    assert type(results[-1]) is CompletedScope


async def test_incremental_removal_pagination_and_no_full_scan():
    get = ScriptedGet(
        [
            {"changes": [{"fileId": "gone", "removed": True}], "nextPageToken": "next"},
            {"changes": [], "newStartPageToken": "after"},
        ]
    )
    cursor = state("before")
    results = [item async for item in generate_drive_observations(get, cursor)]
    assert len(results) == 1 and results[0].kind == "delete"
    assert results[0].removal_reason == "scope_removed"
    assert cursor.data["canonical_page_token"] == "after"
    assert all(url.endswith("/changes") for url, _ in get.calls)


async def test_incomplete_enumeration_cannot_mark_scope_complete_or_save_cursor():
    get = ScriptedGet([{"startPageToken": "before"}, {"files": [], "incompleteSearch": True}])
    cursor = state()
    observed = []
    with pytest.raises(ValueError, match="incomplete enumeration"):
        async for item in generate_drive_observations(get, cursor):
            observed.append(item)
    assert cursor.data["canonical_page_token"] == ""
    assert [type(item) for item in observed] == [StartedScope]


async def test_shared_drive_membership_change_restarts_with_new_boundary():
    get = ScriptedGet(
        [
            {"changes": [{"changeType": "drive", "removed": True, "driveId": "lost"}]},
            {"startPageToken": "new-before"},
            {"files": []},
            {"changes": [], "newStartPageToken": "new-after"},
        ]
    )
    cursor = state("old")
    results = [item async for item in generate_drive_observations(get, cursor)]
    assert [type(item) for item in results] == [StartedScope, CompletedScope]
    assert cursor.data["canonical_page_token"] == "new-after"


async def test_failed_change_page_retains_saved_checkpoint():
    get = ScriptedGet(
        [{"changes": [], "nextPageToken": "next"}, ConnectionError("provider unavailable")]
    )
    cursor = state("before")
    with pytest.raises(ConnectionError):
        _ = [item async for item in generate_drive_observations(get, cursor)]
    assert cursor.data["canonical_page_token"] == "before"
