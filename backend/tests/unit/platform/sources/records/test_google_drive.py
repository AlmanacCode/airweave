"""Drive bounded acquisition; lifecycle checkpoint proof lives in canonical SQL tests."""

import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.page_source import InvalidCaptureCheckpoint
from airweave.domains.sources.exceptions import (
    SourceEntityForbiddenError,
    SourceEntityNotFoundError,
)
from airweave.platform.sources.records.google_drive import file_record
from airweave.platform.sources.records.google_drive_pages import DrivePages, _Progress


class ScriptedGet:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    async def __call__(self, url, params):
        self.calls.append((url, params.copy()))
        value = next(self.replies)
        if isinstance(value, Exception):
            raise value
        return value


async def identity(record):
    return record


async def test_pending_native_ids_resume_without_relisting_and_terminal_only_checkpoint():
    get = ScriptedGet(
        [
            {"files": [{"id": "one"}, {"id": "two"}]},
            {"id": "one"},
            {"id": "two"},
            {
                "changes": [{"changeType": "file", "fileId": "one", "removed": True}],
                "newStartPageToken": "after",
            },
            {"id": "one", "trashed": True},
        ]
    )
    first = await DrivePages(get).page(
        _Progress(phase="list", boundary="before").continuation(), identity
    )
    assert first.provider_checkpoint is None and first.continuation.value["pending"] == ["two"]
    second = await DrivePages(get).page(first.continuation, identity)
    assert second.provider_checkpoint is None
    final = await DrivePages(get).page(second.continuation, identity)
    assert final.final and final.provider_checkpoint.value == {"page_token": "after"}
    assert final.records[0].kind == "upsert" and final.records[0].payload["trashed"]
    assert get.calls[3][1]["pageToken"] == "before"
    assert len([url for url, _ in get.calls if url.endswith("/files")]) == 1


@pytest.mark.parametrize(
    "raw,error",
    [
        ({"incompleteSearch": True}, ValueError),
        ({"files": [{"id": str(n)} for n in range(101)]}, ValidationError),
    ],
)
async def test_incomplete_or_oversized_listing_cannot_return_progress(raw, error):
    with pytest.raises(error):
        await DrivePages(ScriptedGet([raw])).page(
            _Progress(phase="list", boundary="before").continuation(), identity
        )


async def test_shared_drive_event_requires_whole_cycle_restart():
    with pytest.raises(InvalidCaptureCheckpoint):
        await DrivePages(ScriptedGet([{"changes": [{"changeType": "drive"}]}])).page(
            _Progress(phase="changes", boundary="before", token="before").continuation(), identity
        )


async def test_exact_unavailability_is_not_a_general_provider_error():
    record = await DrivePages(ScriptedGet([SourceEntityNotFoundError("gone")])).current("one")
    assert record.kind == "delete" and record.removal_reason == "scope_removed"
    with pytest.raises(SourceEntityForbiddenError):
        await DrivePages(ScriptedGet([SourceEntityForbiddenError("forbidden")])).current("one")
    with pytest.raises(ValueError, match="different file"):
        await DrivePages(ScriptedGet([{"id": "other"}])).current("one")


def test_continuation_bound_and_native_dates():
    state = _Progress(
        phase="changes",
        boundary="b" * 8192,
        token="t" * 8192,
        pending=tuple(f"{n:03d}" + ("x" * 253) for n in range(100)),
        recent=tuple("a" * 64 for _ in range(128)),
        loaded=True,
    )
    state.continuation()


def test_native_dates_preserved_without_inventing_folder_edit_times():
    record = file_record(
        {"id": "a", "createdTime": "2026-01-01T00:00:00Z", "modifiedTime": "2026-02-01T00:00:00Z"}
    )
    assert record.source_created_at.month == 1 and record.source_updated_at.month == 2
    assert file_record({"id": "b"}).source_updated_at is None


async def test_actual_drive_page_source_preserves_factory_selected_node_rejection():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from airweave.domains.entities.canonical.page_source import CanonicalPageSource
    from airweave.domains.sources.token_providers.static import StaticTokenProvider
    from airweave.domains.sync_pipeline.factory import SourceBuildResult, SyncFactory
    from airweave.platform.configs.config import GoogleDriveConfig
    from airweave.platform.sources.google_drive import GoogleDriveSource

    source = await GoogleDriveSource.create(
        auth=StaticTokenProvider("synthetic"),
        logger=MagicMock(),
        http_client=MagicMock(),
        config=GoogleDriveConfig(),
    )
    assert isinstance(source, CanonicalPageSource)
    result = SourceBuildResult(
        source=source, cursor=None, files=None, node_selections=[MagicMock()]
    )
    with pytest.raises(ValueError, match="does not support selected nodes"):
        SyncFactory._build_stream(None, SimpleNamespace(source=source), result, MagicMock())
