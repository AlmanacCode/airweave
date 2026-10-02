"""Wispr resumes completed bodies through the real canonical forest, without absence claims."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord, RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.sources.exceptions import SourceServerError
from airweave.domains.sources.token_providers.protocol import ManagedToolAuthProvider
from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.platform.sources.wispr import WisprSource


async def connector(rows, body_calls, *, fail_at=None, failure=None):
    result = await WisprSource.create(
        auth=ManagedToolAuthProvider(
            api_key="fixture", connected_account_id="fixture", user_id="fixture"
        ),
        logger=MagicMock(),
        http_client=MagicMock(),
    )

    async def execute(slug, arguments):
        if slug == "WISPR_FLOW_MCP_SEARCH_SCRATCHPAD_NOTES":
            return {"notes": [], "has_more": False}
        if slug == "WISPR_FLOW_MCP_SEARCH_MEETINGS":
            return {"meetings": rows, "has_more": False}
        native = arguments["meeting_id"]
        body_calls.append(native)
        if len(body_calls) == fail_at:
            raise failure or ValueError("Synthetic opaque body failure")
        return {
            "id": native,
            "content": "notes",
            "transcript": "transcript",
            "modified_at": "2026-09-30T00:00:00Z",
            "start": "2026-09-20T00:00:00Z",
        }

    result._execute = execute
    return result


def driver(service, database, fence, source):
    return CanonicalScanDriver(
        service, database, fence, source, AsyncMock(), AsyncMock(), MagicMock()
    )


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("Synthetic opaque body failure"),
        SourceServerError("Synthetic rate signal; retry delay unknown", source_short_name="wispr"),
    ],
)
async def test_failed_body_resumes_without_repeating_completed_siblings(database, source, failure):
    service, fence = source
    rows = [
        {"id": f"m{i}", "title": "Earlier listing", "start": "2026-09-20T00:00:00Z"}
        for i in range(10)
    ]
    calls = []
    first = await connector(rows, calls, fail_at=10, failure=failure)
    with pytest.raises(type(failure), match="Synthetic"):
        await driver(service, database, fence, first).run()
    assert len(calls) == 10
    failed = calls[-1]
    async with database() as db:
        scans = list(
            (
                await db.scalars(select(CaptureScan).where(CaptureScan.record_type == "meeting"))
            ).all()
        )
        assert sum(scan.phase == "complete" for scan in scans) == 9
        assert sum(scan.phase == "collecting" for scan in scans) == 1
        before = await service.read_cycle(db, fence)
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    # Listing changes must not masquerade as body freshness.
    changed = [
        {**row, "title": "Updated listing", "modified_at": "2026-10-01T00:00:00Z"}
        for row in rows
        if row["id"] != calls[0]
    ]
    second = await connector(changed, calls)
    after = await driver(service, database, newer, second).run()
    assert before.version.cycle_id == after.version.cycle_id
    assert len(calls) == 11 and calls[-1] == failed
    async with database() as db:
        bodies = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "meeting")
                )
            ).all()
        )
        assert len(bodies) == 10
        assert all(row.deleted_at is None for row in bodies)
        assert all(row.source_created_at is None for row in bodies)
        assert all(
            row.source_updated_at == datetime(2026, 9, 30, tzinfo=timezone.utc) for row in bodies
        )
        # Inventory refresh does not rewrite the body captured on the prior attempt.
        assert (
            sum(row.source_payload["listing"]["title"] == "Earlier listing" for row in bodies) == 9
        )


async def test_old_parentless_body_requires_explicit_migration(database, source):
    service, fence = source
    async with database() as db:
        await service.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    CaptureRecord(
                        identity=RecordIdentity(record_type="meeting", native_id="old"),
                        payload={"prior": True},
                        completeness="partial",
                        observed_at=datetime.now(timezone.utc),
                    ),
                ),
            ),
        )
    current = await connector([{"id": "old", "start": "2026-09-20T00:00:00Z"}], [])
    with pytest.raises(CanonicalStoreError, match="different parent"):
        await driver(service, database, fence, current).run()
    async with database() as db:
        body = await db.scalar(
            select(Entity).where(Entity.entity_definition_short_name == "meeting")
        )
        assert body.source_payload == {"prior": True}
        assert body.parent_record_type is None


async def test_scratchpad_failure_resumes_failed_body_without_replaying_completed_notes(
    database, source
):
    service, fence = source
    listings = [
        {"id": "same-id", "title": "First", "modified_at": "2026-09-30T00:00:00Z"},
        {"id": "n2", "title": "Second", "modified_at": "2026-09-30T00:00:00Z"},
    ]
    calls = []
    failing = True
    completed: set[str] = set()
    failed_native: str | None = None

    async def execute(slug, arguments):
        nonlocal failed_native
        if slug == "WISPR_FLOW_MCP_SEARCH_MEETINGS":
            # Native IDs may collide between resource kinds without aliasing storage.
            return {"meetings": [{"id": "same-id"}], "has_more": False}
        if slug == "WISPR_FLOW_MCP_SEARCH_SCRATCHPAD_NOTES":
            return {"notes": listings, "has_more": False}
        if slug == "WISPR_FLOW_MCP_GET_MEETING":
            return {"id": "same-id", "content": "meeting", "transcript": "verbatim"}
        assert slug == "WISPR_FLOW_MCP_GET_SCRATCHPAD_NOTE"
        native = arguments["note_id"]
        offset = arguments["view_content"]["start_char"]
        calls.append((native, offset))
        if failing and len(completed) == 1 and offset == 3:
            failed_native = native
            raise SourceServerError("Synthetic Wispr rate signal", source_short_name="wispr")
        if offset == 3:
            completed.add(native)
        return {
            "id": native,
            "modified_at": "2026-09-30T00:00:00Z",
            "content": (
                "abc\n(...truncated, 3 chars remaining; continue with view_content.start_char=3...)"
            )
            if offset == 0
            else "def",
        }

    first = await connector([], [])
    first._execute = execute
    with pytest.raises(SourceServerError):
        await driver(service, database, fence, first).run()
    async with database() as db:
        notes = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "scratchpad_note")
                )
            ).all()
        )
        assert len(notes) == 1 and notes[0].native_id in completed
        assert notes[0].record_revision == 1
        before = await service.read_cycle(db, fence)
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    assert failed_native is not None and failed_native not in completed
    failing = False
    calls.clear()
    second = await connector([], [])
    second._execute = execute
    after = await driver(service, database, newer, second).run()
    assert before.version.cycle_id == after.version.cycle_id
    # Durable scope order follows stored UUIDs, not the provider listing order.
    assert calls == [
        (failed_native, 0),
        (failed_native, 3),
    ]  # Bodies commit atomically, ranges do not checkpoint separately.
    async with database() as db:
        rows = list(
            (
                await db.scalars(
                    select(Entity).where(
                        Entity.entity_definition_short_name.in_(["meeting", "scratchpad_note"])
                    )
                )
            ).all()
        )
        assert len(rows) == 3 and all(
            row.deleted_at is None and row.record_revision == 1 for row in rows
        )
        notes = [row for row in rows if row.entity_definition_short_name == "scratchpad_note"]
        assert all(len(row.source_payload["responses"]) == 2 for row in notes)
        assert all(row.completeness == "partial" for row in notes)


async def test_active_meeting_only_cycle_requires_explicit_restart_for_scratchpad(database, source):
    import hashlib
    import json

    from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
    from airweave.domains.entities.canonical.cycle_store import CycleConflict

    service, fence = source
    old_fingerprint = hashlib.sha256(
        json.dumps(
            {
                "version": 2,
                "account": "fixture",
                "inventory": "meeting_listing",
                "body": "meeting",
                "policy": "discovery_only",
                "listing_limit": 200,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    old = CycleConfiguration.from_source(
        fingerprint=old_fingerprint,
        record_types=("meeting_listing", "meeting"),
        container_parents={"meeting": "meeting_listing"},
        completion_policies={"meeting_listing": "discovery_only"},
    )
    async with database() as db:
        before = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=old))
    current = await connector([], [])
    with pytest.raises(CycleConflict, match="configuration changed"):
        await driver(service, database, fence, current).run()
    async with database() as db:
        after = await service.read_cycle(db, fence)
        assert after == before


async def test_body_byte_overflow_preserves_committed_siblings_and_resume(
    database, source, monkeypatch
):
    from airweave.domains.sources.exceptions import SourceError
    from airweave.platform.sources import wispr

    service, fence = source
    rows = [{"id": f"bounded{i}", "start": "2026-09-20T00:00:00Z"} for i in range(2)]
    calls = []
    first = await connector(rows, calls)
    execute = first._execute

    async def oversized_second(slug, arguments):
        result = await execute(slug, arguments)
        if "meeting_id" in arguments and len(calls) == 2:
            result["native_metadata"] = "x" * 2048
        return result

    first._execute = oversized_second
    monkeypatch.setattr(wispr, "MAX_BODY_BYTES", 1024)
    with pytest.raises(SourceError, match="body exceeds byte limit"):
        await driver(service, database, fence, first).run()
    async with database() as db:
        states = list(
            (
                await db.scalars(select(CaptureScan).where(CaptureScan.record_type == "meeting"))
            ).all()
        )
        assert sorted(state.phase for state in states) == ["collecting", "complete"]
        bodies = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "meeting")
                )
            ).all()
        )
        assert len(bodies) == 1 and bodies[0].native_id == calls[0]
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    second = await connector(rows, calls)
    completed = await driver(service, database, newer, second).run()
    from airweave.domains.entities.canonical.cycle_models import CompleteCycle

    async with database() as db:
        completed = await service.complete_cycle(
            db, CompleteCycle(fence=newer, expected=completed.version)
        )
    assert completed.phase == "complete"
    assert len(calls) == 3 and calls[1] == calls[2]
