"""New cycles refresh old content; incomplete omission cannot erase retained state."""

import hashlib
import json
from collections import Counter
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CompleteCycle,
)
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.tests.test_slack_recovery import (
    HISTORY,
    MESSAGE,
    REPLIES,
    ROOT,
    run,
    runner,
    saved,
)
from airweave.domains.entities.canonical.tests.test_wispr_recovery import connector, driver
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange
from airweave.models.sync_job import SyncJob


async def next_job(database, source):
    """A new existing-job lifecycle, not a retry of a completed capture."""
    service, fence = source
    job_id = uuid4()
    async with database() as db:
        old = await db.get(SyncJob, fence.job_id)
        old.status = "completed"
        db.add(
            SyncJob(
                id=job_id,
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                status="running",
            )
        )
        await db.commit()
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            job_id,
            attempt_id=uuid4(),
            attempt_number=1,
        )
    return service, newer


async def wispr_cycle(database, source, connector):
    service, fence = source
    cycle = await driver(service, database, fence, connector).run()
    async with database() as db:
        return await service.complete_cycle(db, CompleteCycle(fence=fence, expected=cycle.version))


async def revisions(database, record_id):
    async with database() as db:
        return list(
            (
                await db.scalars(
                    select(EntityChange)
                    .where(EntityChange.entity_record_id == record_id)
                    .order_by(EntityChange.sequence)
                )
            ).all()
        )


async def test_wispr_new_cycle_refreshes_known_body_without_parent_change_or_false_delete(
    database, source
):
    listing = [
        {"id": native_id, "title": "Same listing", "start": "2026-09-20T00:00:00Z"}
        for native_id in ("known", "omitted")
    ]
    calls = []
    first = await connector(listing, calls)
    completed = await wispr_cycle(database, source, first)
    assert completed.phase == "complete"
    newer_source = await next_job(database, source)
    # Listing metadata is identical. Only a subsequently finalized/edited body changes.
    second = await connector(listing[:1], calls)
    transport = second._execute

    async def execute(slug, arguments):
        value = await transport(slug, arguments)
        if slug == "WISPR_FLOW_MCP_GET_MEETING" and arguments["meeting_id"] == "known":
            return {
                **value,
                "transcript": "Later transcript",
                "modified_at": "2026-10-01T00:00:00Z",
            }
        return value

    second._execute = execute
    refreshed = await wispr_cycle(database, newer_source, second)
    assert (
        refreshed.phase == "complete" and refreshed.version.cycle_id != completed.version.cycle_id
    )
    assert refreshed.promoted_checkpoint is None  # Reconciliation, no native delta promise.
    assert Counter(calls) == {"known": 2, "omitted": 2}
    async with database() as db:
        rows = list((await db.scalars(select(Entity))).all())
        known = next(
            row
            for row in rows
            if row.native_id == "known" and row.entity_definition_short_name == "meeting"
        )
        omitted = next(
            row
            for row in rows
            if row.native_id == "omitted" and row.entity_definition_short_name == "meeting"
        )
        parent = next(
            row
            for row in rows
            if row.native_id == "known" and row.entity_definition_short_name == "meeting_listing"
        )
        assert parent.record_revision == 1
        assert known.record_revision == 2 and known.deleted_at is None
        assert omitted.record_revision == 1 and omitted.deleted_at is None
        assert omitted.removal_reason is None
        coverage = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
    assert coverage.phase == "complete" and coverage.discovery == "incomplete"
    assert coverage.policies["meeting_listing"] == "discovery_only"
    changes = await revisions(database, known.id)
    assert [item.record_revision for item in changes] == [1, 2]
    assert all(item.kind == "upsert" for item in changes)
    assert [
        item.snapshot["payload"]["responses"][0]["response"]["transcript"] for item in changes
    ] == ["transcript", "Later transcript"]
    assert len(await revisions(database, omitted.id)) == 1


async def test_slack_new_cycle_refreshes_old_thread_and_preserves_unconfirmed_omission(
    database, source
):
    first, _, _ = await runner(database, source, [ROOT, HISTORY, REPLIES])
    await run(first)
    _, prior, _ = await saved(database)
    newer_source = await next_job(database, source)
    edited = {**MESSAGE, "text": "Edited old message", "edited": {"ts": "2"}}
    late_reply = {"ts": "1.2", "thread_ts": "1", "text": "Late reply on old thread"}
    page = {"messages": [edited, late_reply]}
    second, transport, _ = await runner(
        database, newer_source, [ROOT, {"messages": [edited]}, page, page]
    )
    # Missing old reply: even a successful empty exact lookup cannot certify deletion.
    with pytest.raises(ValueError, match="message access remains unconfirmed"):
        await run(second)
    rows, current, _ = await saved(database)
    assert transport._get.await_count == 4
    assert current["canonical_cycle"]["phase"] == "active"
    assert (
        current["canonical_cycle"]["version"]["cycle_id"]
        != prior["canonical_cycle"]["version"]["cycle_id"]
    )
    assert current.get("canonical_checkpoint") == prior.get("canonical_checkpoint")
    messages = {row.native_id: row for row in rows if row.entity_definition_short_name == "message"}
    assert messages["1"].record_revision == 2
    assert messages["1"].source_payload["text"] == "Edited old message"
    assert messages["1.2"].record_revision == 1
    assert messages["1.2"].source_payload["text"] == "Late reply on old thread"
    assert messages["1.1"].record_revision == 1
    assert all(row.deleted_at is None and row.removal_reason is None for row in messages.values())
    changes = await revisions(database, messages["1"].id)
    assert [item.record_revision for item in changes] == [1, 2]
    assert [item.snapshot["payload"]["text"] for item in changes] == [
        "Synthetic parent",
        "Edited old message",
    ]
    assert len(await revisions(database, messages["1.1"].id)) == 1
    async with database() as db:
        coverage = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
    assert coverage.phase == "active" and coverage.discovery == "pending"


async def test_slack_legacy_active_cycle_cannot_resume_without_exact_omission_policy(
    database, source
):
    first, connector, pipeline = await runner(database, source, [])
    strong = connector.capture_cycle_configuration
    assert strong.known_object_validation == ("message",)
    weak = strong.model_copy(
        update={
            "fingerprint": hashlib.sha256(
                json.dumps(
                    {"version": 2, "team_id": "T1", "user_id": "U1"}, sort_keys=True
                ).encode()
            ).hexdigest(),
            "known_object_validation": (),
        }
    )
    assert weak.digest() != strong.digest()
    async with database() as db:
        prior = await source[0].begin_cycle(db, BeginCycle(fence=source[1], configuration=weak))
    with pytest.raises(
        CycleConflict, match="Active cycle configuration changed; explicit abandonment required"
    ):
        await run(first)
    connector._get.assert_not_awaited()
    async with database() as db:
        current = await source[0].read_cycle(db, pipeline._writer())
        assert current == prior
        assert not list((await db.scalars(select(Entity))).all())
