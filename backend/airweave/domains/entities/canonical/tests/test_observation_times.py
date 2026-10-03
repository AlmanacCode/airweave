"""Real SQL timestamp ownership without changing capture/index identity."""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text

from airweave.domains.entities.canonical.tests.conftest import migrate
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange


async def test_observation_revision_and_database_time_survive_reappearance(database, source):
    service, fence = source
    first_time = datetime(2001, 1, 1, tzinfo=timezone.utc)
    initial = observation(observed_at=first_time)
    first = (await capture(database, service, fence, initial)).changes[0].record
    assert first.first_observed_at == first.revision_observed_at == first_time
    assert first.first_stored_at > first_time
    same = await capture(
        database,
        service,
        fence,
        initial.model_copy(
            update={
                "observed_at": first_time + timedelta(days=1),
            }
        ),
    )
    assert same.unchanged == 1 and not same.changes
    async with database() as db:
        row = await db.get(Entity, first.id)
        assert row.observed_at == first_time + timedelta(days=1)
        assert row.revision_observed_at == first_time
        journal = (await db.scalars(select(EntityChange))).one()
        assert journal.created_at.replace(tzinfo=timezone.utc) == first.first_stored_at
        assert journal.snapshot["first_stored_at"] == first.first_stored_at.isoformat().replace(
            "+00:00", "Z"
        )
    for day, kind in ((2, "upsert"), (3, "delete"), (4, "upsert")):
        changed = initial.model_copy(
            update={
                "kind": kind,
                "removal_reason": "provider_deleted" if kind == "delete" else None,
                "payload": {"changed": day},
                "observed_at": first_time + timedelta(days=day),
            }
        )
        result = (await capture(database, service, fence, changed)).changes[0].record
        assert result.first_observed_at == first_time
        assert result.first_stored_at == first.first_stored_at
        assert result.revision_observed_at == changed.observed_at
        assert result.revision == day


@pytest.mark.parametrize("database", ["legacy"], indirect=True)
async def test_backfill_requires_exact_journal_and_keeps_unknown_history_null(database, source):
    service, fence = source
    first_time = datetime(2001, 1, 1, tzinfo=timezone.utc)
    first = (await capture(database, service, fence, observation(observed_at=first_time))).changes[
        0
    ]
    await capture(
        database,
        service,
        fence,
        observation(
            payload={"changed": True},
            observed_at=first_time + timedelta(days=2),
        ),
    )
    missing = (await capture(database, service, fence, observation("missing"))).changes[0]
    latest = (
        (
            await capture(
                database,
                service,
                fence,
                observation(
                    "missing",
                    payload={"changed": True},
                    observed_at=first_time,
                ),
            )
        )
        .changes[0]
        .record
    )
    absent = (await capture(database, service, fence, observation("no-journal"))).changes[0]
    async with database() as db:
        # Recreate pre-0019 historical evidence: Python created_at is deliberately
        # not considered a DB-time attestation. Leave immutable observed_at intact.
        await db.execute(
            text("""
            UPDATE entity_change SET snapshot = snapshot - 'first_stored_at'
                - 'first_observed_at' - 'revision_observed_at'
        """)
        )
        await db.execute(
            text("DELETE FROM entity_change WHERE entity_record_id=:id AND record_revision=1"),
            {"id": missing.record.id},
        )
        await db.execute(
            text("DELETE FROM entity_change WHERE entity_record_id=:id"), {"id": absent.record.id}
        )
        before = (
            await db.execute(
                text(
                    "SELECT id,record_revision,capture_hash,indexed_revision "
                    "FROM entity ORDER BY id"
                )
            )
        ).all()
        for field in ("first_stored_at", "first_observed_at", "revision_observed_at"):
            await db.execute(text(f"ALTER TABLE entity DROP COLUMN {field}"))
        await (await db.connection()).run_sync(migrate, "0019_record_observation_times.py")
        after = (
            await db.execute(
                text(
                    "SELECT id,record_revision,capture_hash,indexed_revision "
                    "FROM entity ORDER BY id"
                )
            )
        ).all()
        assert before == after
        known = await db.get(Entity, first.record.id)
        assert known.first_observed_at == first_time
        assert known.revision_observed_at == first_time + timedelta(days=2)
        assert known.first_stored_at is None
        unknown = await db.get(Entity, missing.record.id)
        assert unknown.first_observed_at is None
        assert unknown.revision_observed_at == latest.observed_at
        assert unknown.first_stored_at is None

        absent_row = await db.get(Entity, absent.record.id)
        assert absent_row.first_observed_at is absent_row.revision_observed_at is None
        assert absent_row.first_stored_at is None

        legacy = await db.scalar(select(Entity).where(Entity.record_revision == 0))
        assert legacy.created_at is not None
        assert legacy.first_observed_at is legacy.revision_observed_at is None
        assert legacy.first_stored_at is None
