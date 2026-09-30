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
