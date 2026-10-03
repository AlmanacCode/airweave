"""Fresh discovery sightings permit bounded body work, never completion or deletion."""

from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.cycle_store import CycleConflict, next_scope_work
from airweave.domains.entities.canonical.requests import CompletedScope, parent_container_key
from airweave.domains.entities.canonical.scan_models import BeginScan
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.entities.canonical.tests.test_cycles import complete, finish, page, scan
from airweave.domains.entities.canonical.tests.test_forest_scans import item
from airweave.domains.entities.canonical.tests.test_wispr_recovery import connector, driver
from airweave.models.entity import Entity

ROOT = CompletedScope(record_type="meeting_listing")
CONFIG = CycleConfiguration.from_source(
    fingerprint="a" * 64,
    record_types=("meeting_listing", "meeting", "other_listing"),
    container_parents={"meeting": "meeting_listing"},
    completion_policies={"meeting_listing": "discovery_only", "other_listing": "discovery_only"},
)


async def setup(database, source):
    service, fence = source
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=CONFIG))
    parent = item("meeting_listing", "recent").model_copy(
        update={"descendant_visibility_fields": ("access",)}
    )
    root = await scan(database, service, fence, cycle, ROOT)
    root = await page(database, service, fence, root, parent)
    async with database() as db:
        work = await next_scope_work(db, fence, cycle.version.cycle_id, within=ROOT)
    assert work is not None and work.parent.identity == parent.identity
    request = BeginScan(
        fence=fence,
        scope=CompletedScope(
            record_type="meeting",
            container_id=parent_container_key(parent.identity),
            parent=parent.identity,
        ),
        cycle_id=cycle.version.cycle_id,
        fingerprint=CONFIG.fingerprint,
        expected_parent_epoch=work.parent_visibility_epoch,
        expected_parent_revision=work.parent.revision,
    )
    async with database() as db:
        child = await service.begin_scan(db, request)
    return cycle, parent, root, request, child


async def test_discovered_root_body_precedes_inventory_without_blessing_completion(
    database, source
):
    service, fence = source
    cycle, parent, root, _, child = await setup(database, source)
    body = item("meeting", "recent", parent.identity)
    child = await page(database, service, fence, child, body, final=True)
    await finish(database, service, fence, child)
    with pytest.raises(CycleConflict, match="Root enumeration"):
        await complete(database, service, fence)
    async with database() as db:
        current = await service.read_scan(db, fence, ROOT)
        assert current.phase == "collecting" and current.version == root.version
        assert await next_scope_work(db, fence, cycle.version.cycle_id, within=ROOT) is None
    # An unrelated capture is not a sighting in this root's current sweep.
    unseen = item("meeting_listing", "unseen")
    await capture(database, service, fence, unseen)
    with pytest.raises(CycleConflict, match="ancestor membership"):
        await scan(
            database,
            service,
            fence,
            cycle,
            CompletedScope(
                record_type="meeting",
                parent=unseen.identity,
                container_id=parent_container_key(unseen.identity),
            ),
        )


@pytest.mark.parametrize("new_writer", [False, True])
async def test_restart_requires_fresh_sighting_then_resumes_same_child(
    database, source, new_writer
):
    service, fence = source
    cycle, parent, root, request, child = await setup(database, source)
    if new_writer:
        async with database() as db:
            fence = await service.activate_writer(
                db,
                fence.organization_id,
                fence.sync_id,
                fence.job_id,
                attempt_id=uuid4(),
                attempt_number=2,
            )
        with pytest.raises(CycleConflict, match="ancestor membership"):
            await page(database, service, fence, child, final=True)
    root = await scan(database, service, fence, cycle, ROOT, expected=root.version, restart=True)
    with pytest.raises(CycleConflict, match="ancestor membership"):
        await page(database, service, fence, child, final=True)
    async with database() as db:
        assert await next_scope_work(db, fence, cycle.version.cycle_id, within=ROOT) is None
    await page(database, service, fence, root, parent)
    async with database() as db:
        work = await next_scope_work(db, fence, cycle.version.cycle_id, within=ROOT)
        assert work.parent.revision == 1
        resumed = await service.begin_scan(
            db, request.model_copy(update={"fence": fence, "expected": child.version})
        )
        assert resumed.version == child.version
    body = item("meeting", "recent", parent.identity)
    done = await page(database, service, fence, resumed, body, final=True)
    await finish(database, service, fence, done)
    async with database() as db:
        assert await next_scope_work(db, fence, cycle.version.cycle_id, within=ROOT) is None
        rows = list((await db.scalars(select(Entity))).all())
        assert len(rows) == 2 and all(row.record_revision == 1 for row in rows)


@pytest.mark.parametrize("withdrawn", [False, True])
async def test_parent_visibility_change_rejects_already_admitted_child(database, source, withdrawn):
    service, fence = source
    _, parent, root, _, child = await setup(database, source)
    changed = parent.model_copy(
        update=(
            {"kind": "delete", "removal_reason": "access_revoked"}
            if withdrawn
            else {"payload": {"id": "recent", "access": "changed"}}
        )
    )
    await page(database, service, fence, root, changed)
    with pytest.raises((CycleConflict, ScanConflict), match="visible parent|owner changed"):
        await page(database, service, fence, child, item("meeting", "recent", parent.identity))
    async with database() as db:
        assert (
            await db.scalar(
                select(Entity.id).where(Entity.entity_definition_short_name == "meeting")
            )
            is None
        )


async def test_wispr_driver_commits_body_before_older_and_unrelated_listing(database, source):
    service, fence = source
    native = await connector([], [])
    calls = []
    original_execute = native._execute

    async def execute(slug, arguments):
        if slug == "WISPR_FLOW_MCP_SEARCH_MEETINGS":
            older = bool(arguments.get("cursor"))
            calls.append("older_listing" if older else "first_listing")
            if older:
                async with database() as db:
                    body = await db.scalar(
                        select(Entity).where(Entity.entity_definition_short_name == "meeting")
                    )
                    assert body is not None and body.native_id == "recent"
                    cycle = await service.read_cycle(db, fence)
                    assert cycle.phase == "active"
            return {
                "meetings": [{"id": "older" if older else "recent"}],
                "has_more": not older,
                "next_cursor": None if older else "next",
            }
        if slug == "WISPR_FLOW_MCP_SEARCH_SCRATCHPAD_NOTES":
            calls.append("unrelated_listing")
        else:
            calls.append("body:" + arguments["meeting_id"])
        return await original_execute(slug, arguments)

    native._execute = execute
    await driver(service, database, fence, native).run()
    assert calls.index("body:recent") < calls.index("older_listing")
    assert calls.index("body:recent") < calls.index("unrelated_listing")
    assert calls.count("body:recent") == calls.count("body:older") == 1
    async with database() as db:
        rows = list((await db.scalars(select(Entity))).all())
        assert len(rows) == 4 and all(row.record_revision == 1 for row in rows)
