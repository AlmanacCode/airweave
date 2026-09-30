"""Real SQL frontier and identity proofs for nested source inventories."""

from datetime import datetime, timezone

import pytest

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    parent_container_key,
)
from airweave.domains.entities.canonical.tests.test_cycles import complete, finish, page, scan


def test_source_owned_parent_digest_is_bounded_and_distinguishes_full_identity():
    left = RecordIdentity(record_type="block", container_id="left", native_id="same")
    right = RecordIdentity(record_type="block", container_id="right", native_id="same")
    assert parent_container_key(left) != parent_container_key(right)
    for depth in range(96):
        key = parent_container_key(left)
        assert len(key) == 71
        left = RecordIdentity(record_type="block", container_id=key, native_id=str(depth))
    assert parent_container_key(left) == parent_container_key(
        RecordIdentity.model_validate_json(left.model_dump_json())
    )


def item(kind, native, parent=None):
    return CaptureRecord(
        identity=RecordIdentity(
            record_type=kind,
            native_id=native,
            container_id=parent_container_key(parent) if parent else None,
        ),
        parent=parent,
        payload={"id": native},
        observed_at=datetime.now(timezone.utc),
    )


async def work(database, service, fence, active):
    async with database() as db:
        return await service.next_scope_work(db, fence, active.version.cycle_id)


async def collect(database, service, fence, active, scope, *records):
    state = await scan(database, service, fence, active, scope)
    state = await page(database, service, fence, state, *records, final=True)
    return await finish(database, service, fence, state)


async def test_nested_frontier_discovers_descendants_and_requires_every_scope(database, source):
    service, fence = source
    config = CycleConfiguration.from_source(
        fingerprint="a" * 64,
        record_types=("repository", "issue", "comment", "file"),
        container_parents={"issue": "repository", "comment": "issue", "file": "repository"},
    )
    async with database() as db:
        active = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    assert (await work(database, service, fence, active)).record_type == "repository"
    repo = item("repository", "repo")
    await collect(database, service, fence, active, CompletedScope(record_type="repository"), repo)
    issue_work = await work(database, service, fence, active)
    assert issue_work.record_type == "issue" and issue_work.parent.identity == repo.identity
    issue = item("issue", "1", repo.identity)
    issue_scope = CompletedScope(
        record_type="issue", container_id=issue.identity.container_id, parent=repo.identity
    )
    await collect(database, service, fence, active, issue_scope, issue)
    next_item = await work(database, service, fence, active)
    assert next_item.record_type == "comment" and next_item.parent.identity == issue.identity
    comment = item("comment", "1", issue.identity)
    await collect(
        database,
        service,
        fence,
        active,
        CompletedScope(
            record_type="comment", container_id=comment.identity.container_id, parent=issue.identity
        ),
        comment,
    )
    assert (await work(database, service, fence, active)).record_type == "file"
    await collect(
        database,
        service,
        fence,
        active,
        CompletedScope(
            record_type="file",
            container_id=parent_container_key(repo.identity),
            parent=repo.identity,
        ),
    )
    assert await work(database, service, fence, active) is None
    assert (await complete(database, service, fence)).phase == "complete"


def test_single_source_topology_accepts_same_kind_and_upgrades_old_flat_snapshot():
    config = CycleConfiguration.from_source(
        fingerprint="a" * 64,
        record_types=("page", "block"),
        container_parents={"block": ("page", "block")},
    )
    assert config.children_of("block") == ("block",)
    assert config.root_record_types == ("page",)
    old = CycleConfiguration(
        fingerprint="a" * 64, root_record_type="page", child_record_types=("block",)
    )
    assert old.parents == {"page": (None,), "block": ("page",)}
    with pytest.raises(ValueError, match="reachable"):
        CycleConfiguration(fingerprint="a" * 64, parents={"block": ("block",)})


@pytest.mark.parametrize("database", ["scan_upgrade"], indirect=True)
async def test_upgrade_preserves_old_acknowledged_scan_and_cycle_restart_cas(database):
    from uuid import uuid4

    from sqlalchemy import select

    from airweave.domains.entities.canonical.cycle_models import RestartCycle
    from airweave.domains.entities.canonical.cycle_store import CycleConflict
    from airweave.domains.entities.canonical.service import CanonicalCaptureService
    from airweave.domains.entities.canonical.store import CanonicalRecordStore
    from airweave.models import SyncJob
    from airweave.models.capture_scan import CaptureScan

    service = CanonicalCaptureService(CanonicalRecordStore())
    async with database() as db:
        job = await db.scalar(select(SyncJob))
        job.status = "running"
        await db.commit()
        fence = await service.activate_writer(
            db, job.organization_id, job.sync_id, job.id, attempt_id=uuid4(), attempt_number=1
        )
    async with database() as db:
        cycle = await service.read_cycle(db, fence)
    assert cycle.version.revision == 7 and cycle.configuration.parents == {
        "block": (None,),
        "leaf": ("block",),
    }
    scope = CompletedScope(record_type="leaf", container_id="active-root")
    async with database() as db:
        state = await service.read_scan(db, fence, scope)
    assert state.version.revision == 11
    assert state.continuation.value == {"cursor": "synthetic-acknowledged-page"}
    assert state.scope.parent == RecordIdentity(record_type="block", native_id="active-root")
    async with database() as db:
        restarted = await service.restart_cycle(
            db, RestartCycle(fence=fence, expected=cycle.version, configuration=cycle.configuration)
        )
    assert restarted.version.cycle_id != cycle.version.cycle_id
    async with database() as db:
        old = await db.scalar(select(CaptureScan))
        assert old.sweep_id == state.version.sweep_id and old.revision == 11
        assert old.continuation == state.continuation.value
    async with database() as db:
        with pytest.raises(CycleConflict):
            await service.restart_cycle(
                db,
                RestartCycle(
                    fence=fence, expected=cycle.version, configuration=cycle.configuration
                ),
            )


async def test_driver_finds_new_same_kind_descendants_behind_uuid_frontier(
    database, source, monkeypatch
):
    from unittest.mock import AsyncMock, MagicMock
    from uuid import UUID, uuid4

    from airweave.domains.entities.canonical.page_source import CapturePage
    from airweave.domains.entities.canonical.scan_models import ScanContinuation
    from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver

    service, fence = source
    ids = iter([UUID(int=100), UUID(int=50), UUID(int=1)])
    monkeypatch.setattr("airweave.domains.entities.canonical.store.uuid4", lambda: next(ids))

    class NestedSource:
        canonical_record_types = ("block",)
        canonical_container_parents = {"block": (None, "block")}
        capture_cycle_configuration = CycleConfiguration.from_source(
            fingerprint="a" * 64,
            record_types=canonical_record_types,
            container_parents=canonical_container_parents,
        )

        def __init__(self):
            self.calls = []
            self.interrupt = True

        def child_scope(self, parent, record_type):
            return CompletedScope(
                record_type=record_type,
                parent=parent.identity,
                container_id=parent_container_key(parent.identity),
            )

        async def capture_page(self, scope, continuation, *, files, parent=None):
            name = parent.identity.native_id if parent else None
            self.calls.append(name)
            if name == "leaf" and self.interrupt:
                self.interrupt = False
                raise RuntimeError("Synthetic interruption before final descendant scan")
            next_name = {None: "root", "root": "child", "child": "leaf"}.get(name)
            records = (item("block", next_name, scope.parent),) if next_name else ()
            return CapturePage(records=records, continuation=ScanContinuation(), final=True)

        async def confirm_absent(self, record):
            raise AssertionError("No omitted records in this synthetic tree")

    connector = NestedSource()
    driver = CanonicalScanDriver(
        service, database, fence, connector, AsyncMock(), AsyncMock(), MagicMock()
    )
    with pytest.raises(RuntimeError, match="Synthetic interruption"):
        await driver.run()
    async with database() as db:
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
    resumed = CanonicalScanDriver(
        service, database, newer, connector, AsyncMock(), AsyncMock(), MagicMock()
    )
    after = await resumed.run()
    assert before.version.cycle_id == after.version.cycle_id
    assert connector.calls == [None, "root", "child", "leaf"] * 2
    assert (await complete(database, service, newer)).phase == "complete"


async def test_inflight_page_rejected_after_exact_parent_epoch_changes(database, source):
    from airweave.domains.entities.canonical.requests import CaptureBatch
    from airweave.domains.entities.canonical.scan_store import ScanConflict

    service, fence = source
    config = CycleConfiguration.from_source(
        fingerprint="a" * 64,
        record_types=("repository", "issue"),
        container_parents={"issue": "repository"},
    )
    async with database() as db:
        active = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    repo = item("repository", "repo")
    await collect(database, service, fence, active, CompletedScope(record_type="repository"), repo)
    issue = item("issue", "one", repo.identity)
    scope = CompletedScope(
        record_type="issue", parent=repo.identity, container_id=issue.identity.container_id
    )
    inflight = await scan(database, service, fence, active, scope)
    from airweave.domains.entities.canonical.cycle_store import CycleConflict

    with pytest.raises(CycleConflict, match="identity mapping changed"):
        await scan(
            database, service, fence, active, scope.model_copy(update={"container_id": "different"})
        )
    async with database() as db:
        await service.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    repo.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
                ),
            ),
        )
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(repo,)))
    with pytest.raises(ScanConflict, match="owner changed"):
        await page(database, service, fence, inflight, issue, final=True)
    async with database() as db:
        saved = await service.read_scan(db, fence, scope)
    assert saved.version == inflight.version and saved.continuation == inflight.continuation
    restarted = await scan(
        database, service, fence, active, scope, expected=saved.version, restart=True
    )
    assert restarted.parent_visibility_epoch != inflight.parent_visibility_epoch
    await finish(
        database, service, fence, await page(database, service, fence, restarted, issue, final=True)
    )
    assert (await complete(database, service, fence)).phase == "complete"


def test_legacy_topology_root_collision_and_oversized_kind_are_rejected():
    with pytest.raises(ValueError, match="Ambiguous"):
        CycleConfiguration(
            fingerprint="a" * 64, root_record_type="block", child_record_types=("block",)
        )
    with pytest.raises(ValueError):
        CycleConfiguration(fingerprint="a" * 64, parents={"a" * 201: (None,)})


@pytest.mark.parametrize(
    "database",
    [
        "scan_upgrade_children_object",
        "scan_upgrade_foreign_version",
        "scan_upgrade_completed",
        "scan_upgrade_too_many_children",
    ],
    indirect=True,
)
async def test_upgrade_never_guesses_ownership_from_invalid_or_inactive_cycle(database):
    from sqlalchemy import select

    from airweave.models.capture_scan import CaptureScan

    async with database() as db:
        row = await db.scalar(select(CaptureScan))
        assert row.parent_record_id is None and row.parent_visibility_epoch is None
        assert row.scope_key == '["leaf","active-root"]'
        assert row.revision == 11 and row.continuation == {"cursor": "synthetic-acknowledged-page"}


@pytest.mark.parametrize("database", ["scan_upgrade_default_children"], indirect=True)
async def test_upgrade_accepts_historical_omitted_child_list_default(database):
    from sqlalchemy import select

    from airweave.models.capture_scan import CaptureScan
    from airweave.models.sync_cursor import SyncCursor

    async with database() as db:
        root = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "block"))
        cursor = await db.scalar(select(SyncCursor))
        assert (
            str(root.membership_attempt_id)
            == cursor.cursor_data["canonical_cycle"]["root_writer_attempt_id"]
        )
        assert root.revision == 9 and root.phase == "complete"
        child = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "leaf"))
        assert child.parent_record_id is None
