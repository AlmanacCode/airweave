"""Actual Slack page adapter plus orchestrator/capture transactions; synthetic HTTP only."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor
from airweave.platform.configs.config import SlackConfig
from airweave.platform.sources.slack import SlackApiError, SlackSource

ROOT = {"channels": [{"id": "C1", "name": "Synthetic channel"}]}
MESSAGE = {"ts": "1", "reply_count": 1, "text": "Synthetic parent"}
HISTORY = {"messages": [MESSAGE]}
REPLIES = {"messages": [MESSAGE, {"ts": "1.1", "thread_ts": "1", "text": "Synthetic reply"}]}


async def runner(database, source, responses, attempt=1, service=None, *, capture_files=False):
    original_service, fence = source
    service = service or original_service
    ctx, _, runtime, bus = components(database, source)
    connector = SlackSource(
        auth=StaticTokenProvider("fixture"), logger=MagicMock(), http_client=MagicMock()
    )
    connector.slack_config = SlackConfig(
        expected_team_id="T1", expected_user_id="U1", capture_files=capture_files
    )
    connector._get = AsyncMock(return_value={"ok": True, "team_id": "T1", "user_id": "U1"})
    await connector.validate()
    connector._get = AsyncMock(side_effect=responses)
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
        connector.canonical_container_parents,
        page_source=connector,
        files=MagicMock(),
    )
    runtime.source = connector
    runtime.canonical_capture = pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    return instance, connector, pipeline


async def run(instance, *, checkpoint=True):
    await instance._start_sync()
    await instance._process_entities()
    await instance._cleanup_orphaned_entities_if_needed()
    if checkpoint:
        await instance._save_cursor_data()


async def saved(database):
    async with database() as db:
        rows = (await db.scalars(select(Entity))).all()
        cursor = (await db.scalar(select(SyncCursor))).cursor_data
        scans = (await db.scalars(select(CaptureScan))).all()
        return rows, cursor, scans


@pytest.mark.parametrize("capture_files", [False, True])
async def test_cancel_after_history_resumes_pending_reply_without_repeating_history(
    database, source, capture_files
):
    first, _, _ = await runner(
        database, source, [ROOT, HISTORY, asyncio.CancelledError()], capture_files=capture_files
    )
    with pytest.raises(asyncio.CancelledError):
        await run(first)
    rows, cursor, scans = await saved(database)
    assert {row.native_id for row in rows} == {"C1", "1"}
    assert "canonical_checkpoint" not in cursor
    message_scan = next(scan for scan in scans if scan.record_type == "message")
    assert message_scan.continuation["pending_threads"] == ["1"]
    second, connector, _ = await runner(
        database, source, [ROOT, REPLIES], attempt=2, capture_files=capture_files
    )
    await run(second)
    operations = [call.args[0].split("/")[-1] for call in connector._get.call_args_list]
    assert operations[:2] == ["conversations.list", "conversations.replies"]
    # Both attachment owners were freshly returned by the reply page; its SQL
    # commit attests empty inventories without a redundant exact lookup.
    assert len(operations) == 2
    rows, cursor, _ = await saved(database)
    assert {row.native_id for row in rows} == {"C1", "1", "1.1"}
    assert next(row for row in rows if row.native_id == "1").record_revision == 1
    assert cursor["canonical_cycle"]["phase"] == "complete"


async def test_lost_page_ack_reloads_committed_pending_threads(database, source):
    class LostAck(CanonicalCaptureService):
        async def commit_scan_page(self, db, request):
            result = await super().commit_scan_page(db, request)
            if request.scope.record_type == "message":
                raise ConnectionError("synthetic lost commit acknowledgement")
            return result

    first, connector, _ = await runner(
        database, source, [ROOT, HISTORY], service=LostAck(source[0].store)
    )
    with pytest.raises(ConnectionError, match="acknowledgement"):
        await run(first)
    assert connector._get.await_count == 2
    second, connector, _ = await runner(database, source, [ROOT, REPLIES], attempt=2)
    await run(second)
    assert connector._get.call_args_list[-1].args[0].endswith("conversations.replies")


async def test_expired_reply_cursor_restarts_whole_scope_and_never_partial_reconcile(
    database, source
):
    first_reply = {
        "messages": [{"ts": "1.old", "thread_ts": "1"}],
        "response_metadata": {"next_cursor": "expired"},
    }
    first, _, _ = await runner(
        database, source, [ROOT, HISTORY, first_reply, asyncio.CancelledError()]
    )
    with pytest.raises(asyncio.CancelledError):
        await run(first)
    rows, _, scans = await saved(database)
    assert next(row for row in rows if row.native_id == "1.old").deleted_at is None
    old_sweep = next(scan for scan in scans if scan.record_type == "message").sweep_id
    second, connector, _ = await runner(
        database, source, [ROOT, SlackApiError("invalid_cursor"), HISTORY, REPLIES], attempt=2
    )
    await run(second)
    calls = connector._get.call_args_list
    assert calls[1].args[1]["cursor"] == "expired"
    assert calls[2].args[0].endswith("conversations.history") and "cursor" not in calls[2].args[1]
    rows, _, scans = await saved(database)
    assert next(row for row in rows if row.native_id == "1.old").deleted_at is not None
    assert next(scan for scan in scans if scan.record_type == "message").sweep_id != old_sweep


@pytest.mark.parametrize("error_code", ["invalid_cursor", "thread_not_found"])
async def test_repeated_invalid_cursor_fails_without_checkpoint_or_absence(
    database, source, error_code
):
    first, _, _ = await runner(
        database,
        source,
        [ROOT, HISTORY, SlackApiError(error_code), HISTORY, SlackApiError(error_code)],
    )
    from airweave.domains.entities.canonical.page_source import InvalidScanContinuation

    with pytest.raises(InvalidScanContinuation):
        await run(first)
    rows, cursor, _ = await saved(database)
    assert all(row.deleted_at is None for row in rows)
    assert "canonical_checkpoint" not in cursor


@pytest.mark.parametrize("accessible", [False, True])
async def test_membership_loss_on_retry_hides_pending_children_only_after_confirmation(
    database, source, accessible
):
    first, _, _ = await runner(database, source, [ROOT, HISTORY, asyncio.CancelledError()])
    with pytest.raises(asyncio.CancelledError):
        await run(first)
    responses = [
        {"channels": []},
        {"channel": {"id": "C1"}} if accessible else SlackApiError("channel_not_found"),
    ]
    second, connector, pipeline = await runner(database, source, responses, attempt=2)
    if accessible:
        with pytest.raises(ValueError, match="omitted"):
            await run(second)
    else:
        await run(second)
    assert connector._get.await_count == 2
    rows, cursor, _ = await saved(database)
    child = next(row for row in rows if row.native_id == "1")
    async with database() as db:
        value = await source[0].store.read(
            db, source[1].organization_id, source[1].sync_id, child.id
        )
    assert value.content_access == ("available" if accessible else "unavailable")
    assert cursor["canonical_cycle"]["phase"] == ("active" if accessible else "complete")


async def test_crash_after_scans_then_lost_final_ack_same_job_does_not_start_new_cycle(
    database, source
):
    first, _, _ = await runner(database, source, [ROOT, HISTORY, REPLIES])
    await run(first, checkpoint=False)
    _, cursor, _ = await saved(database)
    cycle_id = cursor["canonical_cycle"]["version"]["cycle_id"]
    second, connector, _ = await runner(database, source, [ROOT], attempt=2)
    await run(second)
    assert connector._get.await_count == 1
    third, connector, _ = await runner(database, source, [], attempt=3)
    await run(third)
    assert connector._get.await_count == 0
    _, cursor, _ = await saved(database)
    assert cursor["canonical_cycle"]["version"]["cycle_id"] == cycle_id


async def test_new_job_starts_fresh_cycle_without_overriding_successful_old_job(database, source):
    from airweave.domains.entities.canonical.store import StaleWriter
    from airweave.models.sync_job import SyncJob

    first, _, _ = await runner(database, source, [ROOT, HISTORY, REPLIES])
    await run(first)
    _, cursor, _ = await saved(database)
    prior_cycle = cursor["canonical_cycle"]["version"]["cycle_id"]
    service, fence = source
    new_job_id = uuid4()
    async with database() as db:
        old_job = await db.get(SyncJob, fence.job_id)
        old_job.status = "completed"
        db.add(
            SyncJob(
                id=new_job_id,
                sync_id=fence.sync_id,
                organization_id=fence.organization_id,
                status="running",
            )
        )
        await db.commit()
    stale, _, _ = await runner(database, source, [], attempt=2)
    with pytest.raises(StaleWriter, match="active job"):
        await run(stale)
    new_source = (service, fence.model_copy(update={"job_id": new_job_id, "attempt_id": uuid4()}))
    new, connector, _ = await runner(database, new_source, [ROOT, HISTORY, REPLIES])
    await run(new)
    assert connector._get.await_count == 3
    _, cursor, _ = await saved(database)
    assert cursor["canonical_cycle"]["version"]["cycle_id"] != prior_cycle
    assert cursor["canonical_cycle"]["completed_job_id"] == str(new_job_id)
    async with database() as db:
        assert (await db.get(SyncJob, fence.job_id)).status == "completed"


async def test_access_lost_during_reply_withdraws_root_and_children(database, source):
    first, _, _ = await runner(database, source, [ROOT, HISTORY, SlackApiError("not_in_channel")])
    await run(first)
    rows, cursor, _ = await saved(database)
    assert cursor["canonical_cycle"]["phase"] == "complete"
    assert all(row.removal_reason == "access_revoked" for row in rows)
    async with database() as db:
        for row in rows:
            value = await source[0].store.read(
                db, source[1].organization_id, source[1].sync_id, row.id
            )
            assert value.content_access == "unavailable" and value.payload == {}


@pytest.mark.parametrize("threaded,count", [(False, 501), (True, 101)])
async def test_oversized_page_or_pending_thread_queue_fails_before_commit(
    database, source, threaded, count
):
    oversized = {
        "messages": [
            {
                "ts": str(i),
                "reply_count": int(threaded),
                "text": "private fixture must not enter validation diagnostics",
            }
            for i in range(count)
        ]
    }
    first, _, _ = await runner(database, source, [ROOT, oversized])
    with pytest.raises(ValueError, match="invalid page") as error:
        await run(first)
    assert "private fixture" not in str(error.value)
    rows, cursor, scans = await saved(database)
    assert {row.native_id for row in rows} == {"C1"}
    child = next(scan for scan in scans if scan.record_type == "message")
    assert child.phase == "collecting" and child.revision == 1 and child.continuation == {}
    assert "canonical_checkpoint" not in cursor


async def test_interrupted_omission_confirmations_remove_nothing_then_retry(database, source):
    from airweave.domains.entities.canonical.requests import RecordIdentity
    from airweave.domains.entities.canonical.tests.helpers import capture, observation

    await capture(
        database,
        source[0],
        source[1],
        *(
            observation(identity=RecordIdentity(record_type="channel", native_id=value))
            for value in ("C1", "C2")
        ),
    )
    first, _, _ = await runner(
        database,
        source,
        [
            {"channels": []},
            SlackApiError("channel_not_found"),
            ConnectionError("confirmation failed"),
        ],
    )
    with pytest.raises(ConnectionError, match="confirmation failed"):
        await run(first)
    rows, cursor, _ = await saved(database)
    assert all(row.deleted_at is None for row in rows)
    assert "canonical_checkpoint" not in cursor
    second, connector, _ = await runner(
        database,
        source,
        [{"channels": []}, SlackApiError("channel_not_found"), SlackApiError("channel_not_found")],
        attempt=2,
    )
    await run(second)
    assert connector._get.await_count == 3
    rows, cursor, _ = await saved(database)
    assert all(row.removal_reason == "scope_removed" for row in rows)
    assert cursor["canonical_cycle"]["phase"] == "complete"


async def test_cleanup_budget_stop_preserves_reconciling_state_and_resumes(database, source):
    from airweave.domains.entities.canonical.requests import RecordIdentity
    from airweave.domains.entities.canonical.tests.helpers import capture, observation

    parent = RecordIdentity(record_type="channel", native_id="C1")
    await capture(
        database,
        source[0],
        source[1],
        observation(identity=parent),
        *(
            observation(
                identity=RecordIdentity(record_type="message", native_id=str(i), container_id="C1"),
                parent=parent,
            )
            for i in range(251)
        ),
    )
    first, _, _ = await runner(database, source, [ROOT, {"messages": []}])
    checks = 0

    async def limit():
        nonlocal checks
        checks += 1
        if checks == 5:
            raise RuntimeError("synthetic cleanup budget reached")

    first._check_capture_limits = limit
    with pytest.raises(RuntimeError, match="cleanup budget"):
        await run(first)
    rows, cursor, scans = await saved(database)
    messages = [row for row in rows if row.entity_definition_short_name == "message"]
    assert sum(row.deleted_at is not None for row in messages) == 250
    assert next(scan for scan in scans if scan.record_type == "message").phase == "reconciling"
    assert "canonical_checkpoint" not in cursor
    second, connector, _ = await runner(database, source, [ROOT], attempt=2)
    await run(second)
    assert connector._get.await_count == 1
    rows, cursor, _ = await saved(database)
    assert all(
        row.deleted_at is not None for row in rows if row.entity_definition_short_name == "message"
    )
    assert cursor["canonical_cycle"]["phase"] == "complete"


@pytest.mark.parametrize("completed_child", [False, True])
async def test_restored_root_restarts_withdrawn_child_scope_in_same_cycle(
    database, source, completed_child
):
    first_responses = [
        ROOT,
        HISTORY,
        REPLIES if completed_child else SlackApiError("not_in_channel"),
    ]
    first, _, _ = await runner(database, source, first_responses)
    await run(first, checkpoint=False)
    restore_attempt = 2
    if completed_child:
        removed, _, _ = await runner(
            database, source, [{"channels": []}, SlackApiError("channel_not_found")], attempt=2
        )
        await run(removed, checkpoint=False)
        restore_attempt = 3
    before, cursor, scans = await saved(database)
    assert all(row.deleted_at is not None for row in before)
    old_sweep = next(scan for scan in scans if scan.record_type == "message").sweep_id
    restored, connector, _ = await runner(
        database, source, [ROOT, HISTORY, REPLIES], attempt=restore_attempt
    )
    await run(restored)
    operations = [call.args[0].split("/")[-1] for call in connector._get.call_args_list]
    assert operations == ["conversations.list", "conversations.history", "conversations.replies"]
    after, _, scans = await saved(database)
    assert {row.native_id for row in after} == {"C1", "1", "1.1"}
    assert all(row.deleted_at is None for row in after)
    assert next(scan for scan in scans if scan.record_type == "message").sweep_id != old_sweep


async def test_root_revival_and_child_invalidation_roll_back_together(database, source):
    from airweave.domains.entities.canonical.scan_store import CanonicalScanStore

    first, _, _ = await runner(database, source, [ROOT, HISTORY, REPLIES])
    await run(first, checkpoint=False)
    removed, _, _ = await runner(
        database, source, [{"channels": []}, SlackApiError("channel_not_found")], attempt=2
    )
    await run(removed, checkpoint=False)
    before, _, scans = await saved(database)
    root_before = next(row for row in before if row.native_id == "C1")
    child_before = next(scan for scan in scans if scan.record_type == "message")

    class FailAfterPage(CanonicalScanStore):
        async def page(self, db, request):
            await super().page(db, request)
            raise RuntimeError("synthetic failure after revival and invalidation")

    service = CanonicalCaptureService(source[0].store)
    service.scans = FailAfterPage(service.store)
    restoring, _, _ = await runner(database, source, [ROOT], attempt=3, service=service)
    with pytest.raises(RuntimeError, match="after revival"):
        await run(restoring)
    after, _, scans = await saved(database)
    root_after = next(row for row in after if row.native_id == "C1")
    child_after = next(scan for scan in scans if scan.record_type == "message")
    assert root_after.deleted_at == root_before.deleted_at
    assert root_after.record_revision == root_before.record_revision
    assert child_after.sweep_id == child_before.sweep_id
    assert child_after.revision == child_before.revision and child_after.phase == "complete"


async def test_unavailable_parent_without_tombstone_also_invalidates_child_progress(
    database, source
):
    first, _, _ = await runner(database, source, [ROOT, HISTORY, SlackApiError("not_in_channel")])
    await run(first, checkpoint=False)
    async with database() as db:
        root = await db.scalar(select(Entity).where(Entity.native_id == "C1"))
        root.deleted_at = None  # Exercise the independent content-access gate representation.
        await db.commit()
    restored, connector, _ = await runner(database, source, [ROOT, HISTORY, REPLIES], attempt=2)
    await run(restored)
    assert connector._get.call_args_list[1].args[0].endswith("conversations.history")
    rows, _, _ = await saved(database)
    assert all(row.deleted_at is None and row.removal_reason is None for row in rows)


async def test_scope_withdrawal_keeps_uncertain_reason_without_claiming_access_revocation(
    database, source
):
    from airweave.domains.entities.canonical.page_source import ScopeAccessLost

    instance, _, pipeline = await runner(database, source, [ROOT, HISTORY])
    original_page = pipeline.page_source.capture_page

    async def unavailable(scope, continuation, *, files, parent=None):
        if scope.record_type == "message" and continuation.value.get("pending_threads"):
            raise ScopeAccessLost(
                "Selected scope is no longer available", removal_reason="scope_removed"
            )
        return await original_page(scope, continuation, files=files)

    pipeline.page_source.capture_page = unavailable
    await run(instance)
    rows, _, _ = await saved(database)
    assert all(row.removal_reason == "scope_removed" for row in rows)
    async with database() as db:
        for row in rows:
            value = await source[0].store.read(
                db, source[1].organization_id, source[1].sync_id, row.id
            )
            assert value.content_access == "unavailable" and value.payload == {}


@pytest.mark.parametrize("change", ["rename", "revive"])
async def test_old_provider_route_failure_cannot_withdraw_changed_same_fence_parent(
    database, source, change
):
    from datetime import datetime, timezone

    from airweave.domains.entities.canonical.page_source import ScopeAccessLost
    from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord
    from airweave.domains.entities.canonical.scan_store import ScanConflict

    service, fence = source
    instance, connector, pipeline = await runner(database, source, [ROOT])
    original_page = connector.capture_page

    async def change_during_network(scope, continuation, *, files, parent=None):
        if parent is None:
            return await original_page(scope, continuation, files=files, parent=parent)
        update = CaptureRecord(
            identity=parent.identity,
            parent=parent.parent,
            payload={**parent.payload, "name": "Current renamed channel"},
            observed_at=datetime.now(timezone.utc),
        )
        records = (update,)
        if change == "revive":
            records = (
                update.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
                update,
            )
        async with database() as db:
            await service.capture(db, CaptureBatch(fence=fence, records=records))
        raise ScopeAccessLost("Old endpoint routing reported access loss")

    connector.capture_page = change_during_network
    with pytest.raises(ScanConflict, match="changed during provider access"):
        await run(instance)
    rows, cursor, scans = await saved(database)
    current = next(row for row in rows if row.native_id == "C1")
    assert current.deleted_at is None and current.removal_reason is None
    assert current.source_payload["name"] == "Current renamed channel"
    assert current.visibility_epoch == (2 if change == "revive" else 1)
    assert "canonical_checkpoint" not in cursor
    assert next(scan for scan in scans if scan.record_type == "message").phase == "collecting"


async def test_missing_queued_thread_restarts_inventory_before_absence(database, source):
    instance, connector, _ = await runner(
        database,
        source,
        [
            ROOT,
            HISTORY,
            SlackApiError("thread_not_found"),
            {"messages": [{"ts": "2", "text": "Current message"}]},
        ],
    )
    await run(instance)
    assert [call.args[0].split("/")[-1] for call in connector._get.call_args_list] == [
        "conversations.list",
        "conversations.history",
        "conversations.replies",
        "conversations.history",
    ]
    assert "cursor" not in connector._get.call_args_list[-1].args[1]
    rows, cursor, scans = await saved(database)
    old = next(row for row in rows if row.native_id == "1")
    assert old.deleted_at is not None and old.removal_reason == "absent"
    assert next(row for row in rows if row.native_id == "2").deleted_at is None
    assert next(row for row in rows if row.native_id == "C1").deleted_at is None
    assert cursor["canonical_cycle"]["phase"] == "complete"
    assert all(scan.phase == "complete" for scan in scans)


async def test_native_slack_dates_reach_persisted_search_fields_and_date_filters(database, source):
    from datetime import datetime, timezone

    from airweave.domains.entities.canonical.projection_mappers import map_record
    from airweave.domains.entities.canonical.search_metadata import stamp_search_metadata
    from airweave.domains.entities.canonical.store import source_record
    from airweave.domains.search.owned import OwnedSearchService
    from airweave.domains.search.owned_models import OwnedSearchRequest
    from airweave.platform.destinations.vespa.transformer import EntityTransformer
    from airweave.platform.entities._base import AirweaveSystemMetadata

    first, _, _ = await runner(
        database,
        source,
        [
            ROOT,
            {
                "messages": [
                    {"ts": "1767225600.000001", "text": "Before editing"},
                    {
                        "ts": "1767225600.000002",
                        "text": "Edited",
                        "edited": {"ts": "1767312000.000003"},
                    },
                ]
            },
        ],
    )
    await run(first)
    rows, _, _ = await saved(database)
    unchanged = next(row for row in rows if row.native_id == "1767225600.000001")
    edited = next(row for row in rows if row.native_id == "1767225600.000002")
    assert unchanged.source_created_at == datetime(2026, 1, 1, microsecond=1, tzinfo=timezone.utc)
    assert unchanged.source_updated_at is None
    assert edited.source_updated_at == datetime(2026, 1, 2, microsecond=3, tzinfo=timezone.utc)

    # Exercise the same canonical metadata stamper and Vespa serializer used by projection.
    async with map_record(source_record(edited), "slack", MagicMock()) as entities:
        entities = entities.entities
        (entity,) = entities
    entity.airweave_system_metadata = AirweaveSystemMetadata()
    stamp_search_metadata(entity.airweave_system_metadata, source_record(edited))
    fields = EntityTransformer()._build_system_metadata(entity)
    assert fields["source_created_us"] == 1767225600000002
    assert fields["source_updated_us"] == 1767312000000003
    assert fields["source_updated_known"] == 1
    assert fields["canonical_record_type"] == "message"

    # Current-SQL postvalidation enforces inclusive lower and exclusive upper bounds.
    request = OwnedSearchRequest(
        query="Edited",
        sync_ids=(edited.sync_id,),
        created_after=datetime(2026, 1, 1, microsecond=2, tzinfo=timezone.utc),
        updated_after=datetime(2026, 1, 2, microsecond=3, tzinfo=timezone.utc),
    )
    assert OwnedSearchService._matches(edited, request)
    assert not OwnedSearchService._matches(unchanged, request)
    assert not OwnedSearchService._matches(
        edited, request.model_copy(update={"updated_before": edited.source_updated_at})
    )
    stamp_search_metadata(entity.airweave_system_metadata, source_record(unchanged))
    fields = EntityTransformer()._build_system_metadata(entity)
    assert fields["source_updated_known"] == 0
    assert fields["source_updated_us"] is None
