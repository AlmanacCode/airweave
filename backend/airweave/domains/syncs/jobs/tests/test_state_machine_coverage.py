"""Coverage tests for SyncJobStateMachine — publish lifecycle edge cases."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from airweave.adapters.event_bus.fake import FakeEventBus
from airweave.core.events.sync import SyncLifecycleEvent
from airweave.core.shared_models import SourceConnectionErrorCategory, SyncJobStatus
from airweave.domains.syncs.jobs.state_machine import SyncJobStateMachine
from airweave.domains.syncs.jobs.types import LifecycleData

ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
SYNC_ID = UUID("00000000-0000-0000-0000-000000000010")
SYNC_JOB_ID = UUID("00000000-0000-0000-0000-000000000020")
COLLECTION_ID = UUID("00000000-0000-0000-0000-000000000030")
SC_ID = UUID("00000000-0000-0000-0000-000000000050")


def _make_lifecycle():
    return LifecycleData(
        organization_id=ORG_ID,
        sync_id=SYNC_ID,
        sync_job_id=SYNC_JOB_ID,
        collection_id=COLLECTION_ID,
        source_connection_id=SC_ID,
        source_type="test_source",
        collection_name="test",
        collection_readable_id="test",
    )


@pytest.mark.unit
async def test_publish_lifecycle_event_unknown_target():
    """CREATED has no lifecycle event factory → returns immediately."""
    sm = SyncJobStateMachine(
        sync_job_repo=MagicMock(),
        event_bus=FakeEventBus(),
    )

    await sm._publish_lifecycle_event(SyncJobStatus.CREATED, _make_lifecycle())


@pytest.mark.unit
async def test_publish_lifecycle_event_publish_failure():
    """Publish failure logs a warning but doesn't raise."""

    class FailingEventBus:
        async def publish(self, event):
            raise RuntimeError("bus broken")

    sm = SyncJobStateMachine(
        sync_job_repo=MagicMock(),
        event_bus=FailingEventBus(),
    )

    await sm._publish_lifecycle_event(SyncJobStatus.COMPLETED, _make_lifecycle())


@pytest.mark.unit
async def test_publish_lifecycle_event_failed_with_error():
    """FAILED target includes error kwarg when provided."""
    event_bus = FakeEventBus()
    sm = SyncJobStateMachine(
        sync_job_repo=MagicMock(),
        event_bus=event_bus,
    )

    await sm._publish_lifecycle_event(
        SyncJobStatus.FAILED, _make_lifecycle(), error="something broke"
    )

    assert len(event_bus.events) == 1


@pytest.mark.asyncio
async def test_publish_lifecycle_event_failed_with_error_category():
    """FAILED events include error_category in the payload — webhook.

    subscribers use this to distinguish classified user errors from
    real outages without parsing the free-text error field.
    """
    event_bus = FakeEventBus()
    sm = SyncJobStateMachine(
        sync_job_repo=MagicMock(),
        event_bus=event_bus,
    )

    await sm._publish_lifecycle_event(
        SyncJobStatus.FAILED,
        _make_lifecycle(),
        error="JWT expired",
        error_category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
    )

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert isinstance(event, SyncLifecycleEvent)
    assert event.error == "JWT expired"
    assert event.error_category == "oauth_credentials_expired"


@pytest.mark.asyncio
async def test_publish_lifecycle_event_failed_without_category_omits_field():
    """FAILED events without an error_category still publish with error_category=None."""
    event_bus = FakeEventBus()
    sm = SyncJobStateMachine(
        sync_job_repo=MagicMock(),
        event_bus=event_bus,
    )

    await sm._publish_lifecycle_event(SyncJobStatus.FAILED, _make_lifecycle(), error="db down")

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert isinstance(event, SyncLifecycleEvent)
    assert event.error_category is None


@pytest.mark.asyncio
async def test_publish_lifecycle_event_completed_ignores_category():
    """error_category is only meaningful for FAILED events. Passing it for.

    COMPLETED is a no-op (the field is not on the success payload).
    """
    event_bus = FakeEventBus()
    sm = SyncJobStateMachine(
        sync_job_repo=MagicMock(),
        event_bus=event_bus,
    )

    await sm._publish_lifecycle_event(
        SyncJobStatus.COMPLETED,
        _make_lifecycle(),
        error_category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
    )

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert isinstance(event, SyncLifecycleEvent)
    # error_category exists on the event class but is None for non-failed payloads
    assert event.error_category is None


@pytest.mark.asyncio
async def test_transition_failed_publishes_lifecycle_event_with_error_category():
    """End-to-end: transition(FAILED, error_category=...) results in a.

    lifecycle event carrying the category to webhook subscribers.
    """
    repo = MagicMock()
    db_job = MagicMock()
    db_job.id = SYNC_JOB_ID
    db_job.status = SyncJobStatus.RUNNING.value
    repo.get = AsyncMock(return_value=db_job)
    repo.update = AsyncMock()

    event_bus = FakeEventBus()
    sm = SyncJobStateMachine(sync_job_repo=repo, event_bus=event_bus)
    ctx = MagicMock()
    ctx.organization = MagicMock()
    ctx.organization.id = ORG_ID

    with patch("airweave.domains.syncs.jobs.state_machine.get_tenant_db_context") as mock_ctx:
        mock_db = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        await sm.transition(
            sync_job_id=SYNC_JOB_ID,
            target=SyncJobStatus.FAILED,
            ctx=ctx,
            lifecycle_data=_make_lifecycle(),
            error="50103/50000",
            error_category=SourceConnectionErrorCategory.USAGE_LIMIT_EXCEEDED,
        )

    assert len(event_bus.events) == 1
    event = event_bus.events[0]
    assert isinstance(event, SyncLifecycleEvent)
    assert event.error_category == "usage_limit_exceeded"


@pytest.mark.asyncio
async def test_transition_failed_with_error_category():
    """RUNNING → FAILED with error_category writes it to the update object."""
    repo = MagicMock()
    db_job = MagicMock()
    db_job.id = SYNC_JOB_ID
    db_job.status = SyncJobStatus.RUNNING.value
    repo.get = AsyncMock(return_value=db_job)
    repo.update = AsyncMock()

    event_bus = FakeEventBus()
    sm = SyncJobStateMachine(sync_job_repo=repo, event_bus=event_bus)
    ctx = MagicMock()
    ctx.organization = MagicMock()
    ctx.organization.id = ORG_ID

    with patch("airweave.domains.syncs.jobs.state_machine.get_tenant_db_context") as mock_ctx:
        mock_db = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await sm.transition(
            sync_job_id=SYNC_JOB_ID,
            target=SyncJobStatus.FAILED,
            ctx=ctx,
            lifecycle_data=_make_lifecycle(),
            error="cred expired",
            error_category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
        )

    assert result.applied is True
    update_call = repo.update.call_args
    update_obj = update_call.kwargs.get("obj_in") or update_call[0][2]
    assert update_obj.error_category == SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED
