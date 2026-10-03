"""Tests for SyncOrchestrator exception paths and heartbeat publication."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from airweave.core.shared_models import SyncJobStatus, SyncStatus
from airweave.domains.sync_pipeline.orchestrator import SyncOrchestrator


def _make_sync_context(sync_id=None, sync_job_id=None, org_id=None):
    ctx = MagicMock()
    ctx.sync = SimpleNamespace(id=sync_id or uuid4())
    ctx.sync_job = SimpleNamespace(id=sync_job_id or uuid4())
    ctx.organization = SimpleNamespace(id=org_id or uuid4())
    ctx.organization_id = ctx.organization.id
    ctx.sync_job.id = ctx.sync_job.id
    ctx.source_connection_id = uuid4()
    ctx.source_short_name = "test_source"
    ctx.should_batch = False
    ctx.batch_size = 10
    ctx.max_batch_latency_ms = 100
    ctx.logger = MagicMock()
    return ctx


def _make_orchestrator(**overrides):
    sync_context = overrides.pop("sync_context", _make_sync_context())
    worker_pool = overrides.pop("worker_pool", MagicMock())
    worker_pool.max_workers = 4

    usage_ledger = overrides.pop("usage_ledger", MagicMock())
    if not hasattr(usage_ledger, "flush") or not callable(usage_ledger.flush):
        usage_ledger.flush = AsyncMock()

    usage_checker = overrides.pop("usage_checker", MagicMock())

    return SyncOrchestrator(
        entity_pipeline=overrides.pop("entity_pipeline", MagicMock()),
        worker_pool=worker_pool,
        stream=overrides.pop("stream", MagicMock()),
        sync_context=sync_context,
        runtime=overrides.pop("runtime", MagicMock(canonical_capture=None)),
        access_control_pipeline=overrides.pop("access_control_pipeline", MagicMock()),
        event_bus=overrides.pop("event_bus", MagicMock()),
        usage_checker=usage_checker,
        usage_ledger=usage_ledger,
        sync_cursor_service=overrides.pop("sync_cursor_service", MagicMock()),
        state_machine=overrides.pop("state_machine", MagicMock()),
        lifecycle_data=overrides.pop("lifecycle_data", MagicMock()),
        sync_state_machine=overrides.pop("sync_state_machine", MagicMock()),
    )


class TestUsageLedgerFlushFailure:
    @pytest.mark.asyncio
    async def test_flush_failure_does_not_mask_original_exception(self):
        """If _usage_ledger.flush raises inside finally, original exception still propagates."""
        from airweave.domains.sync_pipeline.exceptions import SyncFailureError

        usage_ledger = MagicMock()
        usage_ledger.flush = AsyncMock(side_effect=RuntimeError("redis down"))

        orc = _make_orchestrator(usage_ledger=usage_ledger)

        with (
            patch.object(
                orc,
                "_start_sync",
                new_callable=AsyncMock,
                side_effect=SyncFailureError("source failed"),
            ),
            # Returning None signals "unclassified" — original exception
            # propagates instead of being wrapped as ClassifiedUserError.
            patch.object(
                orc,
                "_handle_sync_failure",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch.object(orc, "entity_pipeline", MagicMock()),
            patch(
                "airweave.domains.sync_pipeline.orchestrator.worker_metrics",
                create=True,
            ),
        ):
            with pytest.raises(SyncFailureError, match="source failed"):
                await orc.run()

        orc.sync_context.logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_flush_is_attempted_in_finally_after_success_path(self):
        """Even on successful sync, flush is called exactly once."""
        usage_ledger = MagicMock()
        usage_ledger.flush = AsyncMock()

        orc = _make_orchestrator(usage_ledger=usage_ledger)

        with (
            patch.object(orc, "_start_sync", new_callable=AsyncMock),
            patch.object(orc, "_process_entities", new_callable=AsyncMock),
            patch.object(orc, "_source_supports_access_control", return_value=False),
            patch.object(orc, "_cleanup_orphaned_entities_if_needed", new_callable=AsyncMock),
            patch.object(orc, "_complete_sync", new_callable=AsyncMock),
            patch.object(orc, "entity_pipeline", MagicMock()),
        ):
            await orc.run()

        usage_ledger.flush.assert_awaited_once_with(orc.sync_context.organization.id)


class TestPublishAclHeartbeat:
    @pytest.mark.asyncio
    async def test_publishes_correct_event(self):
        """_publish_acl_heartbeat publishes AccessControlMembershipBatchProcessedEvent."""
        from airweave.core.events.sync import AccessControlMembershipBatchProcessedEvent

        event_bus = MagicMock()
        event_bus.publish = AsyncMock()

        orc = _make_orchestrator(event_bus=event_bus)

        await orc._publish_acl_heartbeat()

        event_bus.publish.assert_awaited_once()
        event = event_bus.publish.call_args[0][0]
        assert isinstance(event, AccessControlMembershipBatchProcessedEvent)
        assert event.sync_id == orc.sync_context.sync.id
        assert event.organization_id == orc.sync_context.organization_id


# ===========================================================================
# _handle_sync_failure — credential error classification + schedule pause
# ===========================================================================


class TestHandleSyncFailure:
    @pytest.mark.asyncio
    async def test_credential_error_writes_error_category_and_pauses(self):
        """Auth error -> error_category on transition + sync paused via state machine."""
        from airweave.core.shared_models import SourceConnectionErrorCategory, SyncStatus
        from airweave.domains.sources.exceptions import SourceAuthError
        from airweave.domains.sources.token_providers.protocol import AuthProviderKind

        state_machine = AsyncMock()
        sync_state_machine = AsyncMock()

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=state_machine,
            sync_state_machine=sync_state_machine,
        )

        exc = SourceAuthError(
            "401 Unauthorized",
            source_short_name="github",
            status_code=401,
            token_provider_kind=AuthProviderKind.OAUTH,
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            await orc._handle_sync_failure(exc)

        state_machine.transition.assert_awaited_once()
        call_kwargs = state_machine.transition.call_args.kwargs
        assert (
            call_kwargs["error_category"] == SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED
        )
        assert call_kwargs["target"] == SyncJobStatus.FAILED

        sync_state_machine.transition.assert_awaited_once()
        pause_kwargs = sync_state_machine.transition.call_args.kwargs
        assert pause_kwargs["target"] == SyncStatus.PAUSED

    @pytest.mark.asyncio
    async def test_non_credential_error_no_category_no_pause(self):
        """Non-auth error -> error_category=None, no pause."""
        state_machine = AsyncMock()
        sync_state_machine = AsyncMock()

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=state_machine,
            sync_state_machine=sync_state_machine,
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            await orc._handle_sync_failure(RuntimeError("network timeout"))

        call_kwargs = state_machine.transition.call_args.kwargs
        assert call_kwargs["error_category"] is None

        sync_state_machine.transition.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pause_failure_is_nonfatal(self):
        """If sync state machine raises, failure handler still completes."""
        from airweave.domains.sources.exceptions import SourceAuthError
        from airweave.domains.sources.token_providers.protocol import AuthProviderKind
        from airweave.domains.syncs.types import InvalidSyncTransitionError

        state_machine = AsyncMock()
        sync_state_machine = AsyncMock()
        sync_state_machine.transition.side_effect = InvalidSyncTransitionError(
            SyncStatus.PAUSED, SyncStatus.PAUSED
        )

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=state_machine,
            sync_state_machine=sync_state_machine,
        )

        exc = SourceAuthError(
            "401",
            source_short_name="github",
            status_code=401,
            token_provider_kind=AuthProviderKind.OAUTH,
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            await orc._handle_sync_failure(exc)

        state_machine.transition.assert_awaited_once()
        ctx.logger.warning.assert_called()

    @pytest.mark.asyncio
    async def test_returns_category_for_classified_error(self):
        """_handle_sync_failure returns the classified category, allowing.

        run() to decide whether to wrap the exception.
        """
        from airweave.core.shared_models import SourceConnectionErrorCategory
        from airweave.domains.sources.exceptions import SourceAuthError
        from airweave.domains.sources.token_providers.protocol import AuthProviderKind

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=AsyncMock(),
            sync_state_machine=AsyncMock(),
        )

        exc = SourceAuthError(
            "401",
            source_short_name="github",
            status_code=401,
            token_provider_kind=AuthProviderKind.OAUTH,
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            category = await orc._handle_sync_failure(exc)

        assert category == SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED

    @pytest.mark.asyncio
    async def test_returns_none_for_unclassified_error(self):
        """A true system failure returns None so run() re-raises unwrapped."""
        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=AsyncMock(),
            sync_state_machine=AsyncMock(),
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            category = await orc._handle_sync_failure(RuntimeError("db down"))

        assert category is None

    @pytest.mark.asyncio
    async def test_rate_limit_does_not_pause(self):
        """Rate-limit errors return the RATE_LIMITED category but do NOT.

        pause the sync — the next scheduled run will succeed once the
        window resets.
        """
        from airweave.core.shared_models import SourceConnectionErrorCategory
        from airweave.domains.sources.exceptions import SourceRateLimitError

        state_machine = AsyncMock()
        sync_state_machine = AsyncMock()

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=state_machine,
            sync_state_machine=sync_state_machine,
        )

        exc = SourceRateLimitError(retry_after=60.0, source_short_name="hubspot")

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            category = await orc._handle_sync_failure(exc)

        assert category == SourceConnectionErrorCategory.RATE_LIMITED
        # Sync state machine NOT called for rate-limited — sync stays ACTIVE.
        sync_state_machine.transition.assert_not_awaited()
        # But the job transition still records the category.
        state_machine.transition.assert_awaited_once()
        call_kwargs = state_machine.transition.call_args.kwargs
        assert call_kwargs["error_category"] == SourceConnectionErrorCategory.RATE_LIMITED

    @pytest.mark.asyncio
    async def test_usage_limit_error_pauses_and_classifies(self):
        """Usage limit errors classify as USAGE_LIMIT_EXCEEDED and pause."""
        from airweave.core.shared_models import SourceConnectionErrorCategory, SyncStatus
        from airweave.domains.usage.exceptions import UsageLimitExceededError

        state_machine = AsyncMock()
        sync_state_machine = AsyncMock()

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=state_machine,
            sync_state_machine=sync_state_machine,
        )

        exc = UsageLimitExceededError(action_type="entities", limit=50000, current_usage=50103)

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            category = await orc._handle_sync_failure(exc)

        assert category == SourceConnectionErrorCategory.USAGE_LIMIT_EXCEEDED
        sync_state_machine.transition.assert_awaited_once()
        pause_kwargs = sync_state_machine.transition.call_args.kwargs
        assert pause_kwargs["target"] == SyncStatus.PAUSED

    @pytest.mark.asyncio
    async def test_classified_error_logs_at_warning(self):
        """Classified failures log at WARNING — they're customer state, not.

        Airweave outages.
        """
        from airweave.domains.sources.exceptions import SourceAuthError
        from airweave.domains.sources.token_providers.protocol import AuthProviderKind

        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=AsyncMock(),
            sync_state_machine=AsyncMock(),
        )

        exc = SourceAuthError(
            "401",
            source_short_name="github",
            status_code=401,
            token_provider_kind=AuthProviderKind.OAUTH,
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            await orc._handle_sync_failure(exc)

        ctx.logger.warning.assert_called()
        ctx.logger.error.assert_not_called()

    @pytest.mark.asyncio
    async def test_unclassified_error_logs_at_error(self):
        """Unclassified failures (true system errors) log at ERROR with traceback."""
        ctx = _make_sync_context()
        ctx.sync_job.started_at = None

        orc = _make_orchestrator(
            sync_context=ctx,
            state_machine=AsyncMock(),
            sync_state_machine=AsyncMock(),
        )

        with patch("airweave.domains.sync_pipeline.orchestrator.business_events"):
            await orc._handle_sync_failure(RuntimeError("db down"))

        ctx.logger.error.assert_called()


# ===========================================================================
# Orchestrator.run() — classified errors are wrapped, system errors are not
# ===========================================================================


class TestRunWrapsClassifiedErrors:
    @pytest.mark.asyncio
    async def test_classified_error_wrapped_as_application_error(self):
        """When the sync raises a classified error, run() wraps it as an.

        ApplicationError of type CLASSIFIED_USER_ERROR_TYPE so the workflow
        completes normally instead of incrementing temporal_workflow_failed.
        """
        from temporalio.exceptions import ApplicationError

        from airweave.core.shared_models import SourceConnectionErrorCategory
        from airweave.domains.sources.exceptions import SourceAuthError
        from airweave.domains.sources.token_providers.protocol import AuthProviderKind
        from airweave.domains.temporal.exceptions import CLASSIFIED_USER_ERROR_TYPE

        orc = _make_orchestrator()
        exc = SourceAuthError(
            "401",
            source_short_name="github",
            status_code=401,
            token_provider_kind=AuthProviderKind.OAUTH,
        )

        async def _fake_handler(_e: Exception):
            return SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED

        with (
            patch.object(
                orc,
                "_start_sync",
                new_callable=AsyncMock,
                side_effect=exc,
            ),
            patch.object(orc, "_handle_sync_failure", side_effect=_fake_handler),
            patch.object(orc, "entity_pipeline", MagicMock()),
        ):
            with pytest.raises(ApplicationError) as exc_info:
                await orc.run()

        assert exc_info.value.type == CLASSIFIED_USER_ERROR_TYPE
        assert exc_info.value.non_retryable is True
        assert exc_info.value.__cause__ is exc

    @pytest.mark.asyncio
    async def test_unclassified_error_not_wrapped(self):
        """A true system failure propagates unwrapped — the workflow records.

        a real temporal_workflow_failed.
        """
        orc = _make_orchestrator()
        original = RuntimeError("redis down")

        async def _fake_handler(_e: Exception):
            return None

        with (
            patch.object(
                orc,
                "_start_sync",
                new_callable=AsyncMock,
                side_effect=original,
            ),
            patch.object(orc, "_handle_sync_failure", side_effect=_fake_handler),
            patch.object(orc, "entity_pipeline", MagicMock()),
        ):
            with pytest.raises(RuntimeError, match="redis down"):
                await orc.run()


async def test_checkpoint_failure_prevents_completed_transition():
    """A successfully processed stream is incomplete if checkpoint persistence fails."""
    service = MagicMock()
    service.create_or_update_cursor = AsyncMock(side_effect=ConnectionError("database unavailable"))
    state_machine = MagicMock()
    state_machine.transition = AsyncMock()
    ctx = _make_sync_context()
    ctx.execution_config = None
    runtime = MagicMock()
    runtime.canonical_capture = None
    runtime.cursor = SimpleNamespace(cursor_data={"history_id": "42"}, cursor_field=None)
    orch = _make_orchestrator(
        sync_context=ctx,
        runtime=runtime,
        sync_cursor_service=service,
        state_machine=state_machine,
    )
    db_context = MagicMock()
    db_context.__aenter__ = AsyncMock(return_value=MagicMock())
    db_context.__aexit__ = AsyncMock(return_value=False)
    with patch(
        "airweave.domains.sync_pipeline.orchestrator.get_tenant_db_context", return_value=db_context
    ):
        with pytest.raises(ConnectionError, match="database unavailable"):
            await orch._complete_sync()
    service.create_or_update_cursor.assert_awaited_once()
    state_machine.transition.assert_not_awaited()
