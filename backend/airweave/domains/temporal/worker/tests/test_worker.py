"""Tests for TemporalWorker — mock only Runtime (port-binding side effect).

Every other dependency (PrometheusConfig, TelemetryConfig, CollectorRegistry,
PrometheusWorkerMetrics, PrometheusMetricsRenderer, WorkerControlServer,
WorkerMetricsRegistry) is constructed for real so tests exercise actual wiring.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientSession
from temporalio.runtime import PrometheusConfig, TelemetryConfig

from airweave.domains.temporal.worker.config import WorkerConfig
from airweave.domains.temporal.worker.control_server import WorkerControlServer


def _make_config(**overrides):
    """Build a minimal WorkerConfig for testing."""
    defaults = {
        "task_queue": "test-queue",
        "metrics_port": 8080,
        "graceful_shutdown_timeout_seconds": 30,
    }
    defaults.update(overrides)
    return WorkerConfig(**defaults)


# ── __init__ tests ──────────────────────────────────────────────────


@patch("temporalio.runtime.Runtime")
def test_init_creates_runtime_with_correct_config(mock_runtime_cls):
    """__init__ builds a real TelemetryConfig(PrometheusConfig) and passes it to Runtime."""
    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config(sdk_metrics_port=9999, bind_host="127.0.0.1")
    worker = TemporalWorker(config)

    # Runtime was called exactly once
    mock_runtime_cls.assert_called_once()
    _, kwargs = mock_runtime_cls.call_args

    # The telemetry arg is a real TelemetryConfig, not a mock
    telemetry = kwargs["telemetry"]
    assert isinstance(telemetry, TelemetryConfig)

    # Its metrics member is a real PrometheusConfig with the right address
    assert isinstance(telemetry.metrics, PrometheusConfig)
    assert telemetry.metrics.bind_address == "127.0.0.1:9999"

    # The worker holds the Runtime *instance*
    assert worker._runtime is mock_runtime_cls.return_value


@patch("temporalio.runtime.Runtime")
def test_init_default_sdk_metrics_port(mock_runtime_cls):
    """Default sdk_metrics_port (9090) wires through to PrometheusConfig."""
    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config()  # sdk_metrics_port defaults to 9090
    TemporalWorker(config)

    _, kwargs = mock_runtime_cls.call_args
    assert kwargs["telemetry"].metrics.bind_address == "0.0.0.0:9090"


@patch("temporalio.runtime.Runtime")
def test_init_constructs_real_control_server(mock_runtime_cls):
    """__init__ creates a real WorkerControlServer with real metrics objects."""
    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config()
    worker = TemporalWorker(config)

    assert isinstance(worker._control_server, WorkerControlServer)


# ── start() tests ───────────────────────────────────────────────────


@patch("airweave.domains.temporal.worker.Worker")
@patch("airweave.domains.temporal.worker.get_workflows", return_value=[])
@patch("airweave.domains.temporal.worker.create_activities", return_value=[])
@patch("temporalio.runtime.Runtime")
async def test_start_passes_runtime_to_get_client(
    mock_runtime_cls,
    _mock_activities,
    _mock_workflows,
    mock_worker_cls,
):
    """start() forwards self._runtime when calling get_client()."""
    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config(sdk_metrics_port=9999)
    worker = TemporalWorker(config)
    runtime_instance = worker._runtime

    # Make control_server.start() and Worker.run() no-ops
    worker._control_server.start = AsyncMock()
    mock_worker_cls.return_value.run = AsyncMock()

    with patch(
        "airweave.domains.temporal.client.get_client",
        new_callable=AsyncMock,
    ) as mock_get_client:
        mock_get_client.return_value = MagicMock()
        await worker.start()

        assert mock_worker_cls.call_args.kwargs["max_concurrent_activities"] == 4
        assert mock_worker_cls.call_args.kwargs["max_concurrent_workflow_tasks"] == 8
        mock_get_client.assert_awaited_once()
        _, kwargs = mock_get_client.call_args
        assert kwargs["runtime"] is runtime_instance


# ── _get_sandbox_runner() tests ─────────────────────────────────────


@patch("temporalio.runtime.Runtime")
def test_get_sandbox_runner_default(mock_runtime_cls):
    """With disable_sandbox=False, returns a SandboxedWorkflowRunner."""
    from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config(disable_sandbox=False)
    worker = TemporalWorker(config)

    runner = worker._get_sandbox_runner()
    assert isinstance(runner, SandboxedWorkflowRunner)


@patch("temporalio.runtime.Runtime")
def test_get_sandbox_runner_disabled(mock_runtime_cls):
    """With disable_sandbox=True, returns an UnsandboxedWorkflowRunner."""
    from temporalio.worker import UnsandboxedWorkflowRunner

    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config(disable_sandbox=True)
    worker = TemporalWorker(config)

    runner = worker._get_sandbox_runner()
    assert isinstance(runner, UnsandboxedWorkflowRunner)


# ── stop() tests ────────────────────────────────────────────────────


@patch("airweave.domains.temporal.client.close", new_callable=AsyncMock)
@patch("temporalio.runtime.Runtime")
async def test_stop_shuts_down_worker_and_control_server(mock_runtime_cls, mock_client_close):
    """stop() shuts down the worker, stops the control server, and closes the client."""
    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config()
    worker = TemporalWorker(config)

    # Simulate a running worker
    mock_temporal_worker = AsyncMock()
    worker._worker = mock_temporal_worker
    worker._state.running = True
    worker._control_server.stop = AsyncMock()

    await worker.stop()

    mock_temporal_worker.shutdown.assert_awaited_once()
    worker._control_server.stop.assert_awaited_once()
    mock_client_close.assert_awaited_once()
    assert worker._state.running is False


@patch("airweave.domains.temporal.client.close", new_callable=AsyncMock)
@patch("temporalio.runtime.Runtime")
async def test_stop_skips_shutdown_when_not_running(mock_runtime_cls, mock_client_close):
    """stop() skips worker shutdown if no worker is active."""
    from airweave.domains.temporal.worker import TemporalWorker

    config = _make_config()
    worker = TemporalWorker(config)
    worker._control_server.stop = AsyncMock()

    # _worker is None, _state.running is False — should not raise
    await worker.stop()

    worker._control_server.stop.assert_awaited_once()
    mock_client_close.assert_awaited_once()


@patch("temporalio.runtime.Runtime")
async def test_control_server_binds_only_loopback(mock_runtime_cls):
    """Real aiohttp listener uses the same configured host as SDK metrics."""
    from airweave.domains.temporal.worker import TemporalWorker

    worker = TemporalWorker(_make_config(bind_host="127.0.0.1", metrics_port=0))
    server = worker._control_server
    try:
        await server.start()
        assert server._runner is not None
        addresses = server._runner.addresses
        assert len(addresses) == 1
        host, port = addresses[0]
        assert host == "127.0.0.1"
        async with ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{port}/health") as response:
                assert response.status == 503
                assert await response.text() == "NOT_RUNNING"
    finally:
        await server.stop()


@patch("airweave.domains.temporal.client.close", new_callable=AsyncMock)
@patch("temporalio.runtime.Runtime")
async def test_concurrent_stop_waits_for_one_complete_shutdown(mock_runtime_cls, mock_client_close):
    """Signal and main-finally callers share SDK shutdown and subsequent cleanup."""
    from airweave.domains.temporal.worker import TemporalWorker

    worker = TemporalWorker(_make_config())
    entered = asyncio.Event()
    release = asyncio.Event()

    async def shutdown():
        entered.set()
        await release.wait()

    worker._worker = AsyncMock()
    worker._worker.shutdown.side_effect = shutdown
    worker._state.running = True
    worker._control_server.stop = AsyncMock()
    first = asyncio.create_task(worker.stop())
    await entered.wait()
    second = asyncio.create_task(worker.stop())
    try:
        await asyncio.sleep(0)
        assert not second.done()
        worker._control_server.stop.assert_not_awaited()
        mock_client_close.assert_not_awaited()
    finally:
        release.set()
        await asyncio.gather(first, second)
    await worker.stop()
    worker._worker.shutdown.assert_awaited_once()
    worker._control_server.stop.assert_awaited_once()
    mock_client_close.assert_awaited_once()


@patch("airweave.domains.temporal.client.close", new_callable=AsyncMock)
@patch("temporalio.runtime.Runtime")
async def test_cancelled_stop_waiter_does_not_cancel_shutdown(mock_runtime_cls, mock_client_close):
    """Cancellation of one caller leaves shutdown owned by the shared task."""
    from airweave.domains.temporal.worker import TemporalWorker

    worker = TemporalWorker(_make_config())
    entered = asyncio.Event()
    release = asyncio.Event()

    async def shutdown():
        entered.set()
        await release.wait()

    worker._worker = AsyncMock()
    worker._worker.shutdown.side_effect = shutdown
    worker._state.running = True
    worker._control_server.stop = AsyncMock()
    caller = asyncio.create_task(worker.stop())
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    release.set()
    await worker.stop()
    worker._worker.shutdown.assert_awaited_once()
    worker._control_server.stop.assert_awaited_once()
    mock_client_close.assert_awaited_once()


@patch("temporalio.runtime.Runtime")
async def test_stop_failure_remains_observable_to_later_callers(mock_runtime_cls):
    """A failed shutdown cannot become an apparent successful repeated stop."""
    from airweave.domains.temporal.worker import TemporalWorker

    worker = TemporalWorker(_make_config())
    worker._worker = AsyncMock()
    worker._worker.shutdown.side_effect = RuntimeError("shutdown failed")
    worker._state.running = True
    worker._control_server.stop = AsyncMock()
    for _ in range(2):
        with pytest.raises(RuntimeError, match="shutdown failed"):
            await worker.stop()
    worker._worker.shutdown.assert_awaited_once()
    worker._control_server.stop.assert_not_awaited()


@patch("airweave.domains.temporal.worker.create_activities", return_value=[])
@patch("airweave.domains.temporal.worker.get_workflows", return_value=[])
@pytest.mark.parametrize("blocked_phase", ["control", "client"])
@patch("airweave.domains.temporal.client.close", new_callable=AsyncMock)
@patch("airweave.domains.temporal.worker.Worker")
@patch("temporalio.runtime.Runtime")
async def test_stop_during_startup_waits_then_prevents_polling(
    mock_runtime_cls,
    mock_worker_cls,
    mock_client_close,
    mock_workflows,
    mock_activities,
    blocked_phase,
):
    """Cleanup waits for resource creation and no SDK worker starts after stop."""
    from airweave.domains.temporal.worker import TemporalWorker

    worker = TemporalWorker(_make_config())
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked(*args, **kwargs):
        entered.set()
        await release.wait()
        return MagicMock()

    worker._control_server.start = AsyncMock(
        side_effect=blocked if blocked_phase == "control" else None
    )
    worker._control_server.stop = AsyncMock()
    mock_worker_cls.return_value.run = AsyncMock()
    with patch("airweave.domains.temporal.client.get_client", new_callable=AsyncMock) as client:
        if blocked_phase == "client":
            client.side_effect = blocked
        starting = asyncio.create_task(worker.start())
        await entered.wait()
        stopping = asyncio.create_task(worker.stop())
        try:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not stopping.done()
            worker._control_server.stop.assert_not_awaited()
            mock_client_close.assert_not_awaited()
        finally:
            release.set()
            await asyncio.gather(starting, stopping)
        mock_worker_cls.assert_not_called()
        if blocked_phase == "control":
            client.assert_not_awaited()
    worker._control_server.stop.assert_awaited_once()
    mock_client_close.assert_awaited_once()
