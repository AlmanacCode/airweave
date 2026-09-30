"""Local subprocess verification for the opt-in live harness, with no provider credentials."""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import text

BACKEND = Path(__file__).resolve().parents[5]


async def subprocess_script(script, environment):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        cwd=BACKEND,
        env={**os.environ, **environment, "PYTHONPATH": str(BACKEND)},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    return process.returncode, stdout.decode(), stderr.decode()


PROCESS_SCRIPT = r"""
import asyncio, json, os, sys
from pathlib import Path
sys.path.insert(0, "tests/live")
import conftest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from airweave.domains.entities.canonical.requests import WriterFence, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.test_slack_recovery import (
    runner, run, ROOT, HISTORY, REPLIES,
)
from capture_resume import PageResumeProbe, inspect_resume_progress
from provider_lifecycle import BoundedPageSource

async def main():
    engine = create_async_engine(os.environ["CANONICAL_TEST_DATABASE_URL"],
        connect_args={"server_settings": {"search_path": os.environ["TEST_SCHEMA"]}})
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    fence = WriterFence.model_validate_json(os.environ["TEST_FENCE"])
    mode = os.environ["TEST_MODE"]
    service = CanonicalCaptureService(CanonicalRecordStore())
    from airweave.platform.sources.slack import SlackApiError
    retry = mode in ("resume", "withdraw", "expired")
    responses = [ROOT, REPLIES] if retry else [ROOT, HISTORY, REPLIES]
    if mode == "withdraw":
        responses = [ROOT, SlackApiError("not_in_channel")]
    if mode == "expired":
        responses = [ROOT, SlackApiError("invalid_cursor"), HISTORY, REPLIES]
    instance, connector, pipeline = runner(sessions, (service, fence), responses,
                                          attempt=2 if retry else 1)
    counters = {"provider_requests": 0, "records_observed": 0}
    get = connector._get
    async def counted_get(*args):
        counters["provider_requests"] += 1
        return await get(*args)
    connector._get = counted_get
    probe = PageResumeProbe(sessions, fence.organization_id, fence.sync_id, fence.job_id,
        pipeline._attempt.id, Path(os.environ["TEST_TARGET"]),
        "resume" if retry else "interrupt", counters)
    if mode == "uncommitted":
        await probe.before_page(CompletedScope(record_type="message", container_id="C1"),
                                ScanContinuation(value={"pending_threads": ["1"]}))
        raise AssertionError("Uncommitted progress was accepted")
    pipeline.page_source = BoundedPageSource(connector, counters, 100, probe)
    await run(instance)
    evidence = await inspect_resume_progress(sessions, fence.organization_id, fence.sync_id,
        fence.job_id, Path(os.environ["TEST_TARGET"]))
    print(json.dumps({**evidence, "resumed": probe.resumed_saved_page,
                      "expired": probe.saved_cursor_expired, **counters}), flush=True)
    await engine.dispose()
asyncio.run(main())
"""


async def test_process_loss_after_commit_then_fresh_same_job_resume(database, source, tmp_path):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    environment = {
        "TEST_SCHEMA": schema,
        "TEST_FENCE": source[1].model_dump_json(),
        "TEST_TARGET": str(tmp_path / "target.json"),
        "TEST_MODE": "uncommitted",
    }
    code, stdout, stderr = await subprocess_script(PROCESS_SCRIPT, environment)
    assert code != 75 and "Durable interruption state is missing" in stderr
    assert not (tmp_path / "target.json").exists()
    code, stdout, stderr = await subprocess_script(
        PROCESS_SCRIPT, {**environment, "TEST_MODE": "interrupt"}
    )
    assert code == 75, stderr
    stopped = json.loads(stdout.splitlines()[-1])
    assert stopped["durable_pending_threads_verified"] and stopped["job_status"] == "running"
    assert stopped["provider_requests"] == 2 and stopped["records_observed"] == 2
    assert stopped["full_scope_completed"] is False
    assert (tmp_path / "target.json").stat().st_mode & 0o077 == 0
    code, stdout, stderr = await subprocess_script(
        PROCESS_SCRIPT, {**environment, "TEST_MODE": "resume"}
    )
    assert code == 0, stderr
    resumed = json.loads(stdout.splitlines()[-1])
    assert resumed == {
        "resumed_page_committed": True,
        "resumed": True,
        "expired": False,
        "provider_requests": 2,
        "records_observed": 3,
    }


@pytest.mark.parametrize("mode", ["withdraw", "expired"])
async def test_withdrawal_or_restarted_sweep_does_not_prove_saved_reply_commit(
    database, source, tmp_path, mode
):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    environment = {
        "TEST_SCHEMA": schema,
        "TEST_FENCE": source[1].model_dump_json(),
        "TEST_TARGET": str(tmp_path / "target.json"),
        "TEST_MODE": "interrupt",
    }
    code, _, stderr = await subprocess_script(PROCESS_SCRIPT, environment)
    assert code == 75, stderr
    code, stdout, stderr = await subprocess_script(
        PROCESS_SCRIPT, {**environment, "TEST_MODE": mode}
    )
    assert code == 0, stderr
    evidence = json.loads(stdout.splitlines()[-1])
    assert evidence["resumed_page_committed"] is False
    assert evidence["expired"] is (mode == "expired")


BUDGET_SCRIPT = r"""
import asyncio, json, os, sys
from pathlib import Path
sys.path.insert(0, "tests/live")
import conftest
import provider_lifecycle as lifecycle

async def main():
    snapshots = []
    outputs = [
        {"failed": False, "intentional_interruption": True,
         "provider_requests": 2, "records_observed": 5},
        {"failed": False, "full_scope_completed": True, "resumed_saved_page": True,
         "provider_requests": 3, "records_observed": 7},
        {"failed": False, "full_scope_completed": True,
         "provider_requests": 1, "records_observed": 2},
    ]
    killed = []
    class Process:
        def __init__(self, index):
            self.index = index
            self.returncode = (None if (os.environ["TEST_MODE"] == "timeout" or
                                      (os.environ["TEST_MODE"] == "partial" and index == 1))
                               else (75 if index == 0 else 0))
        async def communicate(self):
            if self.returncode is None:
                raise asyncio.TimeoutError()
            return json.dumps(outputs[self.index]).encode(), b""
        def kill(self):
            killed.append(True)
            self.returncode = -9
        async def wait(self):
            return self.returncode
    async def spawn(*args, **kwargs):
        snapshots.append(json.loads(Path(args[2]).read_text()))
        return Process(len(snapshots)-1)
    lifecycle.asyncio.create_subprocess_exec = spawn
    async def evidence():
        return {"resumed_page_committed": True}
    result = await lifecycle.run_trials({"provider": "slack", "root": os.environ["TEST_ROOT"],
                                        "timeout": 100, "request_limit": 20, "record_limit": 50},
                                        evidence)
    if os.environ["TEST_MODE"] in ("timeout", "partial"):
        assert len(snapshots) == (2 if os.environ["TEST_MODE"] == "partial" else 1) and killed
        if os.environ["TEST_MODE"] == "partial":
            assert result[-1]["resumed_partial"] is True
            assert result[-1]["full_scope_completed"] is False
        summary = lifecycle.trial_counter_summary(result)
        assert summary["aggregate_counters_complete"] is False
        assert summary["aggregate_provider_requests"] is None
        assert summary["aggregate_records_observed"] is None
    else:
        assert [item["request_limit"] for item in snapshots] == [20,18,15]
        assert [item["record_limit"] for item in snapshots] == [50,45,38]
        assert snapshots[0]["job_id"] == snapshots[1]["job_id"] != snapshots[2]["job_id"]
        assert [item["attempt_number"] for item in snapshots] == [1,2,1]
        assert 100 >= snapshots[0]["timeout"] >= snapshots[1]["timeout"] >= snapshots[2]["timeout"]
        assert lifecycle.trial_counter_summary(result)["aggregate_provider_requests"] == 6
    print("verified")
asyncio.run(main())
"""


async def test_parent_trial_keeps_aggregate_budget_and_marks_killed_usage_unknown(tmp_path):
    for mode in ("normal", "timeout", "partial"):
        code, stdout, stderr = await subprocess_script(
            BUDGET_SCRIPT,
            {
                "TEST_MODE": mode,
                "TEST_ROOT": str(tmp_path),
            },
        )
        assert code == 0, stderr
        assert stdout.splitlines()[-1] == "verified"
