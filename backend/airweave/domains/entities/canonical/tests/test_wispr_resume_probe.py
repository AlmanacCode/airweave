"""Actual process exit/restart around committed Wispr pages; synthetic provider only."""

import json

from sqlalchemy import text

from airweave.domains.entities.canonical.tests.test_resume_probe import subprocess_script

SCRIPT = r"""
import asyncio, os, sys
from pathlib import Path
from uuid import uuid4
sys.path.insert(0, "tests/live")
import conftest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from airweave.domains.entities.canonical.requests import WriterFence
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.test_wispr_recovery import connector, driver
from wispr_resume import WisprResumeProbe
from provider_lifecycle import BoundedPageSource
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from unittest.mock import MagicMock

async def main():
    engine = create_async_engine(os.environ["CANONICAL_TEST_DATABASE_URL"],
        connect_args={"server_settings": {"search_path": os.environ["TEST_SCHEMA"]}})
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    fence = WriterFence.model_validate_json(os.environ["TEST_FENCE"])
    service = CanonicalCaptureService(CanonicalRecordStore())
    mode = os.environ["TEST_MODE"]
    if mode == "resume":
        async with sessions() as db:
            fence = await service.activate_writer(db, fence.organization_id, fence.sync_id,
                fence.job_id, attempt_id=uuid4(), attempt_number=2)
    calls = []
    source = await connector([{"id": str(i)} for i in range(6)], calls,
        fail_at=2 if mode == "fail" else None)
    if os.environ.get("TEST_MIXED") == "1":
        async def mixed(slug, arguments):
            rows=[{"id":"private-same-id-"+str(i)} for i in range(3)]
            if slug == "WISPR_FLOW_MCP_SEARCH_MEETINGS":
                return {"meetings":rows,"has_more":False}
            if slug == "WISPR_FLOW_MCP_SEARCH_SCRATCHPAD_NOTES":
                return {"notes":rows,"has_more":False}
            if slug == "WISPR_FLOW_MCP_GET_MEETING":
                return {"id":arguments["meeting_id"],"content":"private meeting text",
                        "transcript":"private transcript text"}
            assert slug == "WISPR_FLOW_MCP_GET_SCRATCHPAD_NOTE"
            return {"id":arguments["note_id"],"content":"private scratchpad text"}
        source._execute = mixed
    counters = {"provider_requests": 0, "records_observed": 0}
    execute = source._execute
    async def counted(*args):
        counters["provider_requests"] += 1
        return await execute(*args)
    source._execute = counted
    probe = WisprResumeProbe(sessions, fence.organization_id, fence.sync_id, fence.job_id,
        fence.attempt_id, Path(os.environ["TEST_TARGET"]),
        "interrupt" if mode == "fail" else mode, counters)
    ctx, _, runtime, bus = components(sessions, (service, fence))
    wrapped = BoundedPageSource(source, counters, 600, probe)
    if mode == "wrong_topology":
        parents = {}
    else:
        parents = source.canonical_container_parents
    pipeline = CanonicalCapturePipeline(service, sessions, bus, source.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id, number=fence.attempt_number), parents,
        page_source=wrapped, files=MagicMock())
    runtime.source, runtime.canonical_capture = source, pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    await run(instance)
asyncio.run(main())
"""


async def test_wispr_process_restart_skips_completed_bodies(database, source, tmp_path):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    env = {
        "TEST_SCHEMA": schema,
        "TEST_FENCE": source[1].model_dump_json(),
        "TEST_TARGET": str(tmp_path / "target.json"),
    }
    code, output, error = await subprocess_script(SCRIPT, {**env, "TEST_MODE": "interrupt"})
    assert code == 75, error
    first = json.loads(output.splitlines()[-1])
    assert first["completed_bodies"] == 3 and first["provider_requests"] == 5
    code, output, error = await subprocess_script(SCRIPT, {**env, "TEST_MODE": "resume"})
    assert code == 76, error
    second = json.loads(output.splitlines()[-1])
    assert second["wispr_recovery_verified"] and second["provider_requests"] == 3
    assert not second["full_scope_completed"]
    assert (tmp_path / "target.json").stat().st_mode & 0o077 == 0


async def test_wispr_failure_does_not_claim_interruption(database, source, tmp_path):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    code, output, error = await subprocess_script(
        SCRIPT,
        {
            "TEST_SCHEMA": schema,
            "TEST_FENCE": source[1].model_dump_json(),
            "TEST_TARGET": str(tmp_path / "target.json"),
            "TEST_MODE": "fail",
        },
    )
    assert code not in (75, 76)
    assert "Synthetic opaque body failure" in error
    assert not (tmp_path / "target.json").exists()


async def test_wispr_trial_budget_is_aggregate_and_failure_stops(tmp_path):
    script = r"""
import asyncio, os, sys
sys.path.insert(0, "tests/live")
import conftest
import provider_lifecycle as lifecycle
async def main():
    for fail in (False, True):
        snapshots = []
        async def execute(path, timeout):
            import json
            value = json.loads(path.read_text())
            snapshots.append(value)
            if fail:
                return 1, {"failed": True, "provider_requests": 2, "records_observed": 0}
            second = len(snapshots) == 2
            return (76 if second else 75), {"failed": False,
                "intentional_interruption": not second, "wispr_recovery_verified": second,
                "provider_requests": 7, "records_observed": 203}
        lifecycle.execute_trial = execute
        results = await lifecycle.run_trials({"provider": "wispr", "wispr_resume": True,
            "root": os.environ["TEST_ROOT"], "request_limit": 20,
            "record_limit": 600, "timeout": 180})
        if fail:
            assert len(snapshots) == 1
        else:
            assert len(snapshots) == 2
            assert [v["request_limit"] for v in snapshots] == [20, 13]
            assert [v["record_limit"] for v in snapshots] == [600, 397]
            assert snapshots[0]["job_id"] == snapshots[1]["job_id"]
            assert [v["attempt_number"] for v in snapshots] == [1, 2]
            assert 180 >= snapshots[0]["timeout"] >= snapshots[1]["timeout"] > 0
    print("verified")
asyncio.run(main())
"""
    code, output, error = await subprocess_script(script, {"TEST_ROOT": str(tmp_path)})
    assert code == 0, error
    assert output.splitlines()[-1] == "verified"


async def test_wispr_pipeline_rejects_actual_topology_mismatch(database, source, tmp_path):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    code, output, error = await subprocess_script(
        SCRIPT,
        {
            "TEST_SCHEMA": schema,
            "TEST_FENCE": source[1].model_dump_json(),
            "TEST_TARGET": str(tmp_path / "target.json"),
            "TEST_MODE": "wrong_topology",
        },
    )
    assert code not in (0, 75, 76)
    assert "Page source cycle must match its declared container topology" in error


async def test_wispr_legacy_target_is_rejected_without_private_output(tmp_path):
    script = r"""
import sys, json, os
from pathlib import Path
from uuid import uuid4
sys.path.insert(0,"tests/live")
import conftest
from wispr_resume import WisprResumeProbe
path=Path(os.environ["TARGET"])
path.write_text(json.dumps({"cycle_id":str(uuid4()),"bodies":[]}))
try:
    WisprResumeProbe(None,None,None,None,None,path,"resume",{})
except ValueError as exc:
    assert str(exc)=="Incompatible Wispr resume target; start a new private v3 trial"
else:
    raise AssertionError("Legacy target accepted")
print("verified")
"""
    code, output, error = await subprocess_script(
        script, {"TARGET": str(tmp_path / "old-target.json")}
    )
    assert code == 0, error
    assert output.strip() == "verified"


async def test_mixed_kind_native_id_collision_survives_real_process_resume(
    database, source, tmp_path
):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    target = tmp_path / "mixed-target.json"
    env = {
        "TEST_SCHEMA": schema,
        "TEST_FENCE": source[1].model_dump_json(),
        "TEST_TARGET": str(target),
        "TEST_MIXED": "1",
    }
    code, output, error = await subprocess_script(SCRIPT, {**env, "TEST_MODE": "interrupt"})
    assert code == 75, error
    saved = json.loads(target.read_text())
    assert saved["schema_version"] == 3 and len(saved["configuration_digest"]) == 64
    assert len(saved["bodies"]) == 3
    assert all("record_type" in body for body in saved["bodies"])
    assert "private" not in output and "private" not in target.read_text()
    code, output, error = await subprocess_script(SCRIPT, {**env, "TEST_MODE": "resume"})
    assert code == 76, error
    evidence = json.loads(output.splitlines()[-1])
    assert evidence["wispr_recovery_verified"] and evidence["completed_bodies"] == 4
    assert all(count > 0 for count in evidence["completed_body_kinds"].values())
    assert "private" not in output
    assert target.stat().st_mode & 0o077 == 0
