"""Gmail harness proof uses fresh processes and isolated PostgreSQL, no live provider."""

import json

from sqlalchemy import text

from airweave.domains.entities.canonical.tests.test_resume_probe import subprocess_script

SCRIPT = r"""
import asyncio, os, sys
from pathlib import Path
from uuid import uuid4
from unittest.mock import MagicMock
sys.path.insert(0, "tests/live")
import conftest
import httpx
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from airweave.domains.entities.canonical.requests import WriterFence
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run
from airweave.domains.entities.canonical.tests.test_gmail_recovery import NativeHTTP
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.platform.configs.config import GmailConfig
from airweave.platform.sources.gmail import GmailSource
from airweave.platform.sources.tests.test_gmail_capture import message
from gmail_resume import GmailResumeProbe
from provider_lifecycle import bounded_page_source

async def main():
    engine = create_async_engine(os.environ["CANONICAL_TEST_DATABASE_URL"],
        connect_args={"server_settings": {"search_path": os.environ["TEST_SCHEMA"]}})
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    fence = WriterFence.model_validate_json(os.environ["TEST_FENCE"])
    service = CanonicalCaptureService(CanonicalRecordStore())
    mode = os.environ["TEST_MODE"]
    responses = [('/profile', {'historyId': 'initial'}),
        ('/messages', {'messages': [{'id': 'a'}], 'nextPageToken': 'next'}),
        ('/messages/a', message('a'))] if mode == 'interrupt' else [
        ('/history', {'historyId': 'later', 'history': [{'messages': [{'id': 'b'}]}]}),
        ('/messages/b', message('b'))]
    native = NativeHTTP(responses)
    counters = {'provider_requests': 0, 'records_observed': 0, 'capture_profile_requests': 0,
                'blob_bytes_written': 0}
    async def handle(request):
        counters['provider_requests'] += 1
        if request.url.path.endswith('/profile'):
            counters['capture_profile_requests'] += 1
        return await native.handle(request)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    connector = await GmailSource.create(auth=StaticTokenProvider('synthetic'),
        logger=MagicMock(), http_client=client, config=GmailConfig(
            included_labels=[], excluded_labels=[], excluded_categories=[]))
    ctx, _, runtime, bus = components(sessions, (service, fence))
    attempt = CaptureAttempt(id=fence.attempt_id if mode == 'interrupt' else uuid4(),
                             number=1 if mode == 'interrupt' else 2)
    probe = GmailResumeProbe(sessions, fence.organization_id, fence.sync_id, fence.job_id,
        attempt.id, Path(os.environ['TEST_TARGET']), mode, counters)
    wrapper = bounded_page_source(connector, counters, 250, probe)
    pipeline = CanonicalCapturePipeline(service, sessions, bus, connector.canonical_record_types,
        attempt, connector.canonical_container_parents, page_source=wrapper, files=MagicMock())
    runtime.source, runtime.canonical_capture = connector, pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    await run(instance)
asyncio.run(main())
"""


async def test_gmail_process_loss_resumes_saved_plan_without_profile(database, source, tmp_path):
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
    assert first["intentional_interruption"] and first["records_observed"] == 1
    assert first["capture_profile_requests"] == 1
    code, output, error = await subprocess_script(SCRIPT, {**env, "TEST_MODE": "resume"})
    assert code == 76, error
    second = json.loads(output.splitlines()[-1])
    assert second["gmail_recovery_verified"] and second["resumed_saved_page"]
    assert second["capture_profile_requests"] == 0 and second["records_observed"] == 1
    assert not second["full_scope_completed"] and not second["checkpoint_saved"]
    assert (tmp_path / "target.json").stat().st_mode & 0o077 == 0


async def test_gmail_harness_budgets_capabilities_and_early_completion(tmp_path):
    script = r"""
import asyncio, json, os, sys
from types import SimpleNamespace
sys.path.insert(0, 'tests/live')
import conftest
import httpx
import provider_lifecycle as lifecycle
from airweave.domains.entities.canonical.page_source import (
    CheckpointedPageSource, KnownObjectSource)

class Plain:
    canonical_record_types = ('message',)
    canonical_container_parents = {}
    capture_cycle_configuration = None

class Known(Plain):
    async def refresh_known(self, record, *, files):
        return record

class Planned(Known):
    async def prepare_cycle(self, previous):
        return previous
    def initial_continuation(self, cycle):
        return cycle

async def main():
    counters = {'records_observed': 0}
    for source, known, planned in [(Plain(), False, False), (Known(), True, False),
                                   (Planned(), True, True)]:
        wrapper = lifecycle.bounded_page_source(source, counters, 1)
        assert isinstance(wrapper, CheckpointedPageSource) == planned
        assert isinstance(wrapper, KnownObjectSource) == known
    assert await wrapper.prepare_cycle('plan') == 'plan'
    assert wrapper.initial_continuation('saved') == 'saved'
    assert await wrapper.refresh_known('record', files=None) == 'record'
    try:
        await wrapper.refresh_known('record', files=None)
    except lifecycle.BudgetExceeded:
        pass
    else:
        raise AssertionError('Exact reads bypassed record budget')
    counters = {'identity_profile_requests': 0, 'capture_profile_requests': 0,
                'resumed_changes_requests': 0}
    request = httpx.Request('GET', 'https://gmail.googleapis.com/gmail/v1/users/me/profile')
    lifecycle.count_gmail_request('gmail', request, {}, counters, False)
    lifecycle.count_gmail_request('gmail', request, {}, counters, True)
    assert counters['identity_profile_requests'] == counters['capture_profile_requests'] == 1
    for early in (False, True):
        snapshots = []
        async def execute(path, timeout):
            snapshots.append(json.loads(path.read_text()))
            if early:
                return 0, {'failed': False, 'full_scope_completed': True}
            second = len(snapshots) == 2
            return (76 if second else 75), {'failed': False, 'intentional_interruption': not second,
                'gmail_recovery_verified': second, 'provider_requests': 7,
                'records_observed': 50, 'blob_bytes_written': 20}
        lifecycle.execute_trial = execute
        results = await lifecycle.run_trials({'provider': 'gmail', 'gmail_resume': True,
            'root': os.environ['TEST_ROOT'], 'request_limit': 600, 'record_limit': 250,
            'blob_byte_limit': 100, 'timeout': 600})
        if early:
            assert len(snapshots) == 1 and results[0]['recovery_not_exercised']
        else:
            assert len(snapshots) == 2
            assert [s['request_limit'] for s in snapshots] == [600, 593]
            assert [s['record_limit'] for s in snapshots] == [250, 200]
            assert [s['blob_byte_limit'] for s in snapshots] == [100, 80]
            assert snapshots[0]['job_id'] == snapshots[1]['job_id']
            assert [s['attempt_number'] for s in snapshots] == [1, 2]
    print('verified')
asyncio.run(main())
"""
    code, output, error = await subprocess_script(script, {"TEST_ROOT": str(tmp_path)})
    assert code == 0, error
    assert output.splitlines()[-1] == "verified"


async def test_actual_lifecycle_child_context_reaches_committed_gmail_page(
    database, source, tmp_path
):
    """Exercise the real child composition, not the separate pipeline-test context."""
    script = r"""
import asyncio, json, os, sys
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
sys.path.insert(0, 'tests/live')
import conftest
import httpx
import provider_lifecycle as lifecycle
from provider_sample import verify_rest_identity
from airweave.domains.entities.canonical.tests.test_gmail_recovery import NativeHTTP
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import GmailConfig
from airweave.platform.sources.gmail import GmailSource
from airweave.platform.sources.tests.test_gmail_capture import message

@asynccontextmanager
async def synthetic_source(name, account, expected_email, key, fence, **options):
    assert name == 'gmail' and options['gmail_unfiltered'] is True
    native = NativeHTTP([
        ('/profile', {'emailAddress': expected_email, 'historyId': 'identity-only'}),
        ('/profile', {'historyId': 'capture-boundary'}),
        ('/messages', {'messages': [{'id': 'synthetic-a'}], 'nextPageToken': 'next'}),
        ('/messages/synthetic-a', message('synthetic-a')),
    ])
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handle),
            event_hooks={'request': [options['request_hook']]}) as client:
        connector = await GmailSource.create(auth=StaticTokenProvider('synthetic'),
            logger=MagicMock(), http_client=client, config=GmailConfig(
                included_labels=[], excluded_labels=[], excluded_categories=[]))
        await verify_rest_identity(name, connector, expected_email)
        yield connector, 'provider_email'

lifecycle.rest_source = synthetic_source
# All provider traffic uses the scripted MockTransport; synthetic credentials only.
os.environ['COMPOSIO_API_KEY'] = 'synthetic-test-only'
os.environ['LIVE_GMAIL_ACCOUNT_ID'] = 'synthetic-test-only'
os.environ['LIVE_EXPECTED_EMAIL'] = 'synthetic@example.test'
raise SystemExit(asyncio.run(lifecycle.child(json.loads(os.environ['TEST_MANIFEST']))))
"""
    from sqlalchemy import select

    from airweave.models.capture_scan import CaptureScan
    from airweave.models.entity import Entity
    from airweave.models.sync_cursor import SyncCursor

    fence = source[1]
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    manifest = {
        "provider": "gmail",
        "schema": schema,
        "root": str(tmp_path),
        "organization_id": str(fence.organization_id),
        "sync_id": str(fence.sync_id),
        "job_id": str(fence.job_id),
        "attempt_number": 2,
        "gmail_resume": True,
        "gmail_unfiltered": True,
        "resume_stage": "interrupt",
        "record_limit": 250,
        "request_limit": 600,
        "timeout": 30,
        "file_byte_limit": 10 * 1024 * 1024,
        "blob_byte_limit": 256 * 1024 * 1024,
    }
    code, output, error = await subprocess_script(script, {"TEST_MANIFEST": json.dumps(manifest)})
    assert code == 75, (output, error)
    result = json.loads(output.splitlines()[-1])
    assert result["intentional_interruption"] and not result["failed"]
    assert result["identity_profile_requests"] == result["capture_profile_requests"] == 1
    assert result["provider_requests"] == 4 and result["records_observed"] == 1
    assert not result["checkpoint_saved"] and not result["full_scope_completed"]
    async with database() as db:
        record = await db.scalar(select(Entity).where(Entity.native_id == "synthetic-a"))
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.sync_id == fence.sync_id))
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == fence.sync_id))
    assert record is not None and record.record_revision == 1
    assert scan.phase == "collecting" and scan.continuation["phase"] == "history"
    assert cursor.cursor_data["canonical_cycle"]["phase"] == "active"
    assert "canonical_checkpoint" not in cursor.cursor_data
