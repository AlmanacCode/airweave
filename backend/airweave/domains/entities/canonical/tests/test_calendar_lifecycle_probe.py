"""The real live harness must preserve scoped capture through fresh child processes."""

import json

from sqlalchemy import text

from airweave.domains.entities.canonical.tests.test_resume_probe import subprocess_script

SCRIPT = r"""
import asyncio, json, os, sys
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
from urllib.parse import quote
sys.path.insert(0, 'tests/live')
import conftest
import httpx
import provider_lifecycle as lifecycle
from provider_sample import verify_rest_identity
from airweave.domains.entities.canonical.tests.test_calendar_recovery import NativeHTTP, event
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.sources.google_calendar import GoogleCalendarSource

@asynccontextmanager
async def synthetic_source(name, account, expected_email, key, fence, **options):
    assert name == 'google_calendar'
    path = '/calendars/' + quote(expected_email, safe='') + '/events'
    first = os.environ['TEST_STAGE'] == 'first'
    native = NativeHTTP([
        ('/users/me/calendarList/primary', {'id': expected_email, 'primary': True}),
        ('/users/me/calendarList', {'items': [{'id': expected_email, 'timeZone': 'UTC'}]}),
        (path, {'items': [event('a')] if first else [],
                'nextSyncToken': 'full' if first else 'delta'}),
        (path, {'items': [event('a')]}),
    ])
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handle),
            event_hooks={'request': [options['request_hook']]}) as client:
        connector = await GoogleCalendarSource.create(auth=StaticTokenProvider('synthetic'),
            logger=MagicMock(), http_client=client, config=options['calendar_config'])
        await verify_rest_identity(name, connector, expected_email)
        yield connector, 'provider_email'
        assert not native.replies

lifecycle.rest_source = synthetic_source
lifecycle.harness.test_database_url = lambda: os.environ['CANONICAL_TEST_DATABASE_URL']
os.environ['COMPOSIO_API_KEY'] = 'synthetic-test-only'
os.environ['LIVE_CALENDAR_ACCOUNT_ID'] = 'synthetic-test-only'
os.environ['LIVE_EXPECTED_EMAIL'] = 'synthetic@example.test'
raise SystemExit(asyncio.run(lifecycle.child(json.loads(os.environ['TEST_MANIFEST']))))
"""


async def test_calendar_lifecycle_initial_then_published_scope_delta(database, source, tmp_path):
    fence = source[1]
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    manifest = {
        "provider": "google_calendar",
        "schema": schema,
        "root": str(tmp_path),
        "organization_id": str(fence.organization_id),
        "sync_id": str(fence.sync_id),
        "record_limit": 10,
        "request_limit": 20,
        "timeout": 30,
        "file_byte_limit": 1024,
        "blob_byte_limit": 1024,
        "calendar_config": {
            "calendar_ids": ["synthetic@example.test"],
            "occurrence_window": {
                "start": "2026-03-01T00:00:00Z",
                "end": "2026-04-01T00:00:00Z",
            },
        },
    }
    results = []
    for stage in ("first", "second"):
        current = dict(manifest)
        if stage == "first":
            current.update(job_id=str(fence.job_id), attempt_number=2)
        code, output, error = await subprocess_script(
            SCRIPT, {"TEST_MANIFEST": json.dumps(current), "TEST_STAGE": stage}
        )
        if code != 0:
            raise AssertionError(output + error)
        result = json.loads(output.splitlines()[-1])
        assert not result["failed"] and result["job_status"] == "completed"
        assert result["stored_records"] == 3 and result["partial_records"] == 0
        assert result["provider_requests"] == 4
        results.append(result)
    first, second = results
    assert first["records_observed"] == 3 and second["records_observed"] == 2
    assert not first["loaded_durable_checkpoint"] and second["loaded_durable_checkpoint"]
    assert first["sync_token_requests"] == 0 and second["sync_token_requests"] == 1
    assert first["full_scope_completed"] and not second["full_scope_completed"]
    assert first["observed_change_sequence"] == second["observed_change_sequence"]
    assert first["payload_revision_digest"] == second["payload_revision_digest"]
