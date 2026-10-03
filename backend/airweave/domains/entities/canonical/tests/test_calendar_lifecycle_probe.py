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
assert not lifecycle.calendar_delta_expected(None)
assert not lifecycle.calendar_delta_expected({'canonical_cycle': {'phase': 'collecting'}})
assert lifecycle.calendar_delta_expected({'canonical_cycle': {'phase': 'complete'}})
from airweave.domains.entities.canonical.tests.test_calendar_recovery import NativeHTTP, event
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.sources.google_calendar import GoogleCalendarSource

@asynccontextmanager
async def synthetic_source(name, account, expected_email, key, fence, **options):
    assert name == 'google_calendar'
    path = '/calendars/' + quote(expected_email, safe='') + '/events'
    first = os.environ['TEST_STAGE'] == 'first'
    native = NativeHTTP([
        ('/users/me/calendarList', {'items': [{'id': expected_email, 'timeZone': 'UTC'}]}),
        (path, {'items': [event('a')] if first else [],
                'nextSyncToken': 'full' if first else 'delta'}),
        (path, {'items': [event('a')]}),
    ], principal=expected_email)
    all_calendars = os.environ.get('TEST_ALL') == '1'
    calls = []
    async def all_handle(request):
        if request.url.path.endswith('/calendars/primary'):
            return httpx.Response(200, json={'id': expected_email})
        calls.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith('/calendarList'):
            return httpx.Response(200, json={'items': [
                {'id': expected_email, 'timeZone': 'UTC'},
                {'id': 'second@example.test', 'timeZone': 'America/Los_Angeles'}]})
        assert request.url.path.removeprefix('/calendar/v3') in [
            '/calendars/' + expected_email + '/events',
            '/calendars/second@example.test/events',
        ]
        result = {'items': [event('same-native-id')]}
        if request.url.params['singleEvents'] == 'false':
            result['nextSyncToken'] = 'full-' + request.url.path
        else:
            from datetime import datetime, timedelta
            start = datetime.fromisoformat(request.url.params['timeMin'])
            end = datetime.fromisoformat(request.url.params['timeMax'])
            assert end - start == timedelta(days=120)
        return httpx.Response(200, json=result)
    transport = httpx.MockTransport(all_handle if all_calendars else native.handle)
    async with httpx.AsyncClient(transport=transport,
            event_hooks={'request': [options['request_hook']]}) as client:
        connector = await GoogleCalendarSource.create(auth=StaticTokenProvider('synthetic'),
            logger=MagicMock(), http_client=client, config=options['calendar_config'].model_copy(
                update={'expected_primary_calendar_id': expected_email}))
        yield connector, 'provider_primary_calendar_id'
        if all_calendars:
            assert len(calls) == 5
            assert sum(params.get('singleEvents') == 'true' for _, params in calls) == 2
        else:
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


async def test_calendar_lifecycle_all_members_with_default_rolling_window(
    database, source, tmp_path
):
    """None selection and an unset window qualify both native calendar scope pairs."""
    fence = source[1]
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    manifest = {
        "provider": "google_calendar",
        "schema": schema,
        "root": str(tmp_path),
        "organization_id": str(fence.organization_id),
        "sync_id": str(fence.sync_id),
        "job_id": str(fence.job_id),
        "attempt_number": 2,
        "record_limit": 10,
        "request_limit": 20,
        "timeout": 30,
        "file_byte_limit": 1024,
        "blob_byte_limit": 1024,
        "calendar_config": {"calendar_ids": None, "occurrence_window": None},
    }
    code, output, error = await subprocess_script(
        SCRIPT, {"TEST_MANIFEST": json.dumps(manifest), "TEST_STAGE": "first", "TEST_ALL": "1"}
    )
    if code != 0:
        raise AssertionError(output + error)
    result = json.loads(output.splitlines()[-1])
    assert not result["failed"] and result["job_status"] == "completed"
    assert result["stored_records"] == 6 and result["records_observed"] == 6
    assert result["provider_requests"] == 6 and result["sync_token_requests"] == 0
    assert result["full_scope_completed"]
