"""Actual lifecycle children verify the Drive page checkpoint contract offline."""

import json

from sqlalchemy import text

from airweave.domains.entities.canonical.tests.test_resume_probe import subprocess_script

SCRIPT = r"""
import asyncio, json, os, sys
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
sys.path.insert(0, 'tests/live')
import conftest
import httpx
import provider_lifecycle as lifecycle
from provider_sample import verify_rest_identity
from airweave.domains.entities.canonical.tests.test_drive_recovery import NativeHTTP, folder
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import GoogleDriveConfig
from airweave.platform.sources.google_drive import GoogleDriveSource

@asynccontextmanager
async def synthetic_source(name, account, expected_email, key, fence, **options):
    assert name == 'google_drive'
    responses = []
    if os.environ['TEST_STAGE'] == 'first':
        responses += [('/changes/startPageToken', {'startPageToken': 'before'}),
            ('/files', {'files': [{'id': 'folder-a'}]}),
            ('/files/folder-a', folder('folder-a')),
            ('/changes', {'changes': [], 'newStartPageToken': 'after'})]
    else:
        responses += [('/changes', {'changes': [], 'newStartPageToken': 'later'})]
    native = NativeHTTP(responses)
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handle),
            event_hooks={'request': [options['request_hook']]}) as client:
        connector = await GoogleDriveSource.create(auth=StaticTokenProvider('synthetic'),
            logger=MagicMock(), http_client=client,
            config=GoogleDriveConfig(expected_permission_id="principal-a"))
        yield connector, 'provider_email'
        assert not native.replies

lifecycle.rest_source = synthetic_source
# All provider I/O is synthetic; reuse the disposable fixture (TCP in CI).
# The real account harness retains its private Unix-socket requirement.
lifecycle.harness.test_database_url = lambda: os.environ['CANONICAL_TEST_DATABASE_URL']
os.environ['COMPOSIO_API_KEY'] = 'synthetic-test-only'
os.environ['LIVE_DRIVE_ACCOUNT_ID'] = 'synthetic-test-only'
os.environ['LIVE_EXPECTED_EMAIL'] = 'synthetic@example.test'
raise SystemExit(asyncio.run(lifecycle.child(json.loads(os.environ['TEST_MANIFEST']))))
"""


async def test_drive_lifecycle_initial_then_fresh_process_changes(database, source, tmp_path):
    fence = source[1]
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    manifest = {
        "provider": "google_drive",
        "schema": schema,
        "root": str(tmp_path),
        "organization_id": str(fence.organization_id),
        "sync_id": str(fence.sync_id),
        "record_limit": 10,
        "request_limit": 20,
        "timeout": 30,
        "file_byte_limit": 1024,
        "blob_byte_limit": 1024,
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
        assert result["stored_records"] == 1 and result["partial_records"] == 0
        results.append(result)
    first, second = results
    assert first["provider_requests"] == 5 and second["provider_requests"] == 2
    assert first["records_observed"] == 1 and second["records_observed"] == 0
    assert not first["loaded_durable_checkpoint"] and second["loaded_durable_checkpoint"]
    assert first["resumed_changes_requests"] == 0 and second["resumed_changes_requests"] == 1
    assert first["observed_change_sequence"] == second["observed_change_sequence"]
    assert first["payload_revision_digest"] == second["payload_revision_digest"]


async def test_checkpoint_validation_distinguishes_full_resume_from_changes():
    code, output, error = await subprocess_script(
        r"""
import sys
from types import SimpleNamespace
sys.path.insert(0, 'tests/live')
import provider_lifecycle as lifecycle
attempt = SimpleNamespace(id="attempt")
for prior_phase, prior_mode, final_mode, changes_requests in (
    ("active", "full", "full", 0),
    ("complete", "full", "changes", 1),
    ("active", "changes", "changes", 1),
):
    previous = {"canonical_cycle": {"phase": prior_phase, "mode": prior_mode}}
    saved = {
        "canonical_cycle": {"phase": "complete", "mode": final_mode,
            "completed_job_id": "job", "last_full_capture": {"cycle_id": "full"},
            "promoted_checkpoint": {"checkpoint": {"value": {"page_token": "next"}}}},
        "canonical_checkpoint": {"writer_attempt_id": "attempt", "observed_change_sequence": 4},
    }
    counters = {"started": 0, "completed": 0, "resumed_changes_requests": changes_requests}
    lifecycle.validate_checkpoint("google_drive", {}, counters, saved, previous, True, attempt, 4)
# Loading a prior cursor must not excuse a missing promotion after completion.
saved["canonical_cycle"]["promoted_checkpoint"] = None
try:
    lifecycle.validate_checkpoint("google_drive", {}, counters, saved, previous, True, attempt, 4)
except AssertionError:
    pass
else:
    raise AssertionError("Missing checkpoint promotion was accepted")
print("checkpoint modes verified")
""",
        {},
    )
    assert code == 0, output + error
    assert "checkpoint modes verified" in output
