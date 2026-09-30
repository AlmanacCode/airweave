"""Opt-in Gmail, selected Calendar, or full accessible Drive lifecycle proof.

Two fresh processes; no Temporal worker, hosted factory, auth or product binding claim.
Parent owns private schema/files cleanup on success and failure. No provider writes.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
from contextlib import aclosing, asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import canonical_capture as harness  # settings must precede application imports
from provider_sample import rest_source, wispr_source
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.adapters.event_bus.fake import FakeEventBus
from airweave.domains.entities.canonical.page_source import CanonicalPageSource
from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope, StartedScope
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.domains.sync_pipeline.config import SyncConfig
from airweave.domains.sync_pipeline.contexts.runtime import SyncRuntime
from airweave.domains.sync_pipeline.orchestrator import SyncOrchestrator
from airweave.domains.sync_pipeline.pipeline.entity_tracker import EntityTracker
from airweave.domains.sync_pipeline.stream import AsyncSourceStream
from airweave.domains.sync_pipeline.worker_pool import AsyncWorkerPool
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.domains.syncs.cursors.service import SyncCursorService
from airweave.domains.syncs.jobs.repository import SyncJobRepository
from airweave.domains.syncs.jobs.state_machine import SyncJobStateMachine
from airweave.models import Entity, Organization, Sync, SyncJob
from airweave.models import SyncCursor as StoredCursor
from airweave.platform.configs.config import GoogleCalendarConfig
from airweave.platform.cursors.gmail import GmailCursor
from airweave.platform.cursors.google_calendar import GoogleCalendarCursor
from airweave.platform.cursors.google_drive import GoogleDriveCursor
from airweave.platform.http_client.composio_transport import ComposioProxyError
from airweave.platform.sources.slack import SlackApiError


class BudgetExceeded(RuntimeError):
    """An explicit live-test bound was exceeded; source must not finish."""


class BoundedStorage(harness.FilesystemBackend):
    """Cap all writes, even repeated MIME reads, before retaining another blob."""

    def __init__(self, root, byte_limit):
        super().__init__(root)
        self.written_bytes = 0
        self.byte_limit = byte_limit

    async def write_file(self, path, content):
        if self.written_bytes + len(content) > self.byte_limit:
            raise BudgetExceeded("blob_bytes")
        await super().write_file(path, content)
        self.written_bytes += len(content)


class BoundedPageSource:
    """Test budget around the real page adapter; production driver still owns all progress."""

    def __init__(self, source, counters, record_limit):
        self.source, self.counters, self.record_limit = source, counters, record_limit
        self.canonical_record_types = source.canonical_record_types
        self.canonical_container_parents = source.canonical_container_parents
        self.capture_cycle_configuration = source.capture_cycle_configuration

    async def capture_page(self, scope, continuation):
        page = await self.source.capture_page(scope, continuation)
        self.counters["records_observed"] += len(page.records)
        if self.counters["records_observed"] > self.record_limit:
            raise BudgetExceeded("records")
        return page

    async def confirm_root_absent(self, native_id):
        await self.source.confirm_root_absent(native_id)


async def bounded_observations(source, cursor, files, counters, record_limit):
    """A breached test budget fails enumeration before scope completion."""
    async with aclosing(source.generate_observations(cursor=cursor, files=files)) as stream:
        async for item in stream:
            if isinstance(item, CaptureRecord):
                counters["records_observed"] += 1
                if counters["records_observed"] > record_limit:
                    raise BudgetExceeded("records")
            elif isinstance(item, StartedScope):
                counters["started"] += 1
            elif isinstance(item, CompletedScope):
                counters["completed"] += 1
            yield item


def safe_failure_reason(error):
    """Only local fixed diagnostics; never emit provider payloads, URLs or IDs."""
    text = str(error)
    if isinstance(error, BudgetExceeded) and text in {
        "records",
        "provider_requests",
        "blob_bytes",
        "rate_limit",
    }:
        return text
    if type(error) is ValueError and text in {
        "Wispr tool execution failed; capture is incomplete",
        "Wispr returned incomplete or repeated pagination",
        "Wispr query cap cannot be partitioned safely",
        "Wispr date partition made no progress",
        "Wispr returned a meeting without identity",
        "Wispr meeting text is not a string",
        "Wispr returned a different meeting identity",
        "Wispr meeting changed during paginated capture",
        "Wispr continuation made no progress",
        "Wispr meeting exceeds the bounded capture page limit",
    }:
        return text
    if isinstance(error, SlackApiError) and error.code in {
        "missing_scope",
        "not_in_channel",
        "channel_not_found",
        "is_archived",
        "method_not_supported_for_channel_type",
        "not_allowed_token_type",
    }:
        return f"Slack request failed: {error.code}"
    if isinstance(error, ComposioProxyError) and (
        text
        in {
            "Proxy download exceeds the file size limit",
            "Composio response exceeds the file size limit",
            "Invalid Composio response envelope",
            "Temporary download request failed",
        }
        or re.fullmatch(r"(?:Composio proxy|Temporary download) returned HTTP [0-9]{3}", text)
    ):
        return text
    return None


def validate_checkpoint(name, manifest, counters, saved, previous, loaded, attempt, sequence):
    """Match production cursor/scope capabilities; never manufacture a resume claim."""
    if name == "google_calendar":
        calendar_id = manifest["calendar_config"]["calendar_ids"][0]
        assert set(saved["calendar_tokens"]) == {calendar_id}
        assert saved["calendar_tokens"][calendar_id]
        assert set(saved["occurrence_coverage"]) == {calendar_id}
        expected_scopes = 2 if loaded else 3
        assert counters["started"] == counters["completed"] == expected_scopes
        assert bool(counters["sync_token_requests"]) == loaded
    elif name == "google_drive":
        assert saved["canonical_page_token"]
        assert counters["started"] == counters["completed"] == (0 if loaded else 1)
        assert bool(counters["resumed_changes_requests"]) == loaded
    elif name == "slack":
        assert saved["canonical_cycle"]["phase"] == "complete"
        assert saved["canonical_cycle"]["completed_job_id"]
    elif name == "wispr":
        assert saved is None
        assert counters["started"] == counters["completed"] == 0
        return
    else:
        assert saved["canonical_query"] == manifest["query"]
        assert saved["history_id"] == "" and counters["started"] == counters["completed"] == 1
    assert saved["canonical_checkpoint"] == {
        "writer_attempt_id": str(attempt.id),
        "observed_change_sequence": sequence,
    }
    assert saved != previous


async def child(manifest):
    engine = create_async_engine(
        harness.test_database_url(),
        connect_args={"server_settings": {"search_path": manifest["schema"]}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    organization_id, sync_id = UUID(manifest["organization_id"]), UUID(manifest["sync_id"])
    job_id = uuid4()
    counters = {"provider_requests": 0, "records_observed": 0, "started": 0, "completed": 0}
    result = {"failed": True}
    previous = {}
    name = manifest["provider"]
    is_calendar = name == "google_calendar"
    counters["sync_token_requests"] = 0
    counters["resumed_changes_requests"] = 0
    last_operation = "identity"
    rate_limits = []

    async def observe_slack_rate_limit(response):
        await response.aread()
        limited = response.status_code == 429 or response.json().get("error") in {
            "ratelimited",
            "rate_limited",
        }
        if limited:
            from airweave.platform.sources.http_helpers import _parse_retry_after

            rate_limits.append(
                {
                    "status": response.status_code,
                    "retry_after_seconds": _parse_retry_after(response, default=60),
                    "request_number": counters["provider_requests"],
                }
            )

    async def request_hook(request):
        nonlocal last_operation
        last_operation = (
            "export"
            if request.url.path.endswith("/export")
            else ("download" if request.url.params.get("alt") == "media" else "metadata")
        )
        if counters["provider_requests"] >= manifest["request_limit"]:
            raise BudgetExceeded("provider_requests")
        counters["provider_requests"] += 1
        if request.url.params.get("syncToken"):
            counters["sync_token_requests"] += 1
        if (
            request.url.path.endswith("/changes")
            and previous.get("canonical_page_token")
            and request.url.params.get("pageToken") == previous["canonical_page_token"]
        ):
            counters["resumed_changes_requests"] += 1

    @asynccontextmanager
    async def db_context():
        async with sessions() as db:
            yield db

    try:
        async with sessions() as db:
            organization = await db.get(Organization, organization_id)
            sync = await db.get(Sync, sync_id)
            job = SyncJob(
                id=job_id, organization_id=organization_id, sync_id=sync_id, status="pending"
            )
            db.add(job)
            await db.commit()
        config = SyncConfig()
        config.behavior.skip_guardrails = True
        logger = logging.getLogger("private-lifecycle")
        ctx = SimpleNamespace(
            organization_id=organization_id,
            sync_id=sync_id,
            sync_job_id=job_id,
            organization=organization,
            sync=sync,
            sync_job=job,
            logger=logger,
            collection_id=uuid4(),
            source_connection_id=uuid4(),
            source_short_name=name,
            connection=SimpleNamespace(id=uuid4(), short_name=name),
            execution_config=config,
            batch_size=10,
            max_batch_latency_ms=20,
            should_batch=True,
            has_user_context=False,
            tracking_email=None,
        )
        cursor_service = SyncCursorService()
        async with sessions() as db:
            previous = await cursor_service.get_cursor_data(db, sync_id, ctx)
        cursor_schema = {
            "gmail": GmailCursor,
            "google_calendar": GoogleCalendarCursor,
            "google_drive": GoogleDriveCursor,
        }.get(name)
        cursor = SyncCursor(sync_id, cursor_schema, previous or None) if cursor_schema else None
        loaded = cursor.loaded_from_db if cursor else False
        storage = BoundedStorage(Path(manifest["root"]) / "blobs", manifest["blob_byte_limit"])
        files = FileService(job_id, storage, sync_id=sync_id)
        files.MAX_FILE_SIZE_BYTES = manifest["file_byte_limit"]
        fence = SimpleNamespace(organization_id=organization_id, sync_id=sync_id, job_id=job_id)
        key = os.environ["COMPOSIO_API_KEY"]
        account_variable = {
            "gmail": "LIVE_GMAIL_ACCOUNT_ID",
            "google_calendar": "LIVE_CALENDAR_ACCOUNT_ID",
            "google_drive": "LIVE_DRIVE_ACCOUNT_ID",
            "slack": "LIVE_SLACK_ACCOUNT_ID",
            "wispr": "LIVE_WISPR_ACCOUNT_ID",
        }[name]
        connection = (
            wispr_source(os.environ[account_variable], key, fence, request_hook=request_hook)
            if name == "wispr"
            else rest_source(
                name,
                os.environ[account_variable],
                os.environ["LIVE_EXPECTED_EMAIL"],
                key,
                fence,
                gmail_query=manifest.get("query", ""),
                calendar_config=GoogleCalendarConfig.model_validate(manifest["calendar_config"])
                if is_calendar
                else None,
                request_hook=request_hook,
                response_hook=observe_slack_rate_limit if name == "slack" else None,
                max_file_bytes=manifest["file_byte_limit"],
            )
        )
        async with (
            asyncio.timeout(manifest["timeout"]),
            connection as (source, identity_verification),
        ):
            assert source.cursor_class == cursor_schema
            bus = FakeEventBus()
            attempt = CaptureAttempt(id=uuid4(), number=1)
            page_source = (
                BoundedPageSource(source, counters, manifest["record_limit"])
                if isinstance(source, CanonicalPageSource)
                else None
            )
            pipeline = CanonicalCapturePipeline(
                CanonicalCaptureService(CanonicalRecordStore()),
                sessions,
                bus,
                source.canonical_record_types,
                attempt,
                container_parents=getattr(source, "canonical_container_parents", {}),
                page_source=page_source,
            )
            runtime = SyncRuntime(
                source=source,
                entity_tracker=EntityTracker(job_id, sync_id, logger),
                cursor=cursor,
                canonical_capture=pipeline,
            )
            runner = SyncOrchestrator(
                entity_pipeline=pipeline,
                worker_pool=AsyncWorkerPool(logger=logger),
                stream=(
                    None
                    if page_source is not None
                    else AsyncSourceStream(
                        bounded_observations(
                            source, cursor, files, counters, manifest["record_limit"]
                        ),
                        logger=logger,
                    )
                ),
                sync_context=ctx,
                runtime=runtime,
                access_control_pipeline=AsyncMock(),
                event_bus=bus,
                usage_checker=AsyncMock(),
                usage_ledger=AsyncMock(),
                sync_cursor_service=cursor_service,
                state_machine=SyncJobStateMachine(SyncJobRepository(), bus),
                lifecycle_data=None,
                sync_state_machine=AsyncMock(),
            )
            # Only connection ownership and external telemetry are replaced; state
            # transitions, capture, reconciliation and cursor writes are production code.
            with (
                patch("airweave.domains.syncs.jobs.state_machine.get_db_context", db_context),
                patch("airweave.domains.sync_pipeline.orchestrator.business_events"),
            ):
                await runner.run()
        await engine.dispose()
        async with sessions() as db:
            saved = await db.scalar(
                select(StoredCursor.cursor_data).where(StoredCursor.sync_id == sync_id)
            )
            status = await db.scalar(select(SyncJob.status).where(SyncJob.id == job_id))
            sequence = await db.scalar(
                select(Sync.observed_change_sequence).where(Sync.id == sync_id)
            )
            rows = list((await db.scalars(select(Entity).where(Entity.sync_id == sync_id))).all())
        assert status == "completed"
        validate_checkpoint(name, manifest, counters, saved, previous, loaded, attempt, sequence)
        visible = [r for r in rows if r.deleted_at is None and r.source_payload is not None]
        digest = hashlib.sha256(
            json.dumps(
                sorted(
                    (
                        r.entity_definition_short_name,
                        r.container_id or "",
                        r.native_id,
                        r.record_revision,
                        r.source_payload,
                    )
                    for r in visible
                ),
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()
        consumer_result = None
        if is_calendar and loaded and os.environ.get("LIVE_VERIFY_CALENDAR_CONSUMER") == "1":
            from almanac_handoff import verify_calendar_reader

            consumer_result = await verify_calendar_reader(
                sessions=sessions,
                organization_id=organization_id,
                sync_id=sync_id,
                root=Path(manifest["root"]),
                storage=storage,
                window=manifest["calendar_config"]["occurrence_window"],
                calendar_id=manifest["calendar_config"]["calendar_ids"][0],
            )
        result = {
            "failed": False,
            "provider": name,
            "identity_verification": identity_verification,
            "almanac_consumer": consumer_result,
            **counters,
            "rate_limits": rate_limits,
            "stored_records": len(visible),
            "partial_records": sum(r.completeness != "complete" for r in visible),
            "blob_bytes_written": storage.written_bytes,
            "checkpoint_saved": saved is not None,
            "observed_change_sequence": sequence,
            "loaded_durable_checkpoint": loaded,
            "job_status": status,
            "payload_revision_digest": digest,
            "mode": {
                "gmail": "filtered_full_reconciliation",
                "google_calendar": "calendar_incremental" if loaded else "calendar_initial",
                "google_drive": "drive_incremental" if loaded else "drive_initial",
                "slack": "accessible_history_full_reconciliation",
                "wispr": "exposed_meeting_enumeration_no_deletion_guarantee",
            }[name],
            "all_day_occurrences": sum(
                r.entity_definition_short_name == "event_occurrence"
                and "date" in r.source_payload.get("start", {})
                for r in visible
            ),
        }
    except Exception as error:
        async with sessions() as db:
            saved = await db.scalar(
                select(StoredCursor.cursor_data).where(StoredCursor.sync_id == sync_id)
            )
            status = await db.scalar(select(SyncJob.status).where(SyncJob.id == job_id))
        result = {
            "failed": True,
            "error_type": type(error).__name__,
            "safe_reason": safe_failure_reason(error),
            "last_operation": last_operation,
            **counters,
            "rate_limits": rate_limits,
            "checkpoint_unchanged": saved == (previous or None),
            "job_status": status,
        }
    finally:
        await engine.dispose()
    print(json.dumps(result), flush=True)
    return 1 if result["failed"] else 0


def check_free_disk(provider):
    """Keep the explicitly approved local-disk reserve before each Drive process."""
    available = shutil.disk_usage(gettempdir()).free
    if provider == "google_drive" and available < 2 * 1024**3:
        raise BudgetExceeded("free_disk")
    return available


async def main():
    name = os.environ.get("LIVE_LIFECYCLE_PROVIDER", "gmail")
    if name not in {"gmail", "google_calendar", "google_drive", "slack", "wispr"}:
        raise ValueError("Unsupported lifecycle provider")
    free_disk_bytes = check_free_disk(name)
    url = harness.test_database_url()
    schema = "canonical_live_" + uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    created = False
    old_umask = os.umask(0o077)
    results = []
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        created = True
        async with engine.begin() as connection:
            for migration in (
                "0000_baseline.py",
                "0001_canonical_records.py",
                "0002_projection_publication.py",
                "0003_mail_thread_index.py",
                "0004_projection_generation.py",
                "0005_capture_scan.py",
            ):
                await connection.run_sync(harness.migrate, migration)
        organization_id, sync_id = uuid4(), uuid4()
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as db:
            db.add(Organization(id=organization_id, name="Private provider lifecycle proof"))
            await db.flush()
            db.add(
                Sync(
                    id=sync_id,
                    organization_id=organization_id,
                    name="Fixed selected provider scope",
                )
            )
            await db.commit()
        with TemporaryDirectory(prefix="airweave-private-live-") as directory:
            root = Path(directory)
            end = int(time.time())
            manifest = {
                "provider": name,
                "request_limit": {
                    "gmail": 600,
                    "google_calendar": 100,
                    "google_drive": 250,
                    "slack": 600,
                    "wispr": 150,
                }[name],
                "record_limit": {
                    "gmail": 250,
                    "google_calendar": 10000,
                    "google_drive": 2000,
                    "slack": 10000,
                    "wispr": 1000,
                }[name],
                "timeout": {
                    "gmail": 600,
                    "google_calendar": 180,
                    "google_drive": 300,
                    "slack": 1800,
                    "wispr": 300,
                }[name],
                "blob_byte_limit": (512 if name == "google_drive" else 256) * 1024 * 1024,
                "file_byte_limit": FileService.MAX_FILE_SIZE_BYTES
                if name == "google_drive"
                else 10 * 1024 * 1024,
                "schema": schema,
                "root": str(root),
                "organization_id": str(organization_id),
                "sync_id": str(sync_id),
                "query": f"after:{end - 7 * 86400} before:{end}",
            }
            if name == "google_calendar":
                now = datetime.now(timezone.utc).replace(microsecond=0)
                manifest["calendar_config"] = {
                    "calendar_ids": [os.environ["LIVE_EXPECTED_EMAIL"]],
                    "occurrence_window": {
                        "start": now.isoformat(),
                        "end": (now + timedelta(days=7)).isoformat(),
                    },
                }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            for _ in range(2):
                check_free_disk(name)
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(Path(__file__).absolute()),
                    str(path),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, _ = await asyncio.wait_for(
                        process.communicate(), timeout=manifest["timeout"] + 60
                    )
                finally:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
                lines = [
                    json.loads(line)
                    for line in stdout.decode().splitlines()
                    if line.startswith("{")
                ]
                if not lines:
                    raise RuntimeError("Child failed without sanitized result")
                results.append(lines[-1])
                print(json.dumps({"run": len(results), **lines[-1]}), flush=True)
                if process.returncode:
                    break
    finally:
        os.umask(old_umask)
        await engine.dispose()
        if created:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
    print(
        json.dumps(
            {
                "runs": len(results),
                "free_disk_bytes_before": free_disk_bytes,
                "schema_removed": True,
                "blob_directory_removed": True,
                "identical_second_read": len(results) == 2
                and results[0].get("payload_revision_digest")
                == results[1].get("payload_revision_digest"),
            }
        )
    )
    return 0 if len(results) == 2 and not any(r["failed"] for r in results) else 1


if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            manifest = json.loads(Path(sys.argv[1]).read_text())
            harness.StoragePaths.TEMP_PROCESSING = str(Path(manifest["root"]) / "downloads")
            code = asyncio.run(child(manifest))
        else:
            code = asyncio.run(main())
    except Exception as error:
        print(json.dumps({"failed": True, "error_type": type(error).__name__}), flush=True)
        code = 1
    raise SystemExit(code)
