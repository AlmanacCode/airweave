"""Opt-in fixed Gmail reconciliation or selected Calendar incremental lifecycle.

Two fresh processes; no Temporal worker, hosted factory, auth or product binding claim.
Parent owns private schema/files cleanup on success and failure. No provider writes.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from contextlib import aclosing, asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import canonical_capture as harness  # settings must precede application imports
from provider_sample import rest_source
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.adapters.event_bus.fake import FakeEventBus
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


class BudgetExceeded(RuntimeError):
    """An explicit live-test bound was exceeded; source must not finish."""


class BoundedStorage(harness.FilesystemBackend):
    """Cap all writes, even repeated MIME reads, before retaining another blob."""

    def __init__(self, root):
        super().__init__(root)
        self.written_bytes = 0

    async def write_file(self, path, content):
        if self.written_bytes + len(content) > 256 * 1024 * 1024:
            raise BudgetExceeded("blob_bytes")
        await super().write_file(path, content)
        self.written_bytes += len(content)


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

    async def request_hook(request):
        counters["provider_requests"] += 1
        if request.url.params.get("syncToken"):
            counters["sync_token_requests"] += 1
        if counters["provider_requests"] > manifest["request_limit"]:
            raise BudgetExceeded("provider_requests")

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
        cursor = SyncCursor(
            sync_id, GoogleCalendarCursor if is_calendar else GmailCursor, previous or None
        )
        loaded = cursor.loaded_from_db
        storage = BoundedStorage(Path(manifest["root"]) / "blobs")
        files = FileService(job_id, storage, sync_id=sync_id)
        files.MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
        fence = SimpleNamespace(organization_id=organization_id, sync_id=sync_id, job_id=job_id)
        async with (
            asyncio.timeout(manifest["timeout"]),
            rest_source(
                name,
                os.environ["LIVE_CALENDAR_ACCOUNT_ID" if is_calendar else "LIVE_GMAIL_ACCOUNT_ID"],
                os.environ["LIVE_EXPECTED_EMAIL"],
                os.environ["COMPOSIO_API_KEY"],
                fence,
                gmail_query=manifest.get("query", ""),
                calendar_config=GoogleCalendarConfig.model_validate(manifest["calendar_config"])
                if is_calendar
                else None,
                request_hook=request_hook,
            ) as (source, _),
        ):
            bus = FakeEventBus()
            attempt = CaptureAttempt(id=uuid4(), number=1)
            pipeline = CanonicalCapturePipeline(
                CanonicalCaptureService(CanonicalRecordStore()),
                sessions,
                bus,
                source.canonical_record_types,
                attempt,
                container_parents=getattr(source, "canonical_container_parents", {}),
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
                stream=AsyncSourceStream(
                    bounded_observations(source, cursor, files, counters, manifest["record_limit"]),
                    logger=logger,
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
        if is_calendar:
            calendar_id = manifest["calendar_config"]["calendar_ids"][0]
            assert set(saved["calendar_tokens"]) == {calendar_id}
            assert saved["calendar_tokens"][calendar_id]
            assert set(saved["occurrence_coverage"]) == {calendar_id}
            expected_scopes = 2 if loaded else 3
            assert counters["started"] == counters["completed"] == expected_scopes
            assert bool(counters["sync_token_requests"]) == loaded
        else:
            assert saved["canonical_query"] == manifest["query"]
            assert saved["history_id"] == "" and counters["started"] == counters["completed"] == 1
        assert saved["canonical_checkpoint"] == {
            "writer_attempt_id": str(attempt.id),
            "observed_change_sequence": sequence,
        }
        assert saved != previous
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
            "almanac_consumer": consumer_result,
            **counters,
            "stored_records": len(visible),
            "partial_records": sum(r.completeness != "complete" for r in visible),
            "blob_bytes_written": storage.written_bytes,
            "checkpoint_saved": True,
            "observed_change_sequence": sequence,
            "loaded_durable_checkpoint": loaded,
            "job_status": status,
            "payload_revision_digest": digest,
            "mode": ("calendar_incremental" if loaded else "calendar_initial")
            if is_calendar
            else "filtered_full_reconciliation",
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
            **counters,
            "checkpoint_unchanged": saved == (previous or None),
            "job_status": status,
        }
    finally:
        await engine.dispose()
    print(json.dumps(result), flush=True)
    return 1 if result["failed"] else 0


async def main():
    name = os.environ.get("LIVE_LIFECYCLE_PROVIDER", "gmail")
    if name not in {"gmail", "google_calendar"}:
        raise ValueError("Unsupported lifecycle provider")
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
                "request_limit": 100 if name == "google_calendar" else 600,
                "record_limit": 10000 if name == "google_calendar" else 250,
                "timeout": 180 if name == "google_calendar" else 600,
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
