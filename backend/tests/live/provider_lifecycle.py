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
from calendar_lifecycle import count_calendar_request, event_checkpoint, verify_calendar_scopes
from capture_comparison import compare_observations
from provider_sample import rest_source, wispr_source
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.adapters.event_bus.fake import FakeEventBus
from airweave.domains.entities.canonical.page_source import (
    CanonicalPageSource,
    CheckpointedPageSource,
    InvalidCaptureCheckpoint,
    InvalidScanContinuation,
    KnownObjectSource,
    ScopedPageSource,
)
from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope, StartedScope
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore, source_record
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
from airweave.models import Entity, Organization, SourceConnection, Sync, SyncJob
from airweave.models import SyncCursor as StoredCursor
from airweave.platform.configs.config import GoogleCalendarConfig, SlackConfig
from airweave.platform.cursors.gmail import GmailCursor
from airweave.platform.cursors.google_calendar import GoogleCalendarCursor
from airweave.platform.cursors.google_drive import GoogleDriveCursor
from airweave.platform.http_client.composio_transport import ComposioProxyError
from airweave.platform.sources.slack import SlackApiError


class RetainedCaptureBinding(BaseModel):
    """Optional real isolated collection/source routing for retained operator proofs."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    enabled: bool = False
    collection_id: UUID = Field(default_factory=uuid4)
    source_connection_id: UUID = Field(default_factory=uuid4)


class BudgetExceeded(RuntimeError):
    """An explicit live-test bound was exceeded; source must not finish."""


class BoundedStorage(harness.FilesystemBackend):
    """Cap all writes, even repeated MIME reads, before retaining another blob."""

    def __init__(self, root, byte_limit, counters=None):
        super().__init__(root)
        self.written_bytes = 0
        self.byte_limit = byte_limit
        self.counters = counters

    async def write_file(self, path, content):
        if self.written_bytes + len(content) > self.byte_limit:
            raise BudgetExceeded("blob_bytes")
        await super().write_file(path, content)
        self.written_bytes += len(content)
        if self.counters is not None:
            self.counters["blob_bytes_written"] = self.written_bytes


class BoundedPageSource:
    """Test budget around the real page adapter; production driver still owns all progress."""

    def __init__(self, source, counters, record_limit, resume_probe=None):
        self.source, self.counters, self.record_limit = source, counters, record_limit
        self.resume_probe = resume_probe
        self.canonical_record_types = source.canonical_record_types
        self.canonical_container_parents = source.canonical_container_parents
        self.capture_cycle_configuration = source.capture_cycle_configuration

    async def capture_page(self, scope, continuation, *, files: FileService, parent=None):
        if self.resume_probe is not None:
            await self.resume_probe.before_page(scope, continuation)
        try:
            page = await self.source.capture_page(scope, continuation, files=files, parent=parent)
        except (InvalidScanContinuation, InvalidCaptureCheckpoint):
            if self.resume_probe is not None:
                self.resume_probe.cursor_expired()
            raise
        self.counters["records_observed"] += len(page.records) + len(page.discovered_records)
        if self.counters["records_observed"] > self.record_limit:
            raise BudgetExceeded("records")
        return page

    def child_scope(self, parent, record_type):
        return self.source.child_scope(parent, record_type)

    async def confirm_absent(self, record):
        await self.source.confirm_absent(record)


class BoundedKnownSource(BoundedPageSource):
    async def refresh_known(self, record, *, files):
        observation = await self.source.refresh_known(record, files=files)
        self.counters["records_observed"] += 1
        if self.counters["records_observed"] > self.record_limit:
            raise BudgetExceeded("records")
        return observation


class BoundedCheckpointSource(BoundedPageSource):
    async def prepare_cycle(self, previous):
        return await self.source.prepare_cycle(previous)

    def initial_continuation(self, cycle):
        return self.source.initial_continuation(cycle)


class BoundedCheckpointKnownSource(BoundedCheckpointSource, BoundedKnownSource):
    pass


class BoundedScopedSource(BoundedPageSource):
    async def prepare_cycle(self, previous):
        return await self.source.prepare_cycle(previous)

    async def prepare_scope(self, scope, cycle, previous, *, parent, force_full):
        return await self.source.prepare_scope(
            scope, cycle, previous, parent=parent, force_full=force_full
        )

    def initial_scope_continuation(self, scope, cycle, plan):
        return self.source.initial_scope_continuation(scope, cycle, plan)


class BoundedScopedKnownSource(BoundedScopedSource, BoundedKnownSource):
    pass


def bounded_page_source(source, counters, record_limit, probe=None):
    """Preserve only protocols the actual source implements."""
    if isinstance(source, ScopedPageSource):
        wrapper = (
            BoundedScopedKnownSource
            if isinstance(source, KnownObjectSource)
            else BoundedScopedSource
        )
        return wrapper(source, counters, record_limit, probe)
    checkpoint = isinstance(source, CheckpointedPageSource)
    known = isinstance(source, KnownObjectSource)
    wrapper = (
        (BoundedCheckpointKnownSource if known else BoundedCheckpointSource)
        if checkpoint
        else (BoundedKnownSource if known else BoundedPageSource)
    )
    return wrapper(source, counters, record_limit, probe)


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


def calendar_delta_expected(previous):
    """Loading an unfinished full capture resumes it; only completion starts a delta."""
    return (previous or {}).get("canonical_cycle", {}).get("phase") == "complete"


def validate_checkpoint(name, manifest, counters, saved, previous, loaded, attempt, sequence):
    """Match production cursor/scope capabilities; never manufacture a resume claim."""
    if name == "google_calendar":
        cycle = saved["canonical_cycle"]
        assert cycle["phase"] == "complete" and cycle["mode"] == "mixed"
        assert cycle["completed_job_id"]
        assert cycle["last_full_capture"] is None and cycle["promoted_checkpoint"] is None
        assert counters["started"] == counters["completed"] == 0
        assert bool(counters["sync_token_requests"]) == calendar_delta_expected(previous)
    elif name == "google_drive":
        cycle = saved["canonical_cycle"]
        assert cycle["phase"] == "complete" and cycle["completed_job_id"]
        assert cycle["last_full_capture"] and cycle["promoted_checkpoint"]
        assert cycle["promoted_checkpoint"]["checkpoint"]["value"]["page_token"]
        previous_cycle = previous.get("canonical_cycle", {})
        changes_expected = loaded and (
            previous_cycle.get("phase") == "complete" or previous_cycle.get("mode") == "changes"
        )
        # A durable cursor can also be an unfinished full enumeration. Loading it
        # resumes full capture; it is not evidence that a changes pass began.
        assert cycle["mode"] == ("changes" if changes_expected else "full")
        assert counters["started"] == counters["completed"] == 0
        assert bool(counters["resumed_changes_requests"]) == changes_expected
    elif name in {"slack", "wispr"}:
        assert saved["canonical_cycle"]["phase"] == "complete"
        assert saved["canonical_cycle"]["completed_job_id"]
    else:
        cycle = saved["canonical_cycle"]
        assert cycle["phase"] == "complete" and cycle["completed_job_id"]
        if manifest.get("gmail_unfiltered"):
            assert cycle["promoted_checkpoint"] and cycle["last_full_capture"]
            if previous.get("canonical_cycle", {}).get("phase") == "complete":
                assert cycle["mode"] == "changes"
                assert counters["resumed_changes_requests"] > 0
                assert counters["capture_profile_requests"] == 0
        else:
            assert cycle["mode"] == "full" and cycle["promoted_checkpoint"] is None
    assert saved["canonical_checkpoint"] == {
        "writer_attempt_id": str(attempt.id),
        "observed_change_sequence": sequence,
    }
    assert saved != previous


async def prepare_job(db, organization_id, sync_id, job_id, attempt_number):
    """Only an explicit retry may reuse a still-running job in the private schema."""
    if attempt_number == 1:
        job = SyncJob(id=job_id, organization_id=organization_id, sync_id=sync_id, status="pending")
        db.add(job)
        await db.commit()
        return job
    job = await db.get(SyncJob, job_id)
    if (
        job is None
        or job.sync_id != sync_id
        or job.organization_id != organization_id
        or job.status != "running"
    ):
        raise ValueError("Resume requires the same interrupted running job")
    return job


def prepare_resume_probe(manifest, sessions, organization_id, sync_id, job_id, attempt, counters):
    """Fault injection is opt-in for the two explicit cross-process trial stages."""
    if manifest.get("resume_stage") not in {"interrupt", "resume"}:
        return None
    from capture_resume import PageResumeProbe
    from gmail_resume import GmailResumeProbe
    from wispr_resume import WisprResumeProbe

    probe_type = (
        GmailResumeProbe
        if manifest.get("gmail_resume")
        else (WisprResumeProbe if manifest.get("wispr_resume") else PageResumeProbe)
    )
    return probe_type(
        sessions,
        organization_id,
        sync_id,
        job_id,
        attempt.id,
        Path(manifest["root"]) / "resume-target.json",
        manifest["resume_stage"],
        counters,
    )


def count_gmail_request(name, request, previous, counters, identity_verified):
    """Count required identity verification separately from production capture planning."""
    if name != "gmail":
        return
    if request.url.path.endswith("/profile"):
        key = "capture_profile_requests" if identity_verified else "identity_profile_requests"
        counters[key] += 1
    operation = (
        "attachment_get_requests"
        if "/attachments/" in request.url.path
        else "message_list_requests"
        if request.url.path.endswith("/messages")
        else "message_get_requests"
        if "/messages/" in request.url.path
        else "other_gmail_requests"
    )
    counters[operation] = counters.get(operation, 0) + 1
    if request.url.path.endswith("/history"):
        boundary = previous.get("canonical_cycle", {}).get("promoted_checkpoint")
        if (
            boundary
            and request.url.params.get("startHistoryId")
            == boundary["checkpoint"]["value"]["history_id"]
        ):
            counters["resumed_changes_requests"] += 1


def count_drive_request(name, request, previous, counters):
    """Count only an actual request using the previously published Drive boundary."""
    if name != "google_drive" or not request.url.path.endswith("/changes"):
        return
    promoted = previous.get("canonical_cycle", {}).get("promoted_checkpoint")
    if (
        promoted
        and request.url.params.get("pageToken") == promoted["checkpoint"]["value"]["page_token"]
    ):
        counters["resumed_changes_requests"] += 1


async def authenticate_retained_source(sessions, binding, organization_id, sync_id, name):
    """Source creation already verified its pinned profile; retain that attestation only."""
    if not binding.enabled:
        return
    identity_fields = {
        "gmail": ("expected_mailbox", "LIVE_EXPECTED_EMAIL"),
        "google_calendar": ("expected_primary_calendar_id", "LIVE_CALENDAR_PRIMARY_ID"),
        "google_drive": ("expected_permission_id", "LIVE_DRIVE_PERMISSION_ID"),
    }
    if name not in identity_fields:
        raise ValueError("Retained source identity qualification is not configured")
    identity_field, environment_field = identity_fields[name]
    expected_identity = os.environ[environment_field]
    async with sessions() as db:
        bound = await db.get(SourceConnection, binding.source_connection_id)
        if (
            bound is None
            or bound.organization_id != organization_id
            or bound.sync_id != sync_id
            or bound.short_name != name
            or not bound.config_fields
            or bound.config_fields.get(identity_field) != expected_identity
        ):
            raise ValueError("Retained capture source binding does not match")
        bound.is_authenticated = True
        await db.commit()


def require_read_only_drive(name, request):
    """Keep the live Drive qualification incapable of provider mutation."""
    if name == "google_drive" and request.method != "GET":
        raise ValueError("Drive qualification permits provider reads only")


def verify_slack_capture_policy(name, manifest, source):
    if name == "slack" and manifest.get("slack_config", {}).get("capture_files"):
        assert "file" in source.canonical_record_types
        assert source.capture_cycle_configuration.known_object_validation == ("message",)


async def child(manifest, *, runtime_sessions=None, storage_backend=None):
    engine = create_async_engine(
        (
            manifest["runtime_database_url"]
            if runtime_sessions is not None
            else harness.test_database_url()
        ),
        connect_args={"server_settings": {"search_path": manifest["schema"]}},
    )
    sessions = runtime_sessions or async_sessionmaker(engine, expire_on_commit=False)
    organization_id, sync_id = UUID(manifest["organization_id"]), UUID(manifest["sync_id"])
    job_id = UUID(manifest["job_id"]) if "job_id" in manifest else uuid4()
    attempt_number = manifest.get("attempt_number", 1)
    retained_binding = RetainedCaptureBinding.model_validate(manifest.get("retained_binding", {}))
    counters = {"provider_requests": 0, "records_observed": 0, "started": 0, "completed": 0}
    result = {"failed": True}
    previous = {}
    previous_calendar_checkpoint = None
    name = manifest["provider"]
    is_calendar = name == "google_calendar"
    counters["sync_token_requests"] = 0
    counters["resumed_changes_requests"] = 0
    counters["identity_profile_requests"] = 0
    counters["capture_profile_requests"] = 0
    counters["blob_bytes_written"] = 0
    identity_verified = False
    from functools import partial

    from wispr_diagnostics import WisprDiagnostics, observe_request

    wispr_diagnostics = WisprDiagnostics()
    last_operation = "identity"
    rate_limits = []
    resume_probe = None

    async def observe_slack_rate_limit(response):
        await response.aread()
        payload = (
            response.json()
            if response.request.url.host == "slack.com"
            and response.request.url.path.startswith("/api/")
            and "json" in response.headers.get("content-type", "")
            else {}
        )
        limited = response.status_code == 429 or payload.get("error") in {
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
        require_read_only_drive(name, request)
        last_operation = (
            "export"
            if request.url.path.endswith("/export")
            else ("download" if request.url.params.get("alt") == "media" else "metadata")
        )
        if counters["provider_requests"] >= manifest["request_limit"]:
            raise BudgetExceeded("provider_requests")
        counters["provider_requests"] += 1
        count_gmail_request(name, request, previous, counters, identity_verified)
        if is_calendar:
            count_calendar_request(
                request,
                manifest["calendar_config"]["calendar_ids"][0],
                previous_calendar_checkpoint,
                counters,
            )
        count_drive_request(name, request, previous, counters)

    @asynccontextmanager
    async def db_context(scoped_organization_id):
        assert scoped_organization_id == organization_id
        async with sessions() as db:
            yield db

    try:
        async with sessions() as db:
            organization = await db.get(Organization, organization_id)
            sync = await db.get(Sync, sync_id)
            job = await prepare_job(db, organization_id, sync_id, job_id, attempt_number)
            before = {
                row.id: source_record(row)
                for row in await db.scalars(select(Entity).where(Entity.sync_id == sync_id))
                if row.record_revision > 0
            }

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
            collection_id=retained_binding.collection_id,
            source_connection_id=retained_binding.source_connection_id,
            source_short_name=name,
            connection=SimpleNamespace(id=uuid4(), short_name=name),
            execution_config=config,
            force_full_sync=manifest.get("force_full", False),
            batch_size=10,
            max_batch_latency_ms=20,
            should_batch=True,
            has_user_context=False,
            tracking_email=None,
        )
        cursor_service = SyncCursorService()
        async with sessions() as db:
            previous = await cursor_service.get_cursor_data(db, sync_id, ctx)
            if is_calendar:
                previous_calendar_checkpoint = await event_checkpoint(
                    db, organization_id, sync_id, manifest["calendar_config"]["calendar_ids"][0]
                )
        cursor_schema = {
            "gmail": GmailCursor,
            "google_calendar": GoogleCalendarCursor,
            "google_drive": GoogleDriveCursor,
        }.get(name)
        cursor = SyncCursor(sync_id, cursor_schema, previous or None) if cursor_schema else None
        loaded = cursor.loaded_from_db if cursor else False
        storage = storage_backend or BoundedStorage(
            Path(manifest["root"]) / "blobs", manifest["blob_byte_limit"], counters
        )
        storage.counters = counters
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
            wispr_source(
                os.environ[account_variable],
                key,
                fence,
                request_hook=partial(observe_request, wispr_diagnostics, request_hook),
                response_hook=wispr_diagnostics.response,
                envelope_hook=wispr_diagnostics.envelope,
            )
            if name == "wispr"
            else rest_source(
                name,
                os.environ[account_variable],
                os.environ["LIVE_EXPECTED_EMAIL"],
                key,
                fence,
                gmail_query=manifest.get("query", ""),
                gmail_unfiltered=manifest.get("gmail_unfiltered", False),
                calendar_config=GoogleCalendarConfig.model_validate(manifest["calendar_config"])
                if is_calendar
                else None,
                request_hook=request_hook,
                response_hook=observe_slack_rate_limit if name == "slack" else None,
                slack_config=(
                    SlackConfig.model_validate(manifest["slack_config"])
                    if name == "slack" and "slack_config" in manifest
                    else None
                ),
                max_file_bytes=manifest["file_byte_limit"],
            )
        )
        async with (
            asyncio.timeout(manifest["timeout"]),
            connection as (source, identity_verification),
        ):
            verify_slack_capture_policy(name, manifest, source)
            identity_verified = True
            await authenticate_retained_source(
                sessions, retained_binding, organization_id, sync_id, name
            )
            assert source.cursor_class == cursor_schema
            bus = FakeEventBus()
            attempt = CaptureAttempt(id=uuid4(), number=attempt_number)
            resume_probe = prepare_resume_probe(
                manifest, sessions, organization_id, sync_id, job_id, attempt, counters
            )
            page_source = (
                bounded_page_source(source, counters, manifest["record_limit"], resume_probe)
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
                files=files,
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
                patch(
                    "airweave.domains.syncs.jobs.state_machine.get_tenant_db_context", db_context
                ),
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
        await verify_calendar_scopes(
            sessions, organization_id, sync_id, manifest, saved, calendar_delta_expected(previous)
        )
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
            "full_scope_completed": name != "wispr"
            and not (name == "gmail" and saved["canonical_cycle"]["mode"] == "changes")
            and not (is_calendar and calendar_delta_expected(previous)),
            "counters_complete": True,
            "resumed_saved_page": resume_probe.resumed_saved_page if resume_probe else False,
            "saved_cursor_expired": resume_probe.saved_cursor_expired if resume_probe else False,
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
            "observation_comparison": compare_observations(
                before,
                {row.id: source_record(row) for row in rows if row.record_revision > 0},
                name,
            ),
            "mode": {
                "gmail": ("unfiltered_" + saved["canonical_cycle"]["mode"])
                if manifest.get("gmail_unfiltered")
                else "filtered_full_reconciliation",
                "google_calendar": "calendar_incremental"
                if calendar_delta_expected(previous)
                else "calendar_resumed_full"
                if loaded
                else "calendar_initial",
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
            "full_scope_completed": False,
            "counters_complete": True,
            "resumed_saved_page": resume_probe.resumed_saved_page if resume_probe else False,
            "saved_cursor_expired": resume_probe.saved_cursor_expired if resume_probe else False,
            "error_type": type(error).__name__,
            "safe_reason": safe_failure_reason(error),
            "last_operation": wispr_diagnostics.operation if name == "wispr" else last_operation,
            "wispr_diagnostics": wispr_diagnostics.model_dump() if name == "wispr" else None,
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


async def execute_trial(path, timeout):
    """Return only sanitized child evidence; forced termination has unknown usage."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).absolute()),
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        return -9, {
            "failed": True,
            "safe_reason": "aggregate_deadline",
            "full_scope_completed": False,
            "counters_complete": False,
        }
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    lines = [json.loads(line) for line in stdout.decode().splitlines() if line.startswith("{")]
    if not lines:
        return process.returncode, {
            "failed": True,
            "safe_reason": "child_no_result",
            "full_scope_completed": False,
            "counters_complete": False,
        }
    return process.returncode, lines[-1]


def remaining_blob_budget(manifest, results, total):
    """Repeated writes consume the same parent-owned trial allowance."""
    if total is None:
        return True
    manifest["blob_byte_limit"] = total - sum(item.get("blob_bytes_written", 0) for item in results)
    if manifest["blob_byte_limit"] > 0:
        return True
    results.append(
        {"failed": True, "safe_reason": "aggregate_blob_budget", "full_scope_completed": False}
    )
    return False


def mark_early_completion(gmail_resume, stage, code, result):
    if gmail_resume and stage == "interrupt" and code == 0:
        result["recovery_not_exercised"] = True


async def run_trials(manifest, evidence_reader=None):
    """One parent-owned aggregate budget across interruption, retry, and new-job proof."""
    path = Path(manifest["root"]) / "manifest.json"
    results = []
    name = manifest["provider"]
    wispr_resume = manifest.get("wispr_resume", False)
    gmail_resume = manifest.get("gmail_resume", False)
    resume_trial = name == "slack" or wispr_resume or gmail_resume
    stages = (
        (
            ("interrupt", "resume")
            if wispr_resume or gmail_resume
            else ("interrupt", "resume", "new_cycle")
        )
        if resume_trial
        else ("first", "second")
    )
    total_requests, total_records = manifest["request_limit"], manifest["record_limit"]
    total_blob_bytes = manifest.get("blob_byte_limit")
    deadline = time.monotonic() + manifest["timeout"] if resume_trial else None
    interrupted_job_id = str(uuid4())
    for stage in stages:
        check_free_disk(name)
        if resume_trial:
            remaining = deadline - time.monotonic()
            requests_left = total_requests - sum(
                item.get("provider_requests", 0) for item in results
            )
            records_left = total_records - sum(item.get("records_observed", 0) for item in results)
            if remaining <= 0 or requests_left <= 0 or records_left <= 0:
                results.append(
                    {
                        "failed": True,
                        "safe_reason": "aggregate_budget",
                        "full_scope_completed": False,
                    }
                )
                break
            if not remaining_blob_budget(manifest, results, total_blob_bytes):
                break
            manifest.update(
                resume_stage=stage,
                job_id=interrupted_job_id if stage != "new_cycle" else str(uuid4()),
                attempt_number=2 if stage == "resume" else 1,
                request_limit=requests_left,
                record_limit=records_left,
                timeout=remaining,
            )
        path.write_text(json.dumps(manifest))
        code, result = await execute_trial(
            path,
            max(0.001, deadline - time.monotonic()) if resume_trial else manifest["timeout"] + 60,
        )
        if stage == "resume" and evidence_reader is not None:
            result.update(await evidence_reader())
            result["resumed_partial"] = bool(
                result.get("resumed_page_committed") and not result.get("full_scope_completed")
            )
        mark_early_completion(gmail_resume, stage, code, result)
        results.append(result)
        print(json.dumps({"run": len(results), "stage": stage, **result}), flush=True)
        if stage == "interrupt":
            if code != 75 or not result.get("intentional_interruption"):
                break
        elif (
            (wispr_resume or gmail_resume)
            and stage == "resume"
            and code == 76
            and (result.get("wispr_recovery_verified") or result.get("gmail_recovery_verified"))
        ):
            break
        elif code:
            break
    return results


def trial_counter_summary(results):
    """A killed child has unknown usage; known values are only a lower bound."""
    complete = all(item.get("counters_complete", True) for item in results)
    requests = sum(item.get("provider_requests", 0) for item in results)
    records = sum(item.get("records_observed", 0) for item in results)
    return {
        "aggregate_counters_complete": complete,
        "aggregate_provider_requests": requests if complete else None,
        "aggregate_records_observed": records if complete else None,
        "known_provider_requests": requests,
        "known_records_observed": records,
    }


def trial_read_summary(results):
    """Report observed token reuse and revision stability independently of full scopes."""
    successful = [item for item in results if not item.get("failed", True)]
    digests = [item.get("payload_revision_digest") for item in successful]
    reused = any(
        item.get("loaded_durable_checkpoint", False)
        and not item.get("saved_cursor_expired", False)
        and (
            item.get("resumed_saved_page", False)
            or item.get("sync_token_requests", 0) > 0
            or item.get("resumed_changes_requests", 0) > 0
        )
        for item in successful
    )
    return {
        "saved_cursor_reused": reused,
        "identical_second_read": len(results) == len(successful) == 2
        and bool(digests[0])
        and digests[0] == digests[1],
    }


def wispr_request_limit(value: str) -> int:
    """Validate an explicitly reduced trial budget before setup or network access."""
    try:
        limit = int(value)
    except ValueError:
        raise ValueError("Wispr request limit must be an integer from 1 to 20") from None
    if not 1 <= limit <= 20:
        raise ValueError("Wispr request limit must be an integer from 1 to 20")
    return limit


def gmail_trial_options(name):
    resume = os.environ.get("LIVE_GMAIL_RESUME") == "1"
    unfiltered = os.environ.get("LIVE_GMAIL_UNFILTERED") == "1"
    if (resume or unfiltered) and name != "gmail":
        raise ValueError("Gmail trial options require Gmail provider")
    if resume and not unfiltered:
        raise ValueError("Gmail recovery proof requires explicit unfiltered scope")
    return resume, unfiltered


async def main():
    name = os.environ.get("LIVE_LIFECYCLE_PROVIDER", "gmail")
    if name not in {"gmail", "google_calendar", "google_drive", "slack", "wispr"}:
        raise ValueError("Unsupported lifecycle provider")
    wispr_resume = os.environ.get("LIVE_WISPR_RESUME") == "1"
    gmail_resume, gmail_unfiltered = gmail_trial_options(name)
    if wispr_resume and name != "wispr":
        raise ValueError("Wispr resume requires Wispr provider")
    request_limit = (
        wispr_request_limit(os.environ.get("LIVE_WISPR_REQUEST_LIMIT", "20"))
        if wispr_resume
        else None
    )
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
                "0006_record_visibility.py",
                "0007_scan_scope_owner.py",
                "0008_scope_execution.py",
                "0009_extraction_coverage.py",
                "0010_owned_provisioning.py",
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
                    "slack": 7200,
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
            if name == "gmail":
                manifest.update(gmail_resume=gmail_resume, gmail_unfiltered=gmail_unfiltered)
            if wispr_resume:
                manifest.update(
                    wispr_resume=True, request_limit=request_limit, record_limit=600, timeout=180
                )
            if name == "google_calendar":
                now = datetime.now(timezone.utc).replace(microsecond=0)
                manifest["calendar_config"] = {
                    "calendar_ids": [os.environ["LIVE_EXPECTED_EMAIL"]],
                    "occurrence_window": {
                        "start": now.isoformat(),
                        "end": (now + timedelta(days=7)).isoformat(),
                    },
                }

            async def resume_evidence():
                from capture_resume import inspect_resume_progress

                return await inspect_resume_progress(
                    sessions,
                    organization_id,
                    sync_id,
                    UUID(manifest["job_id"]),
                    root / "resume-target.json",
                )

            results = await run_trials(
                manifest, resume_evidence if name == "slack" or gmail_resume else None
            )

    finally:
        os.umask(old_umask)
        await engine.dispose()
        if created:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
    resume_verified = (
        name == "slack"
        and len(results) == 3
        and results[0].get("intentional_interruption")
        and results[1].get("resumed_page_committed")
        and results[1].get("full_scope_completed")
        and results[2].get("full_scope_completed")
    )
    gmail_verified = bool(
        gmail_resume
        and len(results) == 2
        and results[0].get("intentional_interruption")
        and results[1].get("resumed_page_committed")
        and results[1].get("resumed_saved_page")
        and results[1].get("capture_profile_requests") == 0
        and not results[1].get("saved_cursor_expired")
    )
    print(
        json.dumps(
            {
                "runs": len(results),
                "free_disk_bytes_before": free_disk_bytes,
                "schema_removed": True,
                "blob_directory_removed": True,
                "resume_verified": bool(resume_verified),
                "gmail_recovery_verified": gmail_verified,
                "wispr_recovery_verified": bool(
                    wispr_resume
                    and len(results) == 2
                    and results[1].get("wispr_recovery_verified", False)
                ),
                "resumed_page_committed": any(
                    item.get("resumed_page_committed", False) for item in results
                ),
                "resumed_partial": any(item.get("resumed_partial", False) for item in results),
                **trial_counter_summary(results),
                **trial_read_summary(results),
            }
        )
    )
    wispr_verified = (
        wispr_resume and len(results) == 2 and results[1].get("wispr_recovery_verified", False)
    )
    success = (
        gmail_verified
        if gmail_resume
        else wispr_verified
        if wispr_resume
        else (resume_verified if name == "slack" else len(results) == 2)
    )
    return 0 if success and not any(item["failed"] for item in results) else 1


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
