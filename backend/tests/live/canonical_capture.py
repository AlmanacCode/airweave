"""Opt-in durable provider proof against an isolated, private local test PostgreSQL.

Run from backend with PYTHONPATH=. and explicit environment variables documented in
README.md. No provider writes; this script writes real private records only into a
unique disposable schema and a mode0700 temporary blob directory. Both are removed.
"""

import asyncio
import hashlib
import json
import logging
import os
import secrets
from contextlib import aclosing
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

# The application DSN is deliberately made unusable. Only the explicit test DSN below
# is ever handed to an engine. Provider credentials remain subprocess environment data.
os.environ.update(
    ENVIRONMENT="local",
    TESTING="true",
    AUTH_ENABLED="false",
    AUTH_MODE="local",
    POSTGRES_HOST="unused-live-probe.invalid",
    POSTGRES_USER="unused",
    POSTGRES_PASSWORD="unused",
)
import httpx  # noqa: E402
from pydantic import ValidationError  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

import conftest  # noqa: E402,F401 - local test settings must precede Airweave imports
from airweave.adapters.storage.filesystem import FilesystemBackend  # noqa: E402
from airweave.domains.entities.canonical.query import CanonicalQueryService  # noqa: E402
from airweave.domains.entities.canonical.query_models import (  # noqa: E402
    RecordFilters,
    RecordListQuery,
)
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore  # noqa: E402
from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord  # noqa: E402
from airweave.domains.entities.canonical.service import CanonicalCaptureService  # noqa: E402
from airweave.domains.entities.canonical.store import CanonicalRecordStore  # noqa: E402
from airweave.domains.entities.canonical.tests.conftest import migrate  # noqa: E402
from airweave.domains.storage.file_service import FileService  # noqa: E402
from airweave.domains.storage.paths import StoragePaths  # noqa: E402
from airweave.models import Organization, Sync, SyncJob  # noqa: E402

logging.disable(logging.CRITICAL)
PROBE_STAGE = "initialization"


def test_database_url() -> str:
    """Reject network/application databases even if a caller supplies the wrong URL."""
    value = os.environ["CANONICAL_TEST_DATABASE_URL"]
    url = make_url(value)
    socket = url.query.get("host", "")
    if (
        url.drivername != "postgresql+asyncpg"
        or url.database != "sync_tests"
        or url.username != "sync_test"
        or url.host
        or not socket.startswith("/")
    ):
        raise ValueError("An explicit private Unix-socket sync_tests database is required")
    directory = Path(socket).resolve()
    if (
        not directory.is_dir()
        or directory.stat().st_uid != os.getuid()
        or directory.stat().st_mode & 0o077
    ):
        raise ValueError("The database socket directory must belong to the current user")
    return value


async def fetch_records(name, account, expected_email, key, fence, root):
    """Exercise actual source capture, bounded before any complete-scope claim."""
    from provider_sample import rest_source, wispr_source

    from airweave.domains.entities.canonical.page_source import CanonicalPageSource
    from airweave.domains.entities.canonical.requests import RecordIdentity
    from airweave.domains.entities.canonical.source import ContainerScopedSource

    storage = FilesystemBackend(root / "blobs")
    files = FileService(fence.job_id, storage, sync_id=fence.sync_id)
    files.MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
    connection = (
        wispr_source(account, key, fence)
        if name == "wispr"
        else rest_source(name, account, expected_email, key, fence)
    )
    records = {}
    observations = 0
    async with asyncio.timeout(180), connection as (source, identity_verification):
        if isinstance(source, CanonicalPageSource):
            raise ValueError("Page sources require the production provider_lifecycle.py harness")
        parents = (
            source.canonical_container_parents if isinstance(source, ContainerScopedSource) else {}
        )
        async with aclosing(source.generate_observations(files=files)) as stream:
            async for item in stream:
                # No scope-completion marker/checkpoint is saved by a bounded sample.
                if not isinstance(item, CaptureRecord):
                    continue
                observations += 1
                parent_type = parents.get(item.identity.record_type)
                if parent_type is not None:
                    # Apply the declared source relationship just as the pipeline does;
                    # preserve the provider payload verbatim.
                    parent = RecordIdentity(
                        record_type=parent_type, native_id=item.identity.container_id
                    )
                    if item.parent is not None and item.parent != parent:
                        raise ValueError("Source parent conflicts with its declared relationship")
                    item = item.model_copy(update={"parent": parent})
                identity = (item.identity.record_type, item.identity.entity_key)
                records[identity] = item
                if sample_complete(name, list(records.values()), observations):
                    break
    values = list(records.values())
    required = {"google_calendar": "event", "slack": "message", "wispr": "meeting"}.get(name)
    if required and not any(x.identity.record_type == required for x in values):
        raise ValueError("Bounded source sample did not contain the required record type")
    if name in {"gmail", "google_drive"} and not any(x.blobs for x in values):
        raise ValueError("Bounded source sample did not contain a real owned blob")
    return values, storage, identity_verification, observations


def sample_complete(name, records, observations):
    """Bound provider work; a sample is not a completed enumeration."""
    if observations >= 25:
        return True
    if name == "google_calendar":
        return False
    if len(records) < 3:
        return False
    if name in {"gmail", "google_drive"}:
        return any(record.blobs for record in records)
    required = "message" if name == "slack" else "meeting"
    return any(record.identity.record_type == required for record in records)


async def verify(name, account, email, key, sessions, engine, root):
    """Commit, disconnect, reopen, read/list/page, verify disk bytes, then replay."""
    organization_id, sync_id, job_id = uuid4(), uuid4(), uuid4()
    async with sessions() as db:
        db.add(Organization(id=organization_id, name="Private live capture proof"))
        await db.flush()
        db.add(Sync(id=sync_id, organization_id=organization_id, name="Bounded live " + name))
        await db.flush()
        db.add(
            SyncJob(id=job_id, organization_id=organization_id, sync_id=sync_id, status="running")
        )
        await db.commit()
    store = CanonicalRecordStore()
    service = CanonicalCaptureService(store)
    query = CanonicalQueryService(store, CanonicalQueryStore(), secrets.token_urlsafe(32))
    async with sessions() as db:
        fence = await service.activate_writer(
            db, organization_id, sync_id, job_id, attempt_id=uuid4(), attempt_number=1
        )
    global PROBE_STAGE
    PROBE_STAGE = name + ":provider_capture"
    records, _, identity_verification, observations = await fetch_records(
        name, account, email, key, fence, root
    )
    PROBE_STAGE = name + ":durable_capture"
    async with sessions() as db:
        committed = await service.capture(db, CaptureBatch(fence=fence, records=tuple(records)))
    await engine.dispose()  # Force readback over fresh PostgreSQL connections.
    PROBE_STAGE = name + ":durable_readback"
    storage = FilesystemBackend(root / "blobs")  # New filesystem storage instance.
    expected = {(item.identity.record_type, item.identity.entity_key): item for item in records}
    listed = []
    cursor = None
    while True:
        async with sessions() as db:
            page = await query.list_records(
                db,
                organization_id,
                sync_id,
                RecordListQuery(limit=2, cursor=cursor, filters=RecordFilters(state="all")),
            )
        listed.extend(page.records)
        if not page.has_more:
            break
        cursor = page.next_cursor
    assert len(listed) == len(expected)
    total_bytes = blob_count = 0
    for stored in listed:
        assert (
            stored.payload
            == expected[(stored.identity.record_type, stored.identity.entity_key)].payload
        )
        async with sessions() as db:
            reread = await query.read(db, organization_id, sync_id, stored.id)
        assert reread.payload == stored.payload and reread.revision == 1
        for blob in reread.blobs:
            content = await storage.read_file(blob.key)
            assert len(content) == blob.size_bytes
            assert hashlib.sha256(content).hexdigest() == blob.sha256
            total_bytes += len(content)
            blob_count += 1
    changes = []
    change_cursor = None
    while True:
        async with sessions() as db:
            page = await query.changes(db, organization_id, sync_id, cursor=change_cursor, limit=2)
        changes.extend(page.changes)
        change_cursor = page.next_cursor
        if not page.has_more:
            break
    assert len(changes) == len(expected) == len(committed.changes)
    async with sessions() as db:
        replay = await service.capture(db, CaptureBatch(fence=fence, records=tuple(records)))
    assert replay.unchanged == len(records) and not replay.changes
    async with sessions() as db:
        tail = await query.changes(db, organization_id, sync_id, cursor=change_cursor)
    assert not tail.changes
    consumer = None
    if name == "gmail" and os.environ.get("LIVE_ALMANAC_PYTHON"):
        from almanac_handoff import verify_almanac_reader

        PROBE_STAGE = name + ":almanac_http_reader"
        await engine.dispose()
        consumer = await verify_almanac_reader(
            records=listed,
            storage=storage,
            query=query,
            sessions=sessions,
            organization_id=organization_id,
            sync_id=sync_id,
            root=root,
        )
    return {
        "almanac_reader": consumer,
        "provider": name,
        "identity_verification": identity_verification,
        "observations": observations,
        "partial_records": sum(record.completeness != "complete" for record in listed),
        "record_types": sorted({record.identity.record_type for record in listed}),
        "records": len(listed),
        "journal_changes": len(changes),
        "blobs": blob_count,
        "blob_bytes": total_bytes,
        "blob_sha_verified": True if blob_count else None,
        "replay_new_changes": 0,
        "fresh_connections_readback": True,
        "full_scope_completed": False,
        "checkpoint_saved": False,
    }


async def main():
    """Allocate and destroy only private disposable local resources."""
    url = test_database_url()
    key = os.environ["COMPOSIO_API_KEY"]
    selected = os.environ.get("LIVE_PROVIDERS", "gmail,google_drive").split(",")
    if not selected or set(selected) - {
        "gmail",
        "google_drive",
        "google_calendar",
        "slack",
        "wispr",
    }:
        raise ValueError(
            "LIVE_PROVIDERS must select gmail, google_drive, google_calendar, slack or wispr"
        )
    inputs = [
        (name, os.environ[variable], os.environ["LIVE_EXPECTED_EMAIL"] if name != "wispr" else "")
        for name, variable in (
            ("gmail", "LIVE_GMAIL_ACCOUNT_ID"),
            ("google_drive", "LIVE_DRIVE_ACCOUNT_ID"),
            ("google_calendar", "LIVE_CALENDAR_ACCOUNT_ID"),
            ("slack", "LIVE_SLACK_ACCOUNT_ID"),
            ("wispr", "LIVE_WISPR_ACCOUNT_ID"),
        )
        if name in selected
    ]
    schema = "canonical_live_" + uuid4().hex
    admin = create_async_engine(url)
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    old_path = StoragePaths.TEMP_PROCESSING
    old_umask = os.umask(0o077)
    created = False
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
            ):
                await connection.run_sync(migrate, migration)
        with TemporaryDirectory(prefix="airweave-private-live-") as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            StoragePaths.TEMP_PROCESSING = str(root / "downloads")
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            for name, account, email in inputs:
                result = await verify(name, account, email, key, sessions, engine, root)
                results.append(result)
                print(json.dumps({"verified_provider": result}), flush=True)
    finally:
        StoragePaths.TEMP_PROCESSING = old_path
        os.umask(old_umask)
        await engine.dispose()
        if created:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
    print(
        json.dumps(
            {"verification": results, "schema_removed": True, "blob_directory_removed": True}
        )
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        # Never print SQL parameters, native payloads, credentials, or exception tracebacks.
        details = {"failed": True, "error_type": type(error).__name__, "stage": PROBE_STAGE}
        if isinstance(error, ValidationError):
            # Types only: validation input/context can contain entire original messages.
            details["validation_error_types"] = sorted(
                {item["type"] for item in error.errors(include_input=False, include_context=False)}
            )
        if isinstance(error, httpx.HTTPStatusError):
            details["http_status"] = error.response.status_code
            details["host"] = error.request.url.host
            try:
                native = error.response.json().get("error", {})
                reasons = [item.get("reason") for item in native.get("errors", [])]
                allowed = {
                    "exportSizeLimitExceeded",
                    "cannotDownloadFile",
                    "insufficientFilePermissions",
                    "insufficientPermissions",
                    "fileNotDownloadable",
                    "downloadRestrictedForRevision",
                    "forbidden",
                    "rateLimitExceeded",
                    "userRateLimitExceeded",
                }
                details["recognized_provider_reasons"] = [r for r in reasons if r in allowed]
            except (ValueError, AttributeError, TypeError):
                pass
        print(json.dumps(details))
        raise SystemExit(1) from None
