"""Real SQL lifecycle with native synthetic GitHub HTTP and actual hierarchy adapter."""

import hashlib
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.requests import BlobReference
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor
from airweave.platform.configs.config import GitHubConfig
from airweave.platform.sources.github import GitHubSource

REPO = {"id": 100, "owner": {"id": 200}, "full_name": "team/repo", "default_branch": "main"}
ISSUE = {
    "id": 300,
    "number": 7,
    "repository_url": "https://api.github.com/repos/team/repo",
    "title": "Synthetic issue",
}


class NativeGitHub:
    """Fixture native pages, retaining actual endpoint/query semantics."""

    def __init__(self):
        self.code = False
        self.blob_reads = []
        self.code_bytes = b"synthetic code"
        self.blob_sha = hashlib.sha1(b"blob 14\0" + self.code_bytes).hexdigest()
        self.comment_pages = []
        self.unavailable = False

    async def get(self, url, **kwargs):
        parsed = urlsplit(url)
        headers = {}
        status = 200
        if self.unavailable:
            payload, status = {}, 404
        elif parsed.path == "/repos/team/repo":
            payload = REPO
        elif parsed.path == "/repos/team/repo/issues":
            payload = [ISSUE]
        elif parsed.path == "/repos/team/repo/issues/7":
            payload = ISSUE
        elif parsed.path == "/repos/team/repo/issues/7/comments":
            page = int(parse_qs(parsed.query)["page"][0])
            self.comment_pages.append(page)
            payload = [{"id": 500 + page, "body": f"Synthetic page {page}"}]
            if page == 1:
                headers["Link"] = (
                    "<https://api.github.com/repos/team/repo/issues/7/comments"
                    '?per_page=100&page=2>; rel="next"'
                )
        else:
            return self.code_response(url)
        return httpx.Response(
            status, json=payload, headers=headers, request=httpx.Request("GET", url)
        )

    def code_response(self, url):
        parsed = urlsplit(url)
        if parsed.path.endswith("/branches/main"):
            payload = {"name": "main", "commit": {"sha": "1" * 40}}
        elif "/git/commits/" in parsed.path:
            payload = {"sha": "1" * 40, "tree": {"sha": "2" * 40}}
        elif "/git/trees/" in parsed.path:
            payload = {
                "sha": "2" * 40,
                "truncated": False,
                "tree": [
                    {
                        "path": name,
                        "mode": "100644",
                        "type": "blob",
                        "sha": self.blob_sha,
                        "size": len(self.code_bytes),
                    }
                    for name in ("first.py", "second.py")
                ],
            }
        elif "/git/blobs/" in parsed.path:
            self.blob_reads.append(parsed.path)
            return httpx.Response(200, content=self.code_bytes, request=httpx.Request("GET", url))
        else:
            raise AssertionError(f"Unexpected synthetic route: {parsed.path}")
        return httpx.Response(200, json=payload, request=httpx.Request("GET", url))


async def runner(database, source, native, *, attempt=1, service=None):
    default, fence = source
    ctx, _, runtime, bus = components(database, source)
    client = AsyncMock()
    client.get.side_effect = native.get

    @asynccontextmanager
    async def stream(method, url, **kwargs):
        original = await native.get(url, **kwargs)
        streamed = httpx.Response(
            original.status_code,
            headers=original.headers,
            stream=httpx.ByteStream(original.content),
        )
        try:
            yield streamed
        finally:
            await streamed.aclose()

    client.stream = MagicMock(side_effect=stream)
    files = MagicMock()

    async def store_blob(content, *, media_type=None):
        digest = hashlib.sha256(content).hexdigest()
        return BlobReference(
            key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
            sha256=digest,
            size_bytes=len(content),
            media_type=media_type,
        )

    files.store_canonical_blob = AsyncMock(side_effect=store_blob)
    connector = await GitHubSource.create(
        auth=StaticTokenProvider("synthetic"),
        logger=MagicMock(),
        http_client=client,
        config=GitHubConfig.model_validate(
            {
                "repositories": [{"repository_id": 100, "owner_id": 200, "full_name": "team/repo"}],
                "include_code": native.code,
            }
        ),
    )
    pipeline = CanonicalCapturePipeline(
        service or default,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
        connector.canonical_container_parents,
        page_source=connector,
        files=files,
    )
    runtime.source, runtime.canonical_capture = connector, pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    return instance


async def test_nested_comment_lost_ack_resumes_without_skipping_or_repeating_committed_page(
    database, source
):
    class LostAck(CanonicalCaptureService):
        async def commit_scan_page(self, db, request):
            result = await super().commit_scan_page(db, request)
            if request.scope.record_type == "comment":
                raise ConnectionError("synthetic lost acknowledgment")
            return result

    native = NativeGitHub()
    first = await runner(database, source, native, service=LostAck(source[0].store))
    with pytest.raises(ConnectionError, match="acknowledgment"):
        await run(first)
    async with database() as db:
        comment_scan = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "comment")
        )
        assert comment_scan.continuation["page"] == 2
        prior_sweep = comment_scan.sweep_id
        assert (await db.scalar(select(SyncCursor))).cursor_data["canonical_cycle"][
            "phase"
        ] == "active"
    second = await runner(database, source, native, attempt=2)
    await run(second)
    assert native.comment_pages == [1, 2]
    async with database() as db:
        rows = (await db.scalars(select(Entity))).all()
        assert {row.native_id for row in rows} == {"100", "300", "501", "502"}
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "comment"))
        assert scan.sweep_id == prior_sweep and scan.phase == "complete"
        assert (await db.scalar(select(SyncCursor))).cursor_data["canonical_cycle"][
            "phase"
        ] == "complete"


async def test_repository_loss_on_retry_hides_all_nested_originals(database, source):
    class LostAck(CanonicalCaptureService):
        async def commit_scan_page(self, db, request):
            result = await super().commit_scan_page(db, request)
            if request.scope.record_type == "comment":
                raise ConnectionError("synthetic interruption")
            return result

    native = NativeGitHub()
    first = await runner(database, source, native, service=LostAck(source[0].store))
    with pytest.raises(ConnectionError):
        await run(first)
    native.unavailable = True
    await run(await runner(database, source, native, attempt=2))
    async with database() as db:
        rows = (await db.scalars(select(Entity))).all()
        assert rows and all(row.deleted_at is not None for row in rows)
        assert all(row.removal_reason == "scope_removed" for row in rows)
        for row in rows:
            current = await source[0].store.read(
                db, source[1].organization_id, source[1].sync_id, row.id
            )
            assert (
                current.content_access == "unavailable"
                and current.payload == {}
                and not current.blobs
            )
        assert (await db.scalar(select(SyncCursor))).cursor_data["canonical_cycle"][
            "phase"
        ] == "complete"


async def test_code_scope_resumes_after_committed_blob_without_repeating_first_file(
    database, source
):
    class LostAck(CanonicalCaptureService):
        async def commit_scan_page(self, db, request):
            result = await super().commit_scan_page(db, request)
            if request.scope.record_type == "file":
                raise ConnectionError("synthetic file commit acknowledgment loss")
            return result

    native = NativeGitHub()
    native.code = True
    with pytest.raises(ConnectionError, match="acknowledgment"):
        await run(await runner(database, source, native, service=LostAck(source[0].store)))
    async with database() as db:
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "file"))
        assert scan.continuation["stack"][0]["offset"] == 1
        sweep = scan.sweep_id
    await run(await runner(database, source, native, attempt=2))
    assert len(native.blob_reads) == 2
    async with database() as db:
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "file"))
        assert scan.sweep_id == sweep and scan.phase == "complete"
        files = (
            await db.scalars(select(Entity).where(Entity.entity_definition_short_name == "file"))
        ).all()
        assert {item.native_id for item in files} == {"first.py", "second.py"}
