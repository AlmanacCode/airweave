"""Staged publisher resumes durable HTTP progress after lost responses."""

# ruff: noqa: F811
import httpx
import pytest
from evaluation.native_import import NativePublishError, publish_native
from sqlalchemy import func, select

from airweave.api import deps
from airweave.api.v1.endpoints.native_sources import router as source_router
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.native_ingestion.models import SessionVersion
from airweave.domains.native_ingestion.source_service import NativeSources
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.domains.native_ingestion.tests.test_import_api import native_api  # noqa: F401
from airweave.domains.native_ingestion.tests.test_ingestion import snapshot
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


class LostResponse(httpx.AsyncBaseTransport):
    def __init__(self, inner, suffix):
        self.inner = inner
        self.suffix = suffix
        self.dropped = False

    async def handle_async_request(self, request):
        response = await self.inner.handle_async_request(request)
        if not self.dropped and request.url.path.endswith(self.suffix):
            self.dropped = True
            await response.aclose()
            raise httpx.ReadError("synthetic lost response", request=request)
        return response

    async def aclose(self):
        await self.inner.aclose()


def wire_sources(client):
    app = client._transport.app
    app.include_router(source_router, prefix="/native/sources")
    container = app.dependency_overrides[deps.get_container]()
    container.native_sources = NativeSources(NativeSourceStore())
    app.dependency_overrides[deps.get_container] = lambda: container


@pytest.mark.parametrize("lost", ["/pages", "/scopes/reconcile", "/complete"])
async def test_lost_response_resume_and_changed_input_conflict(
    native_api, database, lost, monkeypatch
):
    client, _, source, _ = native_api
    wire_sources(client)
    client._transport = LostResponse(client._transport, lost)
    import evaluation.native_import as publisher

    monkeypatch.setattr(publisher, "_PAGE_SIZE", 2)
    originals = tuple(snapshot(str(index), owner_id="owner") for index in range(3))
    kwargs = {
        "owner_id": "owner",
        "dataset": "knowledge",
        "collection": "native-api",
        "request_key": "stable-publisher",
        "snapshots": originals,
    }
    with pytest.raises(httpx.ReadError):
        await publish_native(client, **kwargs)
    result = await publish_native(client, **kwargs)
    assert result.status == "completed" and result.summary.capture_complete
    assert await publish_native(client, **kwargs) == result
    with pytest.raises(NativePublishError, match="HTTP 409"):
        await publish_native(
            client, **{**kwargs, "snapshots": (snapshot("different", owner_id="owner"),)}
        )
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == source.sync_id)
            )
            == 3
        )
        records = (await db.scalars(select(Entity).where(Entity.sync_id == source.sync_id))).all()
        assert all(record.record_revision == 1 for record in records)
        assert result.summary.sequence == 3
        assert (
            await db.scalar(
                select(func.count()).select_from(SyncJob).where(SyncJob.sync_id == source.sync_id)
            )
            == 1
        )


@pytest.mark.parametrize("native_api", ["knowledge", "sessions"], indirect=True)
async def test_empty_dataset_and_empty_session(native_api):
    client, _, source, _ = native_api
    wire_sources(client)
    snapshots = ()
    if source.binding.dataset == "sessions":
        snapshots = (
            snapshot(
                "session",
                owner_id="owner",
                identity=RecordIdentity(record_type="session", native_id="session"),
                version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=0),
            ),
        )
    result = await publish_native(
        client,
        owner_id="owner",
        dataset=source.binding.dataset,
        collection="native-api",
        request_key="empty",
        snapshots=snapshots,
    )
    assert result.status == "completed"
    assert result.summary.completed_scopes == (2 if snapshots else 1)


async def test_preflight_rejects_mismatched_message_before_http():
    calls = []

    def response(request):
        calls.append(request)
        return httpx.Response(500)

    parent = snapshot(
        "session",
        identity=RecordIdentity(record_type="session", native_id="session"),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=2),
    )
    child = snapshot(
        "message",
        identity=RecordIdentity(record_type="message", native_id="message", container_id="session"),
        parent=parent.identity,
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=1),
    )
    async with httpx.AsyncClient(
        base_url="http://test/", transport=httpx.MockTransport(response)
    ) as client:
        with pytest.raises(NativePublishError, match="matching session"):
            await publish_native(
                client,
                owner_id=parent.owner_id,
                dataset="sessions",
                collection="test",
                request_key="invalid",
                snapshots=(parent, child),
            )
    assert calls == []
