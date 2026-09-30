"""Read-only search cancellation on real TCP, with explicit fixture dependencies."""

import asyncio
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Header, HTTPException

from airweave.api import deps
from airweave.api.v1.endpoints.records import router
from airweave.db.session import get_db
from airweave.domains.search.owned_models import OwnedSearchResponse

BODY = {"query": "synthetic", "sync_ids": [str(uuid4())], "mode": "keyword"}
HEADERS = {"X-API-Key": "fixture-only"}


def response():
    return OwnedSearchResponse(
        items=(),
        sources=(),
        candidate_window_full=False,
        engine_partial=False,
        excluded_candidates=0,
        postfilter_excluded=0,
        retrieval_incomplete=False,
    )


def app_for(search, released):
    app = FastAPI()
    app.include_router(router, prefix="/sync")

    async def database():
        try:
            yield object()
        finally:
            released.set()

    async def context(x_api_key: str = Header()):
        if x_api_key != "fixture-only":
            raise HTTPException(401, "Invalid fixture key")
        return SimpleNamespace(organization=SimpleNamespace(id=uuid4()))

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[deps.get_context] = context
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(
        owned_search=SimpleNamespace(search=search)
    )
    return app


@pytest.mark.asyncio
async def test_search_post_preserves_response_errors_and_auth_without_disconnect_wait():
    released = asyncio.Event()
    search = AsyncMock(return_value=response())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(search, released)), base_url="http://test"
    ) as client:
        async with asyncio.timeout(3):
            good = await client.post("/sync/search", json=BODY, headers=HEADERS)
            assert good.status_code == 200 and good.json()["items"] == []
            assert released.is_set()
            search.side_effect = HTTPException(409, {"code": "reindex_required"})
            failed = await client.post("/sync/search", json=BODY, headers=HEADERS)
            assert failed.status_code == 409
            assert failed.json()["detail"]["code"] == "reindex_required"
            before = search.await_count
            denied = await client.post("/sync/search", json=BODY, headers={"X-API-Key": "wrong"})
            assert denied.status_code == 401 and search.await_count == before


@pytest.mark.asyncio
async def test_tcp_search_disconnect_joins_work_before_database_dependency_closes():
    entered, cancelled, released, unblock = (asyncio.Event() for _ in range(4))

    async def search(db, ctx, request):
        assert request.query == BODY["query"]  # Parsed POST body survives the listener.
        entered.set()
        try:
            await unblock.wait()
            return response()
        except asyncio.CancelledError:
            assert not released.is_set()
            cancelled.set()
            raise

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    server = uvicorn.Server(
        uvicorn.Config(app_for(search, released), lifespan="off", log_config=None, access_log=False)
    )
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.001)
        async with httpx.AsyncClient(timeout=None, trust_env=False) as client:
            task = asyncio.create_task(
                client.post(
                    f"http://127.0.0.1:{listener.getsockname()[1]}/sync/search",
                    json=BODY,
                    headers=HEADERS,
                )
            )
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                await asyncio.wait_for(cancelled.wait(), timeout=2)
                await asyncio.wait_for(released.wait(), timeout=2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        unblock.set()
        server.should_exit = True
        try:
            await asyncio.wait_for(serving, timeout=5)
        finally:
            listener.close()


@pytest.mark.asyncio
async def test_completed_search_error_wins_simultaneous_disconnect():
    from airweave.api.v1.endpoints.records import search_records
    from airweave.domains.search.owned_models import OwnedSearchRequest

    error = HTTPException(409, {"code": "reindex_required"})
    container = SimpleNamespace(owned_search=SimpleNamespace(search=AsyncMock(side_effect=error)))
    request = SimpleNamespace(receive=AsyncMock(return_value={"type": "http.disconnect"}))
    with pytest.raises(HTTPException) as caught:
        await search_records(OwnedSearchRequest(**BODY), request, object(), object(), container)
    assert caught.value is error


@pytest.mark.asyncio
async def test_caller_cancellation_joins_search_and_disconnect_listener():
    from airweave.api.v1.endpoints.records import search_records
    from airweave.domains.search.owned_models import OwnedSearchRequest

    entered = asyncio.Event()
    listener_entered = asyncio.Event()
    stopped = set()

    async def blocked(name, ready):
        ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.add(name)

    container = SimpleNamespace(
        owned_search=SimpleNamespace(search=lambda *args: blocked("search", entered))
    )
    request = SimpleNamespace(receive=lambda: blocked("listener", listener_entered))
    task = asyncio.create_task(
        search_records(OwnedSearchRequest(**BODY), request, object(), object(), container)
    )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.wait_for(listener_entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped == {"search", "listener"}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
