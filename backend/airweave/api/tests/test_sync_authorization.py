"""An authenticated key cannot subscribe to another organization's job topics."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from airweave.api.v1.endpoints import sync


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", [sync.subscribe_sync_job, sync.subscribe_entity_state])
async def test_foreign_job_never_subscribes(monkeypatch, handler):
    owner, foreign_owner, job_id = uuid4(), uuid4(), uuid4()
    db = object()
    ctx = SimpleNamespace(organization=SimpleNamespace(id=owner))

    async def scoped_get(session, *, id, ctx):
        assert session is db
        assert id == job_id
        return SimpleNamespace(id=id) if ctx.organization.id == foreign_owner else None

    monkeypatch.setattr(sync.crud.sync_job, "get", scoped_get)
    pubsub = SimpleNamespace(subscribe=AsyncMock())
    with pytest.raises(HTTPException) as error:
        await handler(job_id=job_id, ctx=ctx, db=db, pubsub=pubsub)
    assert error.value.status_code == 404
    pubsub.subscribe.assert_not_awaited()


@pytest.mark.asyncio
async def test_authorized_job_passes_context_to_scoped_repository(monkeypatch):
    get = AsyncMock(return_value=object())
    monkeypatch.setattr(sync.crud.sync_job, "get", get)
    db, ctx, job_id = object(), object(), uuid4()
    await sync.authorize_job(db, ctx, job_id)
    get.assert_awaited_once_with(db, id=job_id, ctx=ctx)
