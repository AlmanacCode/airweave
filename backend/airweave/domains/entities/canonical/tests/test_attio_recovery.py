"""Actual Attio HTTP adapter and PostgreSQL page recovery with synthetic provider data."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import CompleteCycle
from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.platform.configs.config import AttioConfig
from airweave.platform.sources.attio import AttioSource

WORKSPACE, OBJECT, RECORD = [str(UUID(int=i)) for i in range(1, 4)]
OBJECT_DATA = {"id": {"workspace_id": WORKSPACE, "object_id": OBJECT}, "api_slug": "people"}
RECORD_DATA = {
    "id": {"workspace_id": WORKSPACE, "object_id": OBJECT, "record_id": RECORD},
    "values": {},
}
NOTES = [
    {
        "id": {"workspace_id": WORKSPACE, "note_id": str(UUID(int=i + 100))},
        "parent_object": "people",
        "parent_record_id": RECORD,
        "content_markdown": "Original note",
        "unknown": {"retained": True},
    }
    for i in range(51)
]


async def connector(calls, *, fail=False):
    def result(path, data, status=200):
        return httpx.Response(status, json=data, request=httpx.Request("GET", path))

    async def get(path, **kwargs):
        calls.append((path, kwargs.get("params")))
        if path.endswith("/self"):
            return result(path, {"active": True, "workspace_id": WORKSPACE})
        if path.endswith("/objects"):
            return result(path, {"data": [OBJECT_DATA]})
        if path.endswith("/lists"):
            return result(path, {"data": []})
        if path.endswith("/objects/" + OBJECT):
            return result(path, {"data": OBJECT_DATA})
        assert path.endswith("/notes")
        offset = kwargs["params"]["offset"]
        if fail and offset == 50:
            return result(path, {"code": "merge_in_progress"}, 404)
        return result(path, {"data": NOTES[offset : offset + 50]})

    async def post(path, **kwargs):
        calls.append((path, kwargs["json"]))
        assert path.endswith("/records/query")
        return result(path, {"data": [RECORD_DATA]})

    client = AsyncMock()
    client.get.side_effect = get
    client.post.side_effect = post
    return await AttioSource.create(
        auth=ManagedAuthProvider(
            api_key="fixture",
            connected_account_id="fixture",
            allowed_hosts=frozenset({"api.attio.com"}),
        ),
        logger=MagicMock(),
        http_client=client,
        config=AttioConfig(workspace_id=WORKSPACE),
    )


def driver(service, database, fence, connector):
    return CanonicalScanDriver(
        service, database, fence, connector, AsyncMock(), AsyncMock(), MagicMock()
    )


async def test_note_page_failure_recovers_exact_offset_without_replaying_fifty_notes(
    database, source
):
    service, fence = source
    calls = []
    first = await connector(calls, fail=True)
    with pytest.raises(SourceError):
        await driver(service, database, fence, first).run()
    async with database() as db:
        notes = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "note")
                )
            ).all()
        )
        assert len(notes) == 50
        assert all(row.deleted_at is None and row.record_revision == 1 for row in notes)
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "note"))
        assert scan.phase == "collecting" and scan.continuation["offset"] == 50
        before = await service.read_cycle(db, fence)
        assert before.phase == "active"
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    retry_calls = []
    resumed = await connector(retry_calls)
    after = await driver(service, database, newer, resumed).run()
    async with database() as db:
        after = await service.complete_cycle(db, CompleteCycle(fence=newer, expected=after.version))
    assert after.phase == "complete" and after.version.cycle_id == before.version.cycle_id
    assert after.last_full_capture.discovery == "incomplete"
    assert [params["offset"] for path, params in retry_calls if path.endswith("/notes")] == [50]
    # Discovery-only ancestors are refreshed on resume; the committed note page is not.
    assert any(path.endswith("/records/query") for path, _ in retry_calls)
    assert all(
        after.configuration.policy(kind) == "discovery_only"
        for kind in resumed.canonical_record_types
    )
    async with database() as db:
        notes = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "note")
                )
            ).all()
        )
        assert len(notes) == 51
        assert all(row.deleted_at is None and row.record_revision == 1 for row in notes)
        assert all(row.source_payload["unknown"] == {"retained": True} for row in notes)
