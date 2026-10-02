"""Actual Linear query adapter and SQL capture authority with synthetic provider responses."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.tests.test_capture_pipeline import components, orchestrator
from airweave.domains.entities.canonical.tests.test_slack_recovery import run, saved
from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.platform.configs.config import LinearConfig
from airweave.platform.sources.linear import LinearSource

WORKSPACE, TEAM, ISSUE, COMMENT, ATTACHMENT = (UUID(int=n) for n in range(1, 6))
STAMP = "2026-09-30T00:00:00Z"
ROOT = {
    "id": str(ISSUE),
    "team": {"id": str(TEAM)},
    "createdAt": STAMP,
    "updatedAt": STAMP,
    "archivedAt": STAMP,
    "trashed": True,
}
CHILD = {
    "id": str(COMMENT),
    "issue": {"id": str(ISSUE)},
    "createdAt": STAMP,
    "updatedAt": STAMP,
    "body": "retained original",
}


def connection(nodes, *, following=False, cursor=None):
    return {"nodes": nodes, "pageInfo": {"hasNextPage": following, "endCursor": cursor}}


def result(nodes, *, errors=None, following=False):
    return {
        "data": {"issues": connection(nodes, following=following)},
        **({"errors": errors} if errors else {}),
    }


def children(kind, nodes, *, following=False):
    return result(
        [
            {
                **ROOT,
                kind: connection(
                    nodes, following=following, cursor="child-next" if following else None
                ),
            }
        ]
    )


async def runner(database, source, responses, *, attempt=1):
    service, fence = source
    client = AsyncMock()
    envelopes = [
        {"data": {"organization": {"id": str(WORKSPACE)}, "viewer": {"id": str(UUID(int=9))}}},
        {"data": {"teams": connection([{"id": str(TEAM)}])}},
        *responses,
    ]
    client.post.side_effect = [
        httpx.Response(
            200, json=value, request=httpx.Request("POST", "https://api.linear.app/graphql")
        )
        for value in envelopes
    ]
    connector = await LinearSource.create(
        auth=StaticTokenProvider("fixture"),
        logger=MagicMock(),
        http_client=client,
        config=LinearConfig(workspace_id=WORKSPACE, team_ids=(TEAM,)),
    )
    ctx, _, runtime, bus = components(database, source)
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
        connector.canonical_container_parents,
        page_source=connector,
        files=MagicMock(),
    )
    runtime.source, runtime.canonical_capture = connector, pipeline
    instance = orchestrator(ctx, pipeline, runtime, None, bus)
    instance.stream = None
    return instance, client


async def assert_hidden(database, source, rows):
    assert all(row.removal_reason == "scope_removed" and row.deleted_at is not None for row in rows)
    async with database() as db:
        for row in rows:
            record = await source[0].store.read(
                db, source[1].organization_id, source[1].sync_id, row.id
            )
            assert record.content_access == "unavailable" and record.payload == {}


async def test_zero_outer_child_connection_withdraws_root_and_prior_child(database, source):
    instance, client = await runner(
        database, source, [result([ROOT]), children("comments", [CHILD]), result([])]
    )
    await run(instance)
    rows, cursor, _ = await saved(database)
    assert {row.native_id for row in rows} == {str(ISSUE), str(COMMENT)}
    await assert_hidden(database, source, rows)
    assert cursor["canonical_cycle"]["phase"] == "complete"
    child_queries = [call.kwargs["json"]["query"] for call in client.post.call_args_list[3:]]
    assert all("filter: {id: {eq: $issueId}}" in query for query in child_queries)


async def test_zero_exact_membership_reconciles_old_root_and_both_children(database, source):
    attachment = {**CHILD, "id": str(ATTACHMENT), "url": "https://external.example/link"}
    first, _ = await runner(
        database,
        source,
        [result([ROOT]), children("comments", [CHILD]), children("attachments", [attachment])],
    )
    await run(first, checkpoint=False)
    rows, _, _ = await saved(database)
    assert len(rows) == 3 and all(row.deleted_at is None for row in rows)
    # Archived and trashed fields are retained states, never deletion instructions.
    async with database() as db:
        original = await source[0].store.read(
            db,
            source[1].organization_id,
            source[1].sync_id,
            next(row.id for row in rows if row.native_id == str(ISSUE)),
        )
        assert original.payload["archivedAt"] == STAMP and original.payload["trashed"] is True
    second, _ = await runner(database, source, [result([]), result([])], attempt=2)
    await run(second)
    rows, cursor, _ = await saved(database)
    await assert_hidden(database, source, rows)
    assert cursor["canonical_cycle"]["phase"] == "complete"


@pytest.mark.parametrize(
    "invalid",
    [
        result([], errors=[{"extensions": {"type": "INTERNAL_SERVER_ERROR"}}]),
        {"data": {"issues": None}},
        result([], following=True),
        result([{**ROOT, "id": str(UUID(int=99))}]),
        result([ROOT, ROOT]),
        {"data": {"issues": {"nodes": []}}},
    ],
)
async def test_invalid_membership_keeps_visible_originals_and_saved_continuation(
    database, source, invalid
):
    instance, _ = await runner(
        database, source, [result([ROOT]), children("comments", [CHILD], following=True), invalid]
    )
    with pytest.raises((ValueError, SourceError)):
        await run(instance)
    rows, cursor, scans = await saved(database)
    assert {row.native_id for row in rows} == {str(ISSUE), str(COMMENT)}
    assert all(row.deleted_at is None and row.removal_reason is None for row in rows)
    assert "canonical_checkpoint" not in cursor
    child_scan = next(scan for scan in scans if scan.record_type == "comment")
    assert child_scan.phase == "collecting" and child_scan.continuation["after"] == "child-next"
    async with database() as db:
        for row in rows:
            original = await source[0].store.read(
                db, source[1].organization_id, source[1].sync_id, row.id
            )
            assert original.content_access == "available" and original.payload


@pytest.mark.parametrize(
    "confirmation",
    [
        result([ROOT]),
        result([], following=True),
        result([], errors=[{"extensions": {"type": "INTERNAL_SERVER_ERROR"}}]),
    ],
)
async def test_root_omission_cannot_hide_readable_or_unconfirmed_records(
    database, source, confirmation
):
    first, _ = await runner(
        database,
        source,
        [result([ROOT]), children("comments", [CHILD]), children("attachments", [])],
    )
    await run(first, checkpoint=False)
    second, _ = await runner(database, source, [result([]), confirmation], attempt=2)
    with pytest.raises((ValueError, SourceError)):
        await run(second)
    rows, cursor, _ = await saved(database)
    assert len(rows) == 2 and all(row.deleted_at is None for row in rows)
    assert "canonical_checkpoint" not in cursor
