"""Real SQL observed thread membership, exact numeric order and authority fences."""

from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import update

from airweave.domains.entities.canonical import slack_query
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.slack_models import SlackThreadQuery
from airweave.domains.entities.canonical.slack_query import CanonicalSlackQuery
from airweave.domains.entities.canonical.store import SourceNotFound
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync

CHANNEL = RecordIdentity(record_type="channel", native_id="C123")
ROOT = "9.000001"


def message(ts, thread=ROOT, *, channel=CHANNEL, payload=None):
    return observation(
        identity=RecordIdentity(
            record_type="message", native_id=ts, container_id=channel.native_id
        ),
        parent=channel,
        payload={"ts": ts, "thread_ts": thread, "text": "Observed " + ts, **(payload or {})},
    )


async def read(database, fence, **options):
    async with database() as db:
        return await CanonicalSlackQuery("slack-test-key").thread(
            db,
            fence.organization_id,
            fence.sync_id,
            SlackThreadQuery(channel="C123", thread_ts=ROOT, **options),
        )


async def seed(database, service, fence):
    await bind_projection(database, fence, "slack")
    await capture(database, service, fence, observation(identity=CHANNEL, payload={"id": "C123"}))


async def test_observed_thread_pages_exact_numeric_order_files_root_missing_and_channel_gate(
    database, source
):
    service, fence = source
    await seed(database, service, fence)
    timestamps = [f"10.{index:024}" for index in range(1, 32)] + ["10.1", "10.10"]
    other_channel = RecordIdentity(record_type="channel", native_id="C999")
    await capture(
        database, service, fence, observation(identity=other_channel, payload={"id": "C999"})
    )
    records = [message(ts) for ts in reversed(timestamps)]
    records.append(
        message(
            timestamps[0], channel=other_channel, payload={"text": "OTHER CHANNEL NEVER RETURNED"}
        )
    )
    records += [
        message("11.2", thread="11.2"),
        message("malformed"),
        message("12.0", payload={"ts": "mismatch"}),
    ]
    await capture(database, service, fence, *records)
    parent = records[0]
    await capture(
        database,
        service,
        fence,
        observation(
            identity=RecordIdentity(
                record_type="file", native_id="F123", container_id=parent.identity.native_id
            ),
            parent=parent.identity,
            payload={"id": "F123", "name": "retained.txt"},
        ),
    )
    first = await read(database, fence)
    assert not first.root_present and first.metadata_missing == 2
    # Synthetic capture has no declared full-scan evidence.
    assert first.coverage == "stored_messages_only" and first.capture is None
    seen, cursor = [], None
    while True:
        page = await read(database, fence, cursor=cursor)
        seen.extend(page.messages)
        if not page.has_more:
            break
        assert page.next_cursor and page.next_cursor != cursor
        cursor = page.next_cursor
    assert len(seen) == len({item.id for item in seen}) == 33
    assert [(Decimal(item.ts), item.id) for item in seen] == sorted(
        (Decimal(item.ts), item.id) for item in seen
    )
    assert [item.ts for item in seen[:2]] == timestamps[:2]  # These values collapse as floats.
    owner = next(item for item in seen if item.ts == parent.identity.native_id)
    assert len(owner.captured_files) == 1 and owner.captured_files[0].native_id == "F123"
    async with database() as db:
        await db.execute(update(Sync).where(Sync.id == fence.sync_id).values(status="paused"))
        await db.commit()
    assert (await read(database, fence)).messages
    await capture(
        database,
        service,
        fence,
        observation(identity=CHANNEL, kind="delete", removal_reason="access_revoked", payload={}),
    )
    with pytest.raises(SourceNotFound, match="channel is unavailable"):
        await read(database, fence, cursor=first.next_cursor)


async def test_http_tenant_cursor_mutation_malformed_and_post_read_revocation(
    database, source, monkeypatch
):
    service, fence = source
    await seed(database, service, fence)
    await capture(database, service, fence, message(ROOT), message("10.1"))
    org = [fence.organization_id]
    app = query_app(database, lambda: SimpleNamespace(organization=SimpleNamespace(id=org[0])))
    base = f"/sync/{fence.sync_id}/slack/threads/{ROOT}"
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        first = await client.get(base, params={"channel": "C123", "limit": 1})
        assert first.status_code == 200, first.text
        body = first.json()
        cursor = body["next_cursor"]
        assert body["root_present"] and body["messages"][0]["ts"] == ROOT
        assert first.headers["cache-control"] == "private, no-store"
        assert (
            await client.get(base, params={"channel": "C123", "limit": 1, "cursor": cursor})
        ).json()["messages"][0]["ts"] == "10.1"
        assert (
            await client.get(base, params={"channel": "C123", "limit": 2, "cursor": cursor})
        ).status_code == 400
        assert (
            await client.get(base, params={"channel": "C123", "cursor": "bad"})
        ).status_code == 400
        assert (
            await client.get(base.replace(ROOT, "nan"), params={"channel": "C123"})
        ).status_code == 422
        org[0] = uuid4()
        assert (await client.get(base, params={"channel": "C123"})).status_code == 404
        org[0] = fence.organization_id
        await capture(database, service, fence, message("12.1"))
        assert (
            await client.get(base, params={"channel": "C123", "limit": 1, "cursor": cursor})
        ).status_code == 409
        await capture(
            database, service, fence, message("13.1", payload={"text": {"invalid": True}})
        )
        malformed = await client.get(base, params={"channel": "C123"})
        assert malformed.status_code == 409 and "Observed" not in malformed.text
        original = slack_query.capture_coverage

        async def revoked(*args):
            result = await original(*args)
            async with database() as db:
                await db.execute(
                    update(SourceConnection)
                    .where(SourceConnection.sync_id == fence.sync_id)
                    .values(is_authenticated=False)
                )
                await db.commit()
            return result

        monkeypatch.setattr(slack_query, "capture_coverage", revoked)
        revoked_result = await client.get(base, params={"channel": "C123", "limit": 1})
        assert revoked_result.status_code == 404 and "Observed" not in revoked_result.text
