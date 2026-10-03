"""Real SQL/HTTP retained Drive metadata, without providers or index publications."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import update

from airweave.domains.entities.canonical.drive_models import DriveFilters, DriveListQuery
from airweave.domains.entities.canonical.drive_query import CanonicalDriveQuery, DriveChanged
from airweave.domains.entities.canonical.query import InvalidRecordCursor
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.models.source_connection import SourceConnection

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def file(native_id, *, name="Saved file", updated=NOW, **payload):
    return observation(
        identity=RecordIdentity(record_type="file", native_id=native_id),
        payload={
            "id": native_id,
            "name": name,
            "mimeType": "application/pdf",
            "parents": ["folder"],
            "driveId": "shared",
            "trashed": False,
            "size": "123",
            **payload,
        },
        source_updated_at=updated,
        completeness="metadata_only",
    )


async def listing(database, fence, **options):
    async with database() as db:
        return await CanonicalDriveQuery("drive-test-key").files(
            db, fence.organization_id, fence.sync_id, DriveListQuery(**options)
        )


async def test_filters_before_limit_full_inventory_without_projection(database, source):
    service, fence = source
    await bind_projection(database, fence, "google_drive")
    records = [file(f"match{index}", name=f"Quote %_ {index:03}") for index in range(160)]
    records += [file(f"other{index}", name="Other", parents=["folder-two"]) for index in range(120)]
    records += [file("unknown", mimeType=None)]
    await capture(database, service, fence, *records)
    filters = DriveFilters(
        folder="folder", drive="shared", name="QUOTE %_", mime_type="application/pdf"
    )
    page = await listing(database, fence, filters=filters, limit=17)
    items = list(page.files)
    while page.next_cursor:
        page = await listing(database, fence, filters=filters, limit=17, cursor=page.next_cursor)
        items.extend(page.files)
    assert len(items) == len({item.id for item in items}) == 160
    assert [item.name for item in items] == sorted(item.name for item in items)
    assert all(item.completeness == "metadata_only" and item.extraction is None for item in items)
    assert page.metadata_missing == 1
    assert not page.has_more


async def test_order_ties_nulls_cursor_switch_and_capture_change(database, source):
    service, fence = source
    await bind_projection(database, fence, "google_drive")
    await capture(
        database,
        service,
        fence,
        file("one", updated=NOW + timedelta(days=1)),
        file("two", updated=NOW + timedelta(days=1)),
        file("three", updated=NOW),
        file("four", updated=None),
    )
    filters = DriveFilters(sort="updated")
    names = await listing(database, fence, limit=100)
    assert [item.id for item in names.files] == sorted(item.id for item in names.files)
    page = await listing(database, fence, filters=filters, limit=1)
    items = list(page.files)
    first_cursor = page.next_cursor
    while page.next_cursor:
        page = await listing(database, fence, filters=filters, limit=1, cursor=page.next_cursor)
        items.extend(page.files)
    assert [item.source_updated_at for item in items] == [NOW + timedelta(days=1)] * 2 + [NOW, None]
    assert items[0].id < items[1].id
    with pytest.raises(InvalidRecordCursor):
        await listing(database, fence, filters=DriveFilters(sort="name"), cursor=first_cursor)
    await capture(database, service, fence, file("five"))
    with pytest.raises(DriveChanged):
        await listing(database, fence, filters=filters, cursor=first_cursor)


async def test_http_exact_native_file_wrong_scope_and_disconnect(database, source):
    service, fence = source
    await bind_projection(database, fence, "google_drive")
    await capture(database, service, fence, file("native-file"))
    context = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))
    app = query_app(database, lambda: context)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        path = f"/sync/{fence.sync_id}/drive/files/native-file"
        result = await client.get(path)
        assert result.status_code == 200, result.text
        assert result.json()["file"]["native_id"] == "native-file"
        assert result.json()["file"]["extraction"] is None
        assert (await client.get(path.replace(str(fence.sync_id), str(uuid4())))).status_code == 404
        context.organization.id = uuid4()
        assert (await client.get(path)).status_code == 404
        context.organization.id = fence.organization_id
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        denied = await client.get(path)
        assert denied.status_code == 404
        assert denied.json()["error"]["code"] == "source_not_found"


@pytest.mark.parametrize("sort", ["name", "updated"])
async def test_modified_interval_boundaries_unknowns_and_cursor_binding(database, source, sort):
    service, fence = source
    await bind_projection(database, fence, "google_drive")
    await capture(
        database,
        service,
        fence,
        file("before", name="A before", updated=NOW - timedelta(microseconds=1)),
        file("start", name="B start", updated=NOW),
        file("inside", name="C inside", updated=NOW + timedelta(microseconds=1)),
        file("end", name="D end", updated=NOW + timedelta(days=1)),
        file("unknown", name="A unknown", updated=None),
    )
    filters = DriveFilters(sort=sort, updated_after=NOW, updated_before=NOW + timedelta(days=1))
    page = await listing(database, fence, filters=filters, limit=1)
    assert page.has_more
    items = list(page.files)
    first_cursor = page.next_cursor
    while page.next_cursor:
        page = await listing(database, fence, filters=filters, limit=1, cursor=page.next_cursor)
        items.extend(page.files)
    assert {item.native_id for item in items} == {"start", "inside"}
    for field, value in (
        ("updated_after", NOW - timedelta(seconds=1)),
        ("updated_before", NOW + timedelta(days=2)),
    ):
        with pytest.raises(InvalidRecordCursor):
            await listing(
                database,
                fence,
                filters=filters.model_copy(update={field: value}),
                limit=1,
                cursor=first_cursor,
            )
    assert {
        item.native_id
        for item in (
            await listing(
                database, fence, filters=DriveFilters(updated_before=NOW + timedelta(days=1))
            )
        ).files
    } == {"before", "start", "inside"}
    assert {
        item.native_id
        for item in (await listing(database, fence, filters=DriveFilters(updated_after=NOW))).files
    } == {"start", "inside", "end"}


async def test_http_modified_bounds_and_invalid_intervals(database, source):
    service, fence = source
    await bind_projection(database, fence, "google_drive")
    await capture(database, service, fence, file("start"), file("unknown", updated=None))
    context = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))
    async with AsyncClient(
        transport=ASGITransport(app=query_app(database, lambda: context)), base_url="http://test"
    ) as client:
        path = f"/sync/{fence.sync_id}/drive/files"
        response = await client.get(
            path,
            params={
                "updated_after": "2025-12-31T19:00:00-05:00",
                "updated_before": "2026-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200, response.text
        assert [item["native_id"] for item in response.json()["files"]] == ["start"]
        for bounds in (
            {"updated_after": "2026-01-01"},
            {"updated_after": "2026-01-01T00:00:00Z", "updated_before": "2026-01-01T00:00:00Z"},
        ):
            assert (await client.get(path, params=bounds)).status_code == 422
