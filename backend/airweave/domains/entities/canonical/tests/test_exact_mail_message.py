"""Exact native Gmail identity reads use canonical SQL scope and current authority."""

from types import SimpleNamespace
from uuid import uuid4

from httpx import ASGITransport, AsyncClient
from sqlalchemy import update

from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.domains.entities.canonical.tests.test_mail_query import message
from airweave.models.source_connection import SourceConnection


async def test_exact_message_http_denies_foreign_source_and_withdrawal(database, source):
    writer, fence = source
    await bind_projection(database, fence)
    await capture(database, writer, fence, message("one"), message("two"))
    owner = fence.organization_id
    app = query_app(database, lambda: SimpleNamespace(organization=SimpleNamespace(id=owner)))
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        path = f"/sync/{fence.sync_id}/mail/messages/one"
        result = await client.get(path)
        assert result.status_code == 200, result.text
        original = result.json()
        assert original["identity"]["native_id"] == "one"
        assert original["payload"]["id"] == "one"
        assert (await client.get(path.replace("one", "missing"))).status_code == 404
        assert (await client.get(path.replace(str(fence.sync_id), str(uuid4())))).status_code == 404
        owner = uuid4()
        assert (await client.get(path)).status_code == 404
        owner = fence.organization_id
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        assert (await client.get(path)).status_code == 404
