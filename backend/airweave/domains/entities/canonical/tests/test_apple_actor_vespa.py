"""Disposable Vespa exact actor attribute contract; not relevance evaluation."""

import os
from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.actors import actor_token

pytestmark = pytest.mark.skipif(
    not os.environ.get("APPLE_ACTOR_VESPA_URL"), reason="requires disposable Apple Vespa"
)


async def test_actor_attribute_exact_role_and_raw_handle():
    origin = os.environ["APPLE_ACTOR_VESPA_URL"]
    identifier = "apple-actor-" + uuid4().hex
    path = f"{origin}/document/v1/test/base_entity/docid/{identifier}"
    handle = "+1 (415) 555-0100"
    sender = actor_token("sender", handle)
    async with httpx.AsyncClient(timeout=30) as http:
        try:
            response = await http.post(
                path,
                json={
                    "fields": {
                        "entity_id": identifier,
                        "name": "Synthetic actor fixture",
                        "airweave_system_metadata_actor_tokens": [sender],
                    }
                },
            )
            assert response.status_code == 200, response.text
            for token, expected in (
                (sender, 1),
                (actor_token("current_chat_member", handle), 0),
                (actor_token("sender", "+14155550100"), 0),
            ):
                result = await http.post(
                    f"{origin}/search/",
                    json={
                        "yql": "select entity_id from base_entity where entity_id contains @id "
                        "and airweave_system_metadata_actor_tokens contains @actor",
                        "id": identifier,
                        "actor": token,
                        "ranking": "unranked",
                    },
                )
                assert result.status_code == 200, result.text
                body = result.json()
                assert not body["root"].get("errors"), body
                assert body["root"]["fields"]["totalCount"] == expected, body
        finally:
            await http.delete(path)
