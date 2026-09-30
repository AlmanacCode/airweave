"""Attio search preserves original identity rather than joining CRM and membership."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import (
    ProjectionMappingError,
    map_record,
)
from airweave.domains.entities.canonical.requests import RecordIdentity

WORKSPACE, OBJECT, RECORD, LIST, ENTRY, NOTE = (str(uuid4()) for _ in range(6))


def original(kind, native_id, payload, parent=None):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(
            record_type=kind, native_id=native_id, container_id=parent.native_id if parent else None
        ),
        parent=parent,
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="hash",
        content_hash=None,
        completeness="complete",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


def entry():
    return original(
        "entry",
        ENTRY,
        {
            "id": {"workspace_id": WORKSPACE, "list_id": LIST, "entry_id": ENTRY},
            "parent_record_id": RECORD,
            "parent_object": "people",
            "entry_values": {"status": [{"status": {"title": "Interested"}}]},
            "unknown_native": "must survive",
        },
        RecordIdentity(record_type="list", native_id=LIST),
    )


@pytest.mark.asyncio
async def test_membership_search_keeps_entry_identity_and_never_fetches_record():
    captured = entry()
    before = captured.model_dump()
    storage = AsyncMock()
    async with map_record(captured, "attio", storage) as entities:
        entities = entities.entities
        assert len(entities) == 1
        result = entities[0]
        assert result.native_id == ENTRY
        assert result.original_kind == "entry"
        assert RECORD in result.text and "Interested" in result.text
        assert result.web_url == ""
        assert "separate" in result.content_coverage
    assert captured.model_dump() == before
    assert storage.mock_calls == []


@pytest.mark.asyncio
async def test_note_uses_current_plaintext_field_and_its_record_parent():
    captured = original(
        "note",
        NOTE,
        {
            "id": {"workspace_id": WORKSPACE, "note_id": NOTE},
            "title": "Call",
            "parent_record_id": RECORD,
            "parent_object": "people",
            "content_plaintext": "Discussed fundraising",
            "content_markdown": "**Discussed** fundraising",
        },
        RecordIdentity(record_type="record", native_id=RECORD, container_id=OBJECT),
    )
    async with map_record(captured, "attio", AsyncMock()) as entities:
        entities = entities.entities
        assert entities[0].text == "Discussed fundraising"
        assert entities[0].title == "Call"


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["entry_id", "list_id", "parent", "unavailable"])
async def test_mismatched_or_unavailable_membership_never_projects(corruption):
    captured = entry()
    if corruption in {"entry_id", "list_id"}:
        payload = captured.model_dump()["payload"]
        payload["id"][corruption] = str(uuid4())
        captured = captured.model_copy(update={"payload": payload})
    elif corruption == "parent":
        captured = captured.model_copy(update={"parent": None})
    else:
        captured = captured.model_copy(update={"content_access": "unavailable"})
    with pytest.raises(ProjectionMappingError):
        async with map_record(captured, "attio", AsyncMock()):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url", ["https://app.attio.com/workspace/person/record", "https://evil.example/record"]
)
async def test_record_attributes_and_native_url(url):
    captured = original(
        "record",
        RECORD,
        {
            "id": {"workspace_id": WORKSPACE, "object_id": OBJECT, "record_id": RECORD},
            "values": {"name": [{"full_name": "Sam Example"}], "custom_score": [{"value": 12}]},
            "web_url": url,
        },
        RecordIdentity(record_type="object", native_id=OBJECT),
    )
    if "evil" in url:
        with pytest.raises(ProjectionMappingError):
            async with map_record(captured, "attio", AsyncMock()):
                pass
    else:
        async with map_record(captured, "attio", AsyncMock()) as entities:
            entities = entities.entities
            assert entities[0].title == "Sam Example"
            assert "custom_score" in entities[0].text
            assert entities[0].web_url == url


@pytest.mark.asyncio
async def test_two_memberships_for_one_crm_record_remain_distinct():
    first = entry()
    second_entry, second_list = str(uuid4()), str(uuid4())
    payload = first.model_dump()["payload"]
    payload["id"].update(entry_id=second_entry, list_id=second_list)
    second = original(
        "entry",
        second_entry,
        payload,
        RecordIdentity(record_type="list", native_id=second_list),
    )
    async with map_record(first, "attio", AsyncMock()) as one:
        one = one.entities
        async with map_record(second, "attio", AsyncMock()) as two:
            two = two.entities
            assert one[0].native_id != two[0].native_id
            assert RECORD in one[0].text and RECORD in two[0].text
            assert one[0].original_kind == two[0].original_kind == "entry"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["object", "list"])
async def test_container_projection_does_not_invent_children(kind):
    native_id = OBJECT if kind == "object" else LIST
    payload = {"id": {"workspace_id": WORKSPACE, kind + "_id": native_id}}
    if kind == "object":
        payload.update(singular_noun="Person", plural_noun="People", api_slug="people")
    else:
        payload.update(name="Fundraising")
    captured = original(kind, native_id, payload)
    async with map_record(captured, "attio", AsyncMock()) as entities:
        entities = entities.entities
        assert len(entities) == 1
        assert "metadata only" in entities[0].content_coverage
        assert entities[0].web_url == ""
