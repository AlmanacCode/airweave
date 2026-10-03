"""Exact actor terms, actual PostgreSQL JSON subfields and Vespa filter compilation."""

import json
from uuid import uuid4

import pytest
from sqlalchemy import cast, literal, select
from sqlalchemy.dialects.postgresql import JSONB

from airweave.domains.entities.canonical.actors import (
    ActorFilter,
    ActorHandles,
    actor_sql_columns,
    original_actor_handles,
)
from airweave.domains.entities.canonical.apple_payload_tests.test_payloads import swift_payload
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.platform.destinations.vespa.filter_translator import FilterTranslator


def test_role_and_raw_spelling_are_exact():
    tokens = {
        ActorFilter(role=role, handle=value).token
        for role in ("sender", "current_chat_member", "contact_handle")
        for value in (
            "A@example.com",
            "a@example.com",
            " a@example.com",
            "+14155550100",
            "4155550100",
        )
    }
    assert len(tokens) == 15
    assert all(len(token) == 64 for token in tokens)
    actors = ActorHandles(sender_handle="A@example.com", current_chat_handles=("other",))
    assert actors.matches(ActorFilter(role="sender", handle="A@example.com"))
    assert not actors.matches(ActorFilter(role="current_chat_member", handle="A@example.com"))


def test_contacts_do_not_resolve_names_or_merge_native_identity():
    original = swift_payload("contact")
    actors = original_actor_handles("apple_contacts", original)
    expected = [
        item["rawValue"] for group in ("phones", "emails") for item in original["contact"][group]
    ]
    assert list(actors.contact_handles) == expected
    assert not actors.matches(ActorFilter(role="contact_handle", handle="Synthetic"))
    assert not original_actor_handles("apple_notes", {}).tokens


def test_actor_prefilter_and_real_vespa_compiler():
    actor = ActorFilter(role="sender", handle="danger' OR true")
    request = OwnedSearchRequest(query="hello", sync_ids=(uuid4(),), actor_filters=(actor,))
    conditions = OwnedSearchService._prefilters(request, list(request.sync_ids))
    assert conditions[-1].value == actor.token
    yql = FilterTranslator().translate(
        {"must": [{"key": conditions[-1].field, "match": {"value": conditions[-1].value}}]}
    )
    assert yql == f'(airweave_system_metadata_actor_tokens contains "{actor.token}")'
    assert "danger" not in yql
    with pytest.raises(ValueError, match="unique"):
        OwnedSearchRequest(query="hello", sync_ids=(uuid4(),), actor_filters=(actor, actor))


@pytest.mark.asyncio
async def test_actual_sql_selects_only_actor_subfields(database):
    original = {
        "sender": {"fields": {"id": {"text": {"_0": "Sender"}}}},
        "participants": [{"fields": {"id": {"text": {"_0": "Member"}}}}],
        "contact": {
            "phones": [{"rawValue": "+1 415"}],
            "emails": [{"rawValue": "Case@EXAMPLE"}],
            "givenName": "Not a handle",
        },
        "message": {"secret": "not selected"},
    }
    payload = cast(
        literal(
            json.dumps(
                {
                    "authority": "device",
                    "source_kind": "imessage",
                    "account_id": "synthetic",
                    "original": original,
                }
            )
        ),
        JSONB,
    )
    statement = select(*actor_sql_columns(payload))
    async with database() as db:
        row = (await db.execute(statement)).mappings().one()
    actors = ActorHandles.model_validate(row)
    assert actors.sender_handle == "Sender"
    assert actors.current_chat_handles == ("Member",)
    assert set(actors.contact_handles) == {"+1 415", "Case@EXAMPLE"}
    assert "secret" not in dict(row)


def test_postfilter_checks_current_raw_observation_and_source():
    from types import SimpleNamespace

    row = SimpleNamespace(
        entity_definition_short_name="imessage_message",
        native_type=None,
        actor_source="imessage",
        sender_handle="Original",
        current_chat_handles=("Current",),
        contact_handles=(),
        source_created_at=None,
        source_updated_at=None,
    )
    scope = (uuid4(),)
    request = OwnedSearchRequest(
        query="hello",
        sync_ids=scope,
        actor_filters=(ActorFilter(role="sender", handle="Original"),),
    )
    assert OwnedSearchService._matches(row, request)
    row.sender_handle = "Changed"
    assert not OwnedSearchService._matches(row, request)
    row.sender_handle = "Original"
    row.actor_source = "apple_notes"
    assert not OwnedSearchService._matches(row, request)


def test_push_capability_never_calls_pull_registry():
    from unittest.mock import MagicMock

    from airweave.domains.device_ingestion.models import DeviceBinding
    from airweave.domains.entities.canonical.source import indexed_record_types

    registry = MagicMock()
    registry.get.side_effect = AssertionError("Push sources must not resolve pull provider")
    for kind in ("imessage", "apple_notes", "apple_contacts"):
        binding = DeviceBinding(owner_id="owner", account_id="account", source_kind=kind)
        assert indexed_record_types(kind, registry) == (binding.record_type,)


@pytest.mark.asyncio
@pytest.mark.parametrize("any_of", [False, True])
@pytest.mark.parametrize("match", ["raw", "endpoint"])
@pytest.mark.parametrize("source,version,status", [("apple_notes", 4, 422), ("imessage", 4, 409)])
async def test_actor_scope_failure_precedes_any_index_or_model_call(
    source, version, status, any_of, match
):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from fastapi import HTTPException

    @asynccontextmanager
    async def sessions():
        yield object()

    sync = uuid4()
    executor = MagicMock()
    executor.prepare_query = AsyncMock(side_effect=AssertionError("Index/embedding called"))
    service = OwnedSearchService(executor, MagicMock())
    service._resolve_scopes = AsyncMock(
        return_value=(
            {sync: SimpleNamespace(short_name=source, index_pipeline_version=version)},
            {},
        )
    )
    from airweave.domains.entities.canonical.actors import ActorAnyOf

    request = OwnedSearchRequest(
        query="hello",
        sync_ids=(sync,),
        actor_filters=() if any_of else (ActorFilter(role="sender", handle="Exact"),),
        actor_any_of=ActorAnyOf(role="sender", handles=("Exact", "Other"), match=match)
        if any_of
        else None,
    )
    with pytest.raises(HTTPException) as error:
        await service._retrieve(sessions, object(), request)
    assert error.value.status_code == status
    executor.prepare_query.assert_not_called()


def test_committed_native_actor_tokens_survive_vespa_feed():
    from datetime import datetime, timezone

    from airweave.domains.entities.canonical.models import SourceRecord
    from airweave.domains.entities.canonical.requests import RecordIdentity
    from airweave.domains.entities.canonical.search_metadata import stamp_search_metadata
    from airweave.platform.destinations.vespa.transformer import EntityTransformer
    from airweave.platform.entities._base import AirweaveSystemMetadata
    from airweave.platform.entities.apple import AppleRecordEntity

    original = swift_payload("contact")
    record = SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(
            record_type="apple_contact", native_id=original["contact"]["nativeID"]
        ),
        revision=1,
        payload={
            "authority": "device",
            "source_kind": "apple_contacts",
            "account_id": "synthetic",
            "original": original,
        },
        payload_schema_version=1,
        capture_hash="fixture",
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
    entity = AppleRecordEntity(
        native_id=record.identity.native_id, title="Contact", content="Name", breadcrumbs=[]
    )
    entity.airweave_system_metadata = AirweaveSystemMetadata(source_name="apple_contacts")
    stamp_search_metadata(entity.airweave_system_metadata, record)
    terms = EntityTransformer()._build_system_metadata(entity)["actor_tokens"]
    assert terms == list(original_actor_handles("apple_contacts", original).tokens)
    assert terms


def test_actor_any_of_contract_and_combined_budget():
    from airweave.domains.entities.canonical.actors import ActorAnyOf

    group = ActorAnyOf(role="sender", handles=("Case@EXAMPLE", "case@example"))
    assert group.match == "raw"
    with pytest.raises(ValueError):
        ActorAnyOf(role="sender", handles=("Exact",), match="fuzzy")
    assert len(set(group.tokens)) == 2
    for handles in (
        (),
        ("same", "same"),
        (1,),
        ("",),
        ("x" * 1025,),
        tuple(str(n) for n in range(21)),
    ):
        with pytest.raises(ValueError):
            ActorAnyOf(role="sender", handles=handles)
    singles = tuple(ActorFilter(role="sender", handle=str(n)) for n in range(19))
    with pytest.raises(ValueError, match="Combined"):
        OwnedSearchRequest(
            query="hello", sync_ids=(uuid4(),), actor_filters=singles, actor_any_of=group
        )
    assert OwnedSearchRequest(
        query="hello", sync_ids=(uuid4(),), actor_filters=singles[:18], actor_any_of=group
    )


def test_actor_any_of_prefilter_and_current_postfilter_preserve_and_semantics():
    from types import SimpleNamespace

    from airweave.core.logging import logger
    from airweave.domains.entities.canonical.actors import ActorAnyOf
    from airweave.domains.search.adapters.vector_db.filter_translator import (
        FilterTranslator as PlanTranslator,
    )
    from airweave.domains.search.types.filters import FilterGroup

    group = ActorAnyOf(role="sender", handles=("First", "Second"))
    single = ActorFilter(role="current_chat_member", handle="Member")
    request = OwnedSearchRequest(
        query="hello", sync_ids=(uuid4(),), actor_filters=(single,), actor_any_of=group
    )
    conditions = OwnedSearchService._prefilters(request, list(request.sync_ids))
    assert conditions[-1].operator == "in" and conditions[-1].value == list(group.tokens)
    compiled = PlanTranslator(logger=logger).translate([FilterGroup(conditions=conditions)])
    assert " or " in compiled.lower() and single.token in compiled
    assert all(token in compiled for token in group.tokens)
    assert "First" not in compiled and "Second" not in compiled
    row = SimpleNamespace(
        entity_definition_short_name="imessage_message",
        native_type=None,
        actor_source="imessage",
        sender_handle="Second",
        current_chat_handles=("Member",),
        contact_handles=(),
        source_created_at=None,
        source_updated_at=None,
    )
    assert OwnedSearchService._matches(row, request)
    row.current_chat_handles = ()
    assert not OwnedSearchService._matches(row, request)
    row.current_chat_handles = ("Member",)
    row.sender_handle = "second"
    assert not OwnedSearchService._matches(row, request)
    row.sender_handle = "Second"
    row.actor_source = "apple_contacts"
    assert not OwnedSearchService._matches(row, request)


@pytest.mark.asyncio
async def test_actor_any_of_reads_only_current_sql_handles(database):
    from airweave.domains.entities.canonical.actors import ActorAnyOf

    original = {
        "sender": {"fields": {"id": {"text": {"_0": "Current"}}}},
        "participants": [{"fields": {"id": {"text": {"_0": "Other"}}}}],
        "body": "Never selected",
    }
    payload = cast(literal(json.dumps({"original": original})), JSONB)
    async with database() as db:
        row = (await db.execute(select(*actor_sql_columns(payload)))).mappings().one()
    actors = ActorHandles.model_validate(row)
    assert actors.matches_any(ActorAnyOf(role="sender", handles=("Old", "Current")))
    assert not actors.matches_any(
        ActorAnyOf(role="current_chat_member", handles=("Old", "Current"))
    )
    assert not actors.matches_any(ActorAnyOf(role="sender", handles=("Old",)))
    assert "body" not in row


@pytest.mark.parametrize(
    "raw", ["+1 (415) 555-2671 ext. 12", "+१४१५५५५२६७१ x۱۲", "+١٤١٥٥٥٥٢٦٧١ #١٢"]
)
def test_endpoint_terms_and_postfilter_share_preparation(raw):
    from airweave.domains.entities.canonical.actors import ActorAnyOf, actor_match_token
    from airweave.domains.entities.canonical.apple_payloads import NativeContactHandle
    from airweave.domains.entities.canonical.contact_preparation import (
        phone_endpoint_key,
        prepare_phone,
    )

    group = ActorAnyOf(role="sender", handles=("+14155552671 ext. 12",), match="endpoint")
    observed = ActorHandles(sender_handle=raw)
    assert observed.matches_any(group)
    assert group.tokens[0] in observed.tokens
    assert actor_match_token("sender", raw, "endpoint") == group.tokens[0]
    prepared = prepare_phone(NativeContactHandle(nativeLabelID="fixture", rawValue=raw))
    assert phone_endpoint_key(raw) == (prepared.e164, prepared.extension)
    assert not observed.matches_any(
        ActorAnyOf(role="sender", handles=("+14155552671 x13",), match="endpoint")
    )
    assert not observed.matches_any(
        ActorAnyOf(role="sender", handles=("+14155552671",), match="endpoint")
    )
    assert not observed.matches_any(
        ActorAnyOf(role="current_chat_member", handles=(raw,), match="endpoint")
    )
    assert not observed.matches_any(ActorAnyOf(role="sender", handles=("+14155552671 ext. 12",)))
    assert observed.matches(ActorFilter(role="sender", handle=raw))


@pytest.mark.parametrize(
    "raw", ["4155552671", "Case@EXAMPLE", "Call +14155552671", "+¹4155552671", "+999123456789"]
)
def test_endpoint_unsupported_values_stay_raw_exact(raw):
    from airweave.domains.entities.canonical.actors import ActorAnyOf, actor_token

    group = ActorAnyOf(role="sender", handles=(raw,), match="endpoint")
    actors = ActorHandles(sender_handle=raw)
    assert actors.matches_any(group) and group.tokens == (actor_token("sender", raw),)
    assert not ActorHandles(sender_handle=raw.lower() + " ").matches_any(group)


async def test_endpoint_actual_sql_postfilter_rechecks_current_handle(database):
    from types import SimpleNamespace

    from airweave.core.logging import logger
    from airweave.domains.entities.canonical.actors import ActorAnyOf
    from airweave.domains.search.adapters.vector_db.filter_translator import (
        FilterTranslator as PlanTranslator,
    )
    from airweave.domains.search.types.filters import FilterGroup

    group = ActorAnyOf(
        role="sender", handles=("+14155552671 x12", "Case@EXAMPLE"), match="endpoint"
    )
    request = OwnedSearchRequest(query="hello", sync_ids=(uuid4(),), actor_any_of=group)
    compiled = PlanTranslator(logger=logger).translate(
        [FilterGroup(conditions=OwnedSearchService._prefilters(request, list(request.sync_ids)))]
    )
    assert all(token in compiled for token in group.tokens)
    for raw, matches in [
        ("+۱۴۱۵۵۵۵۲۶۷۱ ext. १२", True),
        ("+14155552671 x13", False),
        ("4155552671", False),
        ("Case@EXAMPLE", True),
        ("case@example", False),
    ]:
        payload = cast(
            literal(
                json.dumps({"original": {"sender": {"fields": {"id": {"text": {"_0": raw}}}}}})
            ),
            JSONB,
        )
        async with database() as db:
            facts = (await db.execute(select(*actor_sql_columns(payload)))).mappings().one()
        row = SimpleNamespace(
            **facts,
            actor_source="imessage",
            entity_definition_short_name="imessage_message",
            native_type=None,
            source_created_at=None,
            source_updated_at=None,
        )
        assert OwnedSearchService._matches(row, request) == matches
        row.actor_source = "apple_contacts"
        assert not OwnedSearchService._matches(row, request)
