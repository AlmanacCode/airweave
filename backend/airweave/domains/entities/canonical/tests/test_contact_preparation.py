"""Public synthetic Contacts field preparation; no live Contacts access or people merge."""

import copy
import json
import os
import unicodedata
from pathlib import Path
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.apple_payloads import NativeContactHandle
from airweave.domains.entities.canonical.apple_projection import map_apple
from airweave.domains.entities.canonical.contact_preparation import prepare_contact, prepare_phone
from airweave.domains.entities.canonical.tests.test_apple_projection import record
from airweave.domains.entities.canonical.text_models import TextArtifact, TextPreparation


def phone(raw, label="Work", native_id="phone-label"):
    return NativeContactHandle.model_validate(
        {"nativeLabelID": native_id, "label": label, "rawValue": raw}
    )


def contact_record():
    original = json.loads(
        (Path(__file__).parents[1] / "apple_payload_tests/fixtures/contact-swift.json").read_text()
    )
    original["contact"]["phones"] = [
        {"nativeLabelID": "work-one", "label": "Work", "rawValue": "+44 20 8366 1177 ext. 123"},
        {"nativeLabelID": "work-two", "label": "Work", "rawValue": "+442083661177 x456"},
        {"nativeLabelID": "national", "rawValue": "020 8366 1177"},
    ]
    original["contact"]["organizationName"] = "Bücher 東京 ACME"
    original["contact"]["nickname"] = "Sáme\u0301ر"
    original["contact"]["emails"] = [
        {"nativeLabelID": "email-one", "label": "Work", "rawValue": "Sam+Work@EXAMPLE.COM"}
    ]
    return record("apple_contact", original["contact"]["nativeID"], original)


@pytest.mark.parametrize(
    "raw,status",
    [
        ("020 8366 1177", "region_required"),
        ("415 555 0100", "region_required"),
        ("", "malformed"),
        ("Call me at +44 20 8366 1177", "unsupported"),
        ("+1800FLOWERS", "unsupported"),
        ("tel:+442083661177;ext=123", "unsupported"),
        ("+999 123456789", "malformed"),
    ],
)
def test_national_prose_and_unsupported_are_not_canonical_numbers(raw, status):
    prepared = prepare_phone(phone(raw))
    assert prepared.raw_value == raw
    assert prepared.status == status
    assert prepared.e164 is None
    assert prepared.region_input is None
    assert prepared.region_provenance == "not_provided"


def test_international_phone_keeps_extensions_and_parser_classification():
    first = prepare_phone(phone("+44 20 8366 1177 ext. 123"))
    second = prepare_phone(phone("+442083661177 x456"))
    assert first.status == second.status == "international"
    assert first.e164 == second.e164 == "+442083661177"
    assert (first.e164, first.extension) != (second.e164, second.extension)
    assert first.extension == "123" and second.extension == "456"
    assert first.possible and first.valid
    assert first.formatting_key == "+442083661177 ext. 123"
    short = prepare_phone(phone("+1 (555) 0100"))
    assert short.status == "malformed" and short.possible is True and short.valid is False
    assert short.e164 is None and short.possible_status == "local_only"


async def test_contact_projection_consumes_labeled_unicode_values_and_derived_keys():
    source = contact_record()
    untouched = copy.deepcopy(source.model_dump())
    prepared = prepare_contact(source)
    assert prepared.origin.record_id == source.id
    assert prepared.origin.native_id == source.identity.native_id
    assert prepared.origin.revision == source.revision
    assert prepared.origin.observed_at == source.observed_at
    assert prepared.origin.account_id == "synthetic-store"
    assert prepared.phones[0].native_label_id == "work-one"
    assert prepared.phones[2].label is None
    assert prepared.source.contact.emails[0].raw_value == "Sam+Work@EXAMPLE.COM"
    assert "Bücher 東京 ACME" in prepared.text and "Sáme\u0301ر" in prepared.text
    assert "Phone [Work]: +44 20 8366 1177 ext. 123" in prepared.text
    assert "International phone: +442083661177 ext. 456" in prepared.text
    assert "Phone interpretation: region required" in prepared.text
    assert "Email [Work]: Sam+Work@EXAMPLE.COM" in prepared.text
    mapped = await map_apple(source, "apple_contacts")
    body = mapped.parts[0]
    assert body.entity.content == body.native_body.text == prepared.text
    assert body.native_body.kind == "extracted_text"
    assert body.native_body.preparation == prepared.preparation
    assert prepared.preparation.processor == "apple_contacts"
    assert prepared.preparation.version == "contacts-fields-v2"
    assert prepared.preparation.dependencies[0].version == "9.0.40"
    assert prepared.preparation.dependencies[1].name == "unicode-decimal"
    assert prepared.preparation.dependencies[1].version == unicodedata.unidata_version
    assert source.model_dump() == untouched
    duplicate = source.model_copy(update={"id": uuid4()})
    assert prepare_contact(duplicate).origin.record_id != prepared.origin.record_id
    assert prepare_contact(duplicate).text == prepared.text  # Cards remain separate observations.


def test_unknown_old_preparation_is_explicitly_absent_and_descriptor_bounded():
    from pydantic import ValidationError

    original = {
        "id": uuid4(),
        "part_index": 0,
        "sha256": "0" * 64,
        "size_bytes": 0,
        "characters": 0,
        "content_start": 0,
        "kind": "native_text",
    }
    assert TextArtifact.model_validate(original).preparation is None
    descriptor = prepare_contact(contact_record()).preparation
    artifact = TextArtifact.model_validate({**original, "preparation": descriptor})
    assert TextArtifact.model_validate(artifact.model_dump(mode="json")).preparation == descriptor
    with pytest.raises(ValidationError):
        TextPreparation.model_validate({**descriptor.model_dump(), "policy": "x" * 129})


@pytest.mark.skipif(
    not os.environ.get("APPLE_PIPELINE_VESPA_URL"),
    reason="requires disposable Apple Vespa",
)
async def test_contact_preparation_published_and_retained(database, source, tmp_path, monkeypatch):
    import os
    from urllib.parse import urlsplit

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.core.config import settings
    from airweave.core.logging import logger
    from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
    from airweave.domains.entities.canonical.projector import CanonicalProjector
    from airweave.domains.entities.canonical.query import CanonicalQueryService
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.store import CanonicalRecordStore
    from airweave.domains.entities.canonical.tests.helpers import (
        bind_projection,
        capture,
        observation,
    )
    from airweave.domains.entities.canonical.tests.test_apple_pipeline import (
        FixedDense,
        FixedSparse,
        TextConverters,
    )
    from airweave.domains.entities.canonical.text_query import CanonicalTextReader
    from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
    from airweave.platform.destinations.vespa.destination import VespaDestination

    service, fence = source
    binding = await bind_projection(database, fence, "apple_contacts")
    original = contact_record()
    await capture(
        database, service, fence, observation(identity=original.identity, payload=original.payload)
    )
    parsed = urlsplit(os.environ["APPLE_PIPELINE_VESPA_URL"])
    assert parsed.hostname in ("localhost", "127.0.0.1") and parsed.port == 8086
    monkeypatch.setattr(settings, "VESPA_URL", f"{parsed.scheme}://{parsed.hostname}")
    monkeypatch.setattr(settings, "VESPA_PORT", parsed.port)
    storage = FilesystemBackend(tmp_path)
    log = logger.with_context(request_id="contacts-preparation-fixture")
    projector = CanonicalProjector(
        CanonicalProjectionStore(),
        database,
        ChunkEmbedProcessor(TextConverters(), FixedDense(384), FixedSparse()),
        storage,
    )
    destination = await VespaDestination.create(
        collection_id=binding.collection_id, organization_id=fence.organization_id, logger=log
    )
    try:
        async with database() as db:
            work = (
                await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id)
            )[0]
        assert (await projector.project_one(work, "apple_contacts", destination, log)).published
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from vespa.application import Vespa

        from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
        from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
        from airweave.domains.search.executor import SearchPlanExecutor
        from airweave.domains.search.owned import OwnedSearchService
        from airweave.domains.search.owned_models import OwnedSearchRequest
        from airweave.domains.sources.fakes.registry import FakeSourceRegistry

        registry = FakeSourceRegistry()
        blocked = AsyncMock(side_effect=AssertionError("No provider or agent calls"))
        engine = VespaVectorDB(
            app=Vespa(url=f"{parsed.scheme}://{parsed.hostname}", port=parsed.port),
            logger=log,
            filter_translator=FilterTranslator(logger=log),
        )
        owned = OwnedSearchService(
            SearchPlanExecutor(
                FixedDense(384), FixedSparse(), engine, blocked, registry, blocked, blocked
            ),
            registry,
        )
        ctx = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id), logger=log)
        # Exact stored Hindi/Urdu names must be discoverable, not only preserved.
        # This does not assert transliteration or accent-insensitive matching.
        for term in ("ACME", "442083661177", "शर्मा", "سمیر"):
            found = await owned.search(
                database,
                ctx,
                OwnedSearchRequest(query=term, sync_ids=(fence.sync_id,), mode="keyword"),
            )
            assert [hit.record_id for hit in found.items] == [work.record.id]
        reader = CanonicalTextReader(
            CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "key"), storage
        )
        async with database() as db:
            representation = (
                await reader.list(db, fence.organization_id, fence.sync_id, work.record.id, 1)
            ).representations[0]
            assert representation.kind == "extracted_text"
            assert representation.preparation.version == "contacts-fields-v2"
            assert representation.preparation.dependencies[0].version == "9.0.40"
            retained = await reader.read(
                db,
                fence.organization_id,
                fence.sync_id,
                work.record.id,
                1,
                representation.generation,
                representation.id,
            )
            assert retained.text == prepare_contact(work.record).text
            assert "International phone: +442083661177 ext. 123" in retained.text
            assert "Sam+Work@EXAMPLE.COM" in retained.text
    finally:
        await destination.delete_by_sync_id(fence.sync_id)
        await destination.close_connection()


@pytest.mark.parametrize(
    "raw",
    [
        "+१४१५५५५२६७१ ext. १२",
        "+١٤١٥٥٥٥٢٦٧١ x١٢",
        "+۱۴۱۵۵۵۵۲۶۷۱ #۱۲",
        "+1४١۵5552671 ext. 1۲",
    ],
)
def test_unicode_decimal_phone_and_extension_preserve_raw(raw):
    prepared = prepare_phone(phone(raw))
    assert prepared.raw_value == raw
    assert prepared.status == "international"
    assert prepared.e164 == "+14155552671"
    assert prepared.extension == "12"
    assert prepared.formatting_key == "+14155552671 ext. 12"


@pytest.mark.parametrize(
    "raw", ["+¹4155552671", "+①4155552671", "+½4155552671", "Call +१४१५५५५२६७१"]
)
def test_nondecimal_numerics_and_prose_are_not_normalized(raw):
    prepared = prepare_phone(phone(raw))
    assert prepared.raw_value == raw and prepared.status == "unsupported"
    assert prepared.e164 is None


def test_unicode_national_phone_has_no_guessed_region():
    raw = "०२० ८३६६ ११७७ ext. ۱۲"
    prepared = prepare_phone(phone(raw))
    assert prepared.raw_value == raw and prepared.status == "region_required"
    assert prepared.formatting_key == "02083661177 ext. 12"
    assert prepared.extension == "12" and prepared.e164 is None
