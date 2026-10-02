"""Synthetic delivery boundaries, independent of corpus indexing and credentials."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from evaluation.owned_retrieval import ConversationIdentity, CorpusRecord, delivered_result

from airweave.domains.entities.canonical.extraction_models import (
    ExtractionCoverage,
    ExtractionOutcome,
)
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.search.owned_models import (
    OwnedSearchGroup,
    OwnedSearchHit,
    OwnedSearchMatch,
    OwnedSearchResponse,
)


def match(source_id="account-one", sync_id=None, native_id="message", provider="almanac"):
    """Build a match plus its separately retained census identity."""
    hit = OwnedSearchHit(
        record_id=uuid4(),
        revision=1,
        sync_id=sync_id or uuid4(),
        source_connection_id=uuid4(),
        provider=provider,
        identity=RecordIdentity(record_type="message", native_id=native_id, container_id="session"),
        title="Synthetic",
        excerpts=("Synthetic passage",),
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        completeness="complete",
    )
    record = CorpusRecord(
        record_id=hit.record_id,
        sync_id=hit.sync_id,
        revision=hit.revision,
        source_id=source_id,
        provider=provider,
        identity=hit.identity,
    )
    return hit, record


def response(*hits, incomplete=False):
    """A delivered bounded response, with no hidden members in items."""
    return OwnedSearchResponse(
        items=hits,
        sources=(),
        candidate_window_full=False,
        engine_partial=False,
        excluded_candidates=0,
        postfilter_excluded=0,
        retrieval_incomplete=incomplete,
    )


def test_cards_and_displayed_originals_are_different_ranked_units():
    first, first_record = match()
    second, second_record = match(sync_id=first.sync_id, native_id="second")
    third, third_record = match(native_id="third")
    first.group = OwnedSearchGroup(
        kind="session",
        native_id="session",
        matched_records=17,
        additional_matches=(
            OwnedSearchMatch.model_validate(second.model_dump(exclude={"group"}, round_trip=True)),
        ),
    )
    membership = ConversationIdentity(kind="session", native_id="session")
    first_record = first_record.model_copy(update={"conversation": membership})
    second_record = second_record.model_copy(update={"conversation": membership})
    records = (first_record, second_record, third_record)
    delivered = response(first, third, incomplete=True)
    cards = delivered_result("q", delivered, records, unit="card")
    originals = delivered_result("q", delivered, records, unit="displayed_original")
    assert cards.record_ids == (
        first_record.card_id,
        third_record.evaluation_id,
    )
    assert originals.record_ids == tuple(record.evaluation_id for record in records)
    assert cards.status == originals.status == "partial"
    assert len(originals.record_ids) == 3  # matched_records is not a delivered member list.


def test_card_identity_is_source_scoped_and_stable_across_destination_rebuilds():
    first, first_record = match()
    other, other_record = match(source_id="account-two")
    rebuilt, rebuilt_record = match()
    for hit in (first, other, rebuilt):
        hit.group = OwnedSearchGroup(kind="session", native_id="session", matched_records=1)
    membership = ConversationIdentity(kind="session", native_id="session")
    first_record = first_record.model_copy(update={"conversation": membership})
    other_record = other_record.model_copy(update={"conversation": membership})
    rebuilt_record = rebuilt_record.model_copy(update={"conversation": membership})
    result = delivered_result(
        "q", response(first, other), (first_record, other_record), unit="card"
    )
    rerun = delivered_result("q", response(rebuilt), (rebuilt_record,), unit="card")
    assert len(set(result.record_ids)) == 2
    assert result.record_ids[0] == rerun.record_ids[0]
    assert first_record.evaluation_id == rebuilt_record.evaluation_id
    assert first_record.evaluation_id != other_record.evaluation_id


def test_stale_or_wrong_source_matches_cannot_be_scored_as_frozen_records():
    first, record = match()
    with pytest.raises(ValueError, match="frozen source identity"):
        delivered_result(
            "q", response(first.model_copy(update={"revision": 2})), (record,), unit="card"
        )
    with pytest.raises(ValueError, match="Duplicate destination"):
        delivered_result("q", response(first), (record, record), unit="card")
    second, second_record = match(source_id="account-two")
    first.group = OwnedSearchGroup(
        kind="session",
        native_id="session",
        matched_records=2,
        additional_matches=(OwnedSearchMatch.model_validate(second.model_dump(exclude={"group"})),),
    )
    record = record.model_copy(
        update={"conversation": ConversationIdentity(kind="session", native_id="session")}
    )
    with pytest.raises(ValueError, match="different source identity"):
        delivered_result("q", response(first), (record, second_record), unit="card")


@pytest.mark.parametrize("violation", ("wrong_card", "missing_attestation", "foreign_member"))
def test_group_membership_must_match_frozen_census(violation):
    first, first_record = match()
    second, second_record = match(sync_id=first.sync_id, native_id="second")
    membership = ConversationIdentity(kind="session", native_id="session")
    first_record = first_record.model_copy(update={"conversation": membership})
    second_record = second_record.model_copy(update={"conversation": membership})
    first.group = OwnedSearchGroup(
        kind="session",
        native_id="session",
        matched_records=2,
        additional_matches=(OwnedSearchMatch.model_validate(second.model_dump(exclude={"group"})),),
    )
    if violation == "wrong_card":
        first.group = first.group.model_copy(update={"native_id": "different-session"})
    elif violation == "missing_attestation":
        first_record = first_record.model_copy(update={"conversation": None})
    else:
        second_record = second_record.model_copy(
            update={
                "conversation": ConversationIdentity(kind="session", native_id="different-session")
            }
        )
    for unit in ("card", "displayed_original"):
        with pytest.raises(ValueError, match="frozen conversation membership"):
            delivered_result("q", response(first), (first_record, second_record), unit=unit)


@pytest.mark.parametrize("location", ("representative", "additional"))
@pytest.mark.parametrize("violation", (None, "wrong_status", "unknown_status", "unknown_field"))
def test_actual_search_wire_extraction_roundtrip_stays_strict(location, violation):
    """The normal API dump includes computed status, including additional matches."""
    coverage = ExtractionCoverage(
        parts=(ExtractionOutcome(part_index=0, key="body", kind="body", outcome="indexed"),)
    )
    first, _ = match()
    second, _ = match(sync_id=first.sync_id, native_id="second")
    first.extraction = second.extraction = coverage
    first.group = OwnedSearchGroup(
        kind="session",
        native_id="session",
        matched_records=2,
        additional_matches=(OwnedSearchMatch.model_validate(second.model_dump(exclude={"group"})),),
    )
    wire = response(first).model_dump(mode="json")
    extraction = (
        wire["items"][0]["extraction"]
        if location == "representative"
        else wire["items"][0]["group"]["additional_matches"][0]["extraction"]
    )
    assert extraction["status"] == "complete"
    if violation == "wrong_status":
        extraction["status"] = "partial"
    elif violation == "unknown_status":
        extraction["status"] = "invented"
    elif violation == "unknown_field":
        extraction["future_field"] = True
    if violation is None:
        parsed = OwnedSearchResponse.model_validate(wire)
        assert parsed.model_dump(mode="json") == wire
        assert ExtractionCoverage.model_validate(coverage.persisted()).status == "complete"
        assert "status" not in coverage.persisted()
    else:
        with pytest.raises(ValueError):
            OwnedSearchResponse.model_validate(wire)
