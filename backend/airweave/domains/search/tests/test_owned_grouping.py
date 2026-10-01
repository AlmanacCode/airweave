"""Conversation grouping retains exact, bounded independent originals."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from airweave.domains.search.owned import OwnedSearchService, _EnrichmentRecord
from airweave.domains.search.owned_models import OwnedSearchGroup, OwnedSearchHit, OwnedSearchMatch


def match(sync_id=None, provider="almanac", kind="message", native="m", container="s"):
    return OwnedSearchHit(
        record_id=uuid4(),
        revision=3,
        sync_id=sync_id or uuid4(),
        source_connection_id=uuid4(),
        provider=provider,
        identity={"record_type": kind, "native_id": native, "container_id": container},
        title=native,
        excerpts=("best " + native, "other"),
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        completeness="complete",
    )


def row(hit, **changes):
    values = {
        "id": hit.record_id,
        "sync_id": hit.sync_id,
        "record_revision": 3,
        "indexed_generation": uuid4(),
        "indexed_pipeline_version": 2,
        "entity_definition_short_name": hit.identity.record_type,
        "native_id": hit.identity.native_id,
        "container_id": hit.identity.container_id,
        "parent_record_type": "session",
        "parent_native_id": "s",
        "parent_container_id": None,
        "observed_at": hit.observed_at,
        "source_created_at": None,
        "source_updated_at": None,
        "completeness": "complete",
        "email_thread_id": None,
    }
    values.update(changes)
    return _EnrichmentRecord(**values)


def test_source_scoped_group_keeps_exact_anchors_and_observed_count():
    sync = uuid4()
    hits = [match(sync, native=str(i)) for i in range(6)]
    other = match(native="other")
    for hit in [*hits, other]:
        hit.group = OwnedSearchService._conversation(row(hit), hit)
    grouped = OwnedSearchService._group_hits([hits[0], other, *hits[1:]])
    assert [h.record_id for h in grouped] == [hits[0].record_id, other.record_id]
    group = grouped[0].group
    assert group.native_id == "s" and group.matched_records == 6
    assert len(group.additional_matches) == 3
    for original, extra in zip(hits[1:4], group.additional_matches, strict=True):
        assert type(extra) is OwnedSearchMatch
        assert (extra.record_id, extra.identity, extra.revision) == (
            original.record_id,
            original.identity,
            original.revision,
        )
        assert extra.excerpts == original.excerpts[:1]
        assert "group" not in extra.model_dump()


def test_only_admitted_sessions_or_validated_gmail_threads_group():
    root = match(kind="session", native="s", container=None)
    root.group = OwnedSearchService._conversation(
        row(root, parent_record_type=None, parent_native_id=None), root
    )
    child = match(root.sync_id)
    child.group = OwnedSearchService._conversation(row(child), child)
    assert OwnedSearchService._group_hits([child, root])[0].group.matched_records == 2
    for provider, kind in [
        ("google_calendar", "event"),
        ("slack", "message"),
        ("almanac", "knowledge"),
        ("gdrive", "file"),
    ]:
        hit = match(provider=provider, kind=kind)
        assert OwnedSearchService._conversation(row(hit), hit) is None
    assert OwnedSearchService._conversation(row(child, parent_native_id="wrong"), child) is None
    assert OwnedSearchService._conversation(row(child, parent_container_id="nested"), child) is None
    gmail = match(provider="gmail")
    gmail.email_thread_id = "thread_1"
    assert OwnedSearchService._conversation(row(gmail), gmail).kind == "email_thread"
    with pytest.raises(ValidationError):
        OwnedSearchGroup(kind="record", native_id="x", matched_records=1)
    with pytest.raises(ValidationError):
        OwnedSearchGroup(kind="session", native_id="x", matched_records=0)
    with pytest.raises(ValidationError):
        OwnedSearchGroup(
            kind="session", native_id="x", matched_records=5, additional_matches=[gmail] * 4
        )
