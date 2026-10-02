"""Canonical chunks retain read provenance without duplicating original bodies."""

import json
from uuid import uuid4

from airweave.domains.entities.canonical.content_models import ContentProvenance, MatchedPart
from airweave.platform.destinations.vespa.transformer import EntityTransformer
from airweave.platform.entities._base import AirweaveSystemMetadata
from airweave.platform.entities.wispr import WisprMeetingEntity


def test_canonical_payload_is_bounded_without_changing_index_fields():
    entity = WisprMeetingEntity(
        breadcrumbs=[],
        meeting_id="meeting",
        title="Meeting",
        notes="",
        summary="Summary",
        transcript="Original transcript " * 10000,
        share_link="https://example.test/meeting",
    )
    entity.entity_id = "captured-part"
    entity.name = "Meeting"
    entity.textual_representation = "Exact indexed chunk"
    entity.airweave_system_metadata = AirweaveSystemMetadata(
        source_name="wispr",
        sync_id=uuid4(),
        canonical_record_type="meeting",
        source_created_known=1,
        source_created_us=123456789,
        original_entity_id="captured-part",
        dense_embedding=[0.1, 0.2],
        content_provenance=ContentProvenance(
            part=MatchedPart(part_index=0, key="transcript", kind="body", title="Meeting"),
            content_start=0,
            content_end=19,
            preview="Exact indexed chunk",
        ),
    )
    transformer = EntityTransformer(collection_id=uuid4())
    before = transformer.transform(entity)
    entity.transcript += "Additional original " * 10000
    after = transformer.transform(entity)
    assert after == before
    payload = json.loads(after.fields["payload"])
    assert set(payload) == {"web_url", "content_provenance"}
    assert payload["content_provenance"]["preview"] == "Exact indexed chunk"
    assert payload["web_url"] == entity.web_url
    assert len(after.fields["payload"].encode()) < 1024
    assert after.fields["textual_representation"] == entity.textual_representation
    assert after.fields["airweave_system_metadata_canonical_record_type"] == "meeting"
    assert after.fields["airweave_system_metadata_source_created_us"] == 123456789

    # Legacy entities keep the full provider payload and otherwise identical fields.
    entity.airweave_system_metadata.canonical_record_type = None
    legacy = transformer.transform(entity)
    assert json.loads(legacy.fields["payload"])["transcript"] == entity.transcript
    canonical_fields = dict(after.fields)
    legacy_fields = dict(legacy.fields)
    canonical_fields.pop("payload")
    canonical_fields.pop("airweave_system_metadata_canonical_record_type")
    legacy_fields.pop("payload")
    assert canonical_fields == legacy_fields
