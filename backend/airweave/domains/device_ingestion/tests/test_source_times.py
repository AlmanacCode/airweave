"""Profile-driven source dates preserve native original evidence."""

from datetime import datetime, timezone
from uuid import uuid4

from airweave.domains.device_ingestion.models import CommitDevicePage, DeviceObservation
from airweave.domains.device_ingestion.source_times import note_source_times
from airweave.domains.device_ingestion.tests.test_admission import (
    bound,  # noqa: F401
    note,
    service,
    setup_kind,
)
from airweave.domains.entities.canonical.apple_payloads import NativeNote
from airweave.domains.entities.canonical.query_models import RecordListQuery
from airweave.domains.entities.canonical.tests.test_query import query_service


def dated(profile="ZACCOUNT7"):
    original = note()
    original["note"]["fields"].update(
        {
            profile: {"integer": {"_0": 1}},
            "ZCREATIONDATE1": {"integer": {"_0": 900}},
            "ZCREATIONDATE3": {"real": {"_0": 1.25}},
            "ZMODIFICATIONDATE1": {"real": {"_0": -0.25}},
        }
    )
    return original


def test_profile_conflicting_columns_and_fractional_utc():
    assert note_source_times(NativeNote.model_validate(dated())) == (
        datetime(2001, 1, 1, 0, 0, 1, 250000, timezone.utc),
        datetime(2000, 12, 31, 23, 59, 59, 750000, timezone.utc),
    )
    assert note_source_times(NativeNote.model_validate(dated("ZACCOUNT3")))[0] == datetime(
        2001, 1, 1, 0, 15, tzinfo=timezone.utc
    )


def test_reader_column_precedence_not_relationship_value():
    original = dated("ZACCOUNT3")
    original["note"]["fields"]["ZACCOUNT7"] = {"null": {}}
    original["note"]["fields"]["ZCREATIONDATE3"] = {"integer": {"_0": 0}}
    assert note_source_times(NativeNote.model_validate(original))[0] == datetime(
        2001, 1, 1, tzinfo=timezone.utc
    )


def test_unknown_wrong_type_overflow_and_no_fallback():
    assert note_source_times(NativeNote.model_validate(note())) == (None, None)
    original = dated()
    original["note"]["fields"]["ZCREATIONDATE3"] = {"text": {"_0": "1.25"}}
    original["note"]["fields"]["ZMODIFICATIONDATE1"] = {"real": {"_0": 1e300}}
    assert note_source_times(NativeNote.model_validate(original)) == (None, None)


async def test_admitted_notes_dates_readback_raw_unchanged(database, bound):  # noqa: F811
    state, principal, run = await setup_kind(database, bound[0], "apple_notes")
    original = dated()
    request = CommitDevicePage(
        **principal.model_dump(),
        page_id=uuid4(),
        expected=run.version,
        observations=(DeviceObservation(native_id="note-one", original=original),),
    )
    async with database() as db:
        await service().page(
            db,
            state.organization_id,
            state.source_id,
            "run-one",
            request.model_dump_json().encode(),
        )
    async with database() as db:
        page = await query_service().list_records(
            db, state.organization_id, state.sync_id, RecordListQuery()
        )
        record = page.records[0]
        assert record.payload["original"] == original
        assert (record.source_created_at, record.source_updated_at) == note_source_times(
            NativeNote.model_validate(original)
        )
