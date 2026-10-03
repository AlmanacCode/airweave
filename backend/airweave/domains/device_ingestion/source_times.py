"""Supported Notes Core Data dates; native fields remain unchanged.

Messages wire schema does not identify legacy seconds versus modern nanoseconds,
so this module deliberately supplies no Messages conversion.
"""

from datetime import datetime, timedelta, timezone

from airweave.domains.entities.canonical.apple_payloads import (
    IntegerField,
    NativeNote,
    NativeValue,
    RealField,
)

_EPOCH = datetime(2001, 1, 1, tzinfo=timezone.utc)


def _core_date(value: NativeValue | None) -> datetime | None:
    if value is None:
        return None
    field = value.root
    if isinstance(field, IntegerField):
        seconds = field.integer.value
    elif isinstance(field, RealField):
        seconds = field.real.value
    else:
        return None
    try:
        return _EPOCH + timedelta(seconds=seconds)
    except (OverflowError, ValueError):
        return None


def note_source_times(note: NativeNote) -> tuple[datetime | None, datetime | None]:
    """Use the reader's relationship-column precedence, never date-value magnitude.

    ZACCOUNT7/4 select creation3; ZACCOUNT3 selects creation1. Even a null
    relationship tag identifies the captured column, matching the native reader's
    schema inspection. Missing profiles remain unknown. Zero/negative intervals
    are valid Core Data dates; only optional Messages events use zero sentinels.
    """
    fields = note.note.fields
    profile = next((key for key in ("ZACCOUNT7", "ZACCOUNT4", "ZACCOUNT3") if key in fields), None)
    if profile is None:
        return None, None
    creation = "ZCREATIONDATE1" if profile == "ZACCOUNT3" else "ZCREATIONDATE3"
    return _core_date(fields.get(creation)), _core_date(fields.get("ZMODIFICATIONDATE1"))
