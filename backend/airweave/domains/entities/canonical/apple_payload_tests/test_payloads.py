"""Validate actual Swift-serialized synthetic envelopes without flattening originals."""

import base64
import copy
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.apple_payloads import (
    BinaryField,
    NativeContact,
    NativeMessage,
    NativeNote,
    native_integer,
    validate_device_original,
)

FIXTURES = Path(__file__).with_name("fixtures")
LARGE_ID = 9_007_199_254_740_993


def swift_payload(kind):
    return json.loads((FIXTURES / f"{kind}-swift.json").read_text())


def test_actual_swift_message_int64_binary_unknown_columns_preserved():
    original = swift_payload("message")
    before = copy.deepcopy(original)
    parsed = validate_device_original("imessage", original)
    assert isinstance(parsed, NativeMessage)
    assert parsed.native_id == "fixture-guid"
    assert parsed.message.row_id == LARGE_ID
    assert native_integer(parsed.message.fields, "largeNativeColumn") == LARGE_ID
    blob = parsed.message.fields["unknownFutureColumn"].root
    assert isinstance(blob, BinaryField)
    assert blob.blob.bytes() == bytes([0, 255, 195, 169])
    assert original == before
    assert original["message"]["fields"]["largeNativeColumn"]["integer"]["_0"] == LARGE_ID


def test_actual_swift_notes_body_and_lifecycle():
    original = swift_payload("note")
    before = copy.deepcopy(original)
    parsed = validate_device_original("apple_notes", original)
    assert isinstance(parsed, NativeNote)
    assert parsed.native_id == "fixture-note"
    assert parsed.note.primary_key == LARGE_ID
    assert not parsed.locked and not parsed.marked_for_deletion
    assert base64.b64decode(parsed.compressed_body, validate=True) == bytes([0, 255, 195, 169])
    assert original == before
    original["note"]["fields"]["ZMARKEDFORDELETION"]["integer"]["_0"] = 1
    assert validate_device_original("apple_notes", original).marked_for_deletion


def test_actual_swift_contacts_identity_and_raw_handles():
    original = swift_payload("contact")
    before = copy.deepcopy(original)
    parsed = validate_device_original("apple_contacts", original)
    assert isinstance(parsed, NativeContact)
    assert parsed.native_id == original["contact"]["nativeID"]
    assert parsed.contact.given_name == "سمیر"
    assert parsed.contact.family_name == "शर्मा"
    assert parsed.contact.phones[0].raw_value == "+1 (555) 0100"
    assert parsed.contact.emails[0].raw_value == "sam+work@example.com"
    assert original == before


def test_strict_versions_and_enum_tags():
    for invalid in (2, True, 1.0, "1"):
        original = swift_payload("message")
        original["schemaVersion"] = invalid
        with pytest.raises(ValidationError):
            validate_device_original("imessage", original)
    original = swift_payload("message")
    original["message"]["fields"]["text"] = {"text": {"value": "unofficial alias"}}
    with pytest.raises(ValidationError):
        validate_device_original("imessage", original)
    original = swift_payload("message")
    original["message"]["fields"]["text"] = {"text": {"_0": "hello"}, "null": {}}
    with pytest.raises(ValidationError):
        validate_device_original("imessage", original)


def test_malformed_native_values_rejected():
    for bad in (
        {"integer": {"_0": True}},
        {"integer": {"_0": 2**63}},
        {"blob": {"_0": "not base64"}},
        {"real": {"_0": float("nan")}},
        {"unknownCase": {"_0": "hello"}},
    ):
        original = swift_payload("message")
        original["message"]["fields"]["unknownFutureColumn"] = bad
        with pytest.raises(ValidationError):
            validate_device_original("imessage", original)


def test_native_guid_primary_key_mismatch_rejected():
    original = swift_payload("message")
    original["guid"] = "foreign-guid"
    with pytest.raises(ValidationError, match="GUID disagrees"):
        validate_device_original("imessage", original)
    original = swift_payload("note")
    original["note"]["primaryKey"] = 42
    with pytest.raises(ValidationError, match="primary key disagrees"):
        validate_device_original("apple_notes", original)


def test_locked_notes_no_stale_body_or_relationship_payload():
    original = swift_payload("note")
    original["note"]["fields"]["ZISPASSWORDPROTECTED"]["integer"]["_0"] = 1
    original["fidelity"] = {"lockedBodyWithheld": {}}
    with pytest.raises(ValidationError, match="withheld content"):
        validate_device_original("apple_notes", original)
    original.pop("compressedBody")
    original["note"]["fields"].pop("unknownFutureColumn")
    parsed = validate_device_original("apple_notes", original)
    assert parsed.locked
    original["account"] = {
        "primaryKey": 1,
        "fields": {"Z_PK": {"integer": {"_0": 1}}, "ZDATA": {"blob": {"_0": "c3RhbGU="}}},
    }
    with pytest.raises(ValidationError, match="relationships contain withheld"):
        validate_device_original("apple_notes", original)


def test_missing_lifecycle_and_inconsistent_fidelity_rejected():
    original = swift_payload("note")
    original["note"]["fields"].pop("ZISPASSWORDPROTECTED")
    with pytest.raises(ValidationError, match="lifecycle evidence"):
        validate_device_original("apple_notes", original)
    original = swift_payload("message")
    original["bodyFidelity"] = {"attributedBodyUndecoded": {}}
    with pytest.raises(ValidationError, match="fidelity disagrees"):
        validate_device_original("imessage", original)


def test_unsupported_source_and_contact_shape_rejected():
    with pytest.raises(ValueError, match="not supported"):
        validate_device_original("unsupported", {})
    original = swift_payload("contact")
    original["contact"]["nativeID"] = ""
    with pytest.raises(ValidationError):
        validate_device_original("apple_contacts", original)
