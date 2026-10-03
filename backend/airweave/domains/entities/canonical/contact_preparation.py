"""Versioned Contacts discovery views; raw values and individual cards remain authoritative."""

import re
import unicodedata
from datetime import datetime
from typing import Literal
from uuid import UUID

import phonenumbers
from pydantic import BaseModel, ConfigDict

from airweave.domains.entities.canonical.apple_payloads import (
    DeviceOriginalEnvelope,
    NativeContact,
    NativeContactHandle,
)
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.text_models import (
    TextPreparation,
    TextPreparationDependency,
)

# Libphonenumber deliberately parses prose/vanity strings permissively. This view
# qualifies only an entire numeric spelling, never a number extracted from prose.
_NUMERIC = re.compile(
    r"(?P<number>\+?[0-9][0-9\s().-]*)(?:\s*(?:ext\.?|x|#)\s*(?P<extension>[0-9]+))?", re.IGNORECASE
)
_MAX_HANDLES = 500


def _numeric_spelling(raw: str) -> tuple[str, re.Match[str]] | None:
    """Whole bounded decimal spelling; never compatibility numerics or prose."""
    if len(raw) > 256:
        return None
    normalized = "".join(
        str(decimal) if (decimal := unicodedata.decimal(character, None)) is not None else character
        for character in raw.strip()
    )
    match = _NUMERIC.fullmatch(normalized)
    return (normalized, match) if match else None


def phone_endpoint_key(raw: str) -> tuple[str, str | None] | None:
    """Qualified explicit international endpoint using the Contacts preparation policy.

    None means keep the entire raw spelling exact; it does not infer a region,
    remove extensions, extract prose, or establish ownership/reachability.
    """
    spelling = _numeric_spelling(raw)
    if spelling is None or not spelling[0].startswith("+"):
        return None
    try:
        parsed = phonenumbers.parse(spelling[0], None, keep_raw_input=True)
    except phonenumbers.NumberParseException:
        return None
    if (
        phonenumbers.is_possible_number_with_reason(parsed)
        != phonenumbers.ValidationResult.IS_POSSIBLE
    ):
        return None
    return phonenumbers.format_number(
        parsed, phonenumbers.PhoneNumberFormat.E164
    ), parsed.extension or None


class ContactViewModel(BaseModel):
    """Immutable preparation results do not mutate retained source payloads."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ContactOrigin(ContactViewModel):
    """The exact observed card revision, separate from authored person identity."""

    source_kind: Literal["apple_contacts"] = "apple_contacts"
    account_id: str
    sync_id: UUID
    record_id: UUID
    native_id: str
    revision: int
    observed_at: datetime


class PreparedPhone(ContactViewModel):
    """Formatting/canonicalization are discovery facts, never identity or reachability proof."""

    native_label_id: str
    label: str | None
    raw_value: str
    status: Literal["international", "region_required", "unsupported", "malformed"]
    formatting_key: str | None = None
    e164: str | None = None
    extension: str | None = None
    possible: bool | None = None
    possible_status: (
        Literal[
            "full_number",
            "local_only",
            "invalid_country_code",
            "too_short",
            "too_long",
            "invalid_length",
        ]
        | None
    ) = None
    valid: bool | None = None
    region_input: None = None
    region_provenance: Literal["not_provided"] = "not_provided"


def prepare_phone(handle: NativeContactHandle) -> PreparedPhone:
    """Only explicit '+' input reaches the parser; national numbers have no default region."""
    raw = handle.raw_value
    result = {"native_label_id": handle.native_label_id, "label": handle.label, "raw_value": raw}
    if not raw.strip():
        return PreparedPhone(**result, status="malformed")
    if len(raw) > 256:
        return PreparedPhone(**result, status="unsupported")
    spelling = _numeric_spelling(raw)
    if spelling is None:
        return PreparedPhone(**result, status="unsupported")
    normalized, match = spelling
    number, extension = match.group("number"), match.group("extension")
    digits = "".join(character for character in number if character in "0123456789")
    formatting = ("+" if number.startswith("+") else "") + digits
    if extension:
        formatting += " ext. " + extension
    if not number.startswith("+"):
        return PreparedPhone(
            **result, status="region_required", formatting_key=formatting, extension=extension
        )
    try:
        parsed = phonenumbers.parse(normalized, None, keep_raw_input=True)
    except phonenumbers.NumberParseException:
        return PreparedPhone(
            **result, status="malformed", formatting_key=formatting, extension=extension
        )
    possible, valid = phonenumbers.is_possible_number(parsed), phonenumbers.is_valid_number(parsed)
    reason = phonenumbers.is_possible_number_with_reason(parsed)
    possibilities = {
        phonenumbers.ValidationResult.IS_POSSIBLE: "full_number",
        phonenumbers.ValidationResult.IS_POSSIBLE_LOCAL_ONLY: "local_only",
        phonenumbers.ValidationResult.INVALID_COUNTRY_CODE: "invalid_country_code",
        phonenumbers.ValidationResult.TOO_SHORT: "too_short",
        phonenumbers.ValidationResult.TOO_LONG: "too_long",
        phonenumbers.ValidationResult.INVALID_LENGTH: "invalid_length",
    }
    full_number = reason == phonenumbers.ValidationResult.IS_POSSIBLE

    return PreparedPhone(
        **result,
        status="international" if full_number else "malformed",
        formatting_key=formatting,
        extension=parsed.extension or None,
        possible=possible,
        possible_status=possibilities[reason],
        valid=valid,
        e164=phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        if full_number
        else None,
    )


class PreparedContact(ContactViewModel):
    """An individual observed card, not a resolved or authored person."""

    origin: ContactOrigin
    source: NativeContact
    phones: tuple[PreparedPhone, ...]
    title: str
    text: str
    preparation: TextPreparation


def prepare_contact(record: SourceRecord) -> PreparedContact:
    """Consume the actual admitted envelope and retain all field spelling and labels."""
    if record.deleted_at is not None or record.content_access != "available":
        raise ValueError("Unavailable Contacts originals cannot be prepared")
    envelope = DeviceOriginalEnvelope.model_validate(record.payload)
    if envelope.source_kind != "apple_contacts" or record.identity.record_type != "apple_contact":
        raise ValueError("Contacts preparation requires a committed Contacts envelope")
    source = NativeContact.model_validate(envelope.original)
    if source.native_id != record.identity.native_id:
        raise ValueError("Retained Contacts identity differs from original")
    contact = source.contact
    if len(contact.phones) + len(contact.emails) > _MAX_HANDLES:
        raise ValueError("Contact handle preparation budget exceeded")
    name = " ".join(
        value
        for value in (
            contact.name_prefix,
            contact.given_name,
            contact.middle_name,
            contact.family_name,
            contact.name_suffix,
        )
        if value
    )
    lines = [
        f"{label}: {value}"
        for label, value in (
            ("Name prefix", contact.name_prefix),
            ("Given name", contact.given_name),
            ("Middle name", contact.middle_name),
            ("Family name", contact.family_name),
            ("Name suffix", contact.name_suffix),
            ("Nickname", contact.nickname),
            ("Organization", contact.organization_name),
        )
        if value
    ]
    phones = tuple(prepare_phone(handle) for handle in contact.phones)
    for phone in phones:
        label = phone.label if phone.label is not None else "unlabeled"
        lines.append(f"Phone [{label}]: {phone.raw_value}")
        if phone.formatting_key and phone.formatting_key != phone.raw_value:
            lines.append(f"Phone formatting: {phone.formatting_key}")
        if phone.e164:
            endpoint = phone.e164 + (f" ext. {phone.extension}" if phone.extension else "")
            lines.append(f"International phone: {endpoint}")
        lines.append(f"Phone interpretation: {phone.status.replace('_', ' ')}")
        if phone.valid is not None:
            lines.append(f"Possible number: {str(phone.possible).lower()}")
            lines.append(f"Number extent: {phone.possible_status.replace('_', ' ')}")
            lines.append(f"Valid numbering range: {str(phone.valid).lower()}")
    for email in contact.emails:
        label = email.label if email.label is not None else "unlabeled"
        lines.append(f"Email [{label}]: {email.raw_value}")
    return PreparedContact(
        origin=ContactOrigin(
            account_id=envelope.account_id,
            sync_id=record.sync_id,
            record_id=record.id,
            native_id=source.native_id,
            revision=record.revision,
            observed_at=record.observed_at,
        ),
        source=source,
        phones=phones,
        title=name or contact.organization_name or "Contact",
        text="\n".join(lines),
        preparation=TextPreparation(
            processor="apple_contacts",
            version="contacts-fields-v2",
            source_path="/original/contact",
            policy="explicit_international_only_no_region",
            dependencies=(
                TextPreparationDependency(
                    name="phonenumberslite", version=phonenumbers.__version__
                ),
                TextPreparationDependency(
                    name="unicode-decimal", version=unicodedata.unidata_version
                ),
            ),
        ),
    )
