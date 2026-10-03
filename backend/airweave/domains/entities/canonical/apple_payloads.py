"""Versioned native acquisition envelopes; no inferred Apple account or rich-body claims."""

import base64
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    RootModel,
    StrictFloat,
    StrictInt,
    StrictStr,
    model_validator,
)


class NativeModel(BaseModel):
    """Closed producer envelope; native columns themselves remain extensible."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, populate_by_name=False
    )


class Empty(NativeModel):
    """Swift Codable emits an empty associated-value object for enum cases."""


class IntegerValue(NativeModel):
    """One exact signed Int64, never a JavaScript float."""

    value: StrictInt = Field(alias="_0", ge=-(2**63), le=2**63 - 1)


class RealValue(NativeModel):
    """Native SQLite real, finite and distinct from the integer tag."""

    value: StrictFloat = Field(alias="_0")


class TextValue(NativeModel):
    """Unmodified native Unicode string."""

    value: StrictStr = Field(alias="_0")


class BinaryValue(TextValue):
    """Canonical Swift Data base64, including empty data."""

    @model_validator(mode="after")
    def valid_base64(self) -> "BinaryValue":
        """Reject invalid/noncanonical encoding without changing retained bytes."""
        decoded = base64.b64decode(self.value, validate=True)
        if base64.b64encode(decoded).decode("ascii") != self.value:
            raise ValueError("Native binary value is not canonical base64")
        return self

    def bytes(self) -> bytes:
        """Return the exact original binary value."""
        return base64.b64decode(self.value, validate=True)


class NullField(NativeModel):
    """Native null tagged by Swift Codable."""

    null: Empty


class IntegerField(NativeModel):
    """Native integer tagged by Swift Codable."""

    integer: IntegerValue


class RealField(NativeModel):
    """Native real tagged by Swift Codable."""

    real: RealValue


class TextField(NativeModel):
    """Native text tagged by Swift Codable."""

    text: TextValue


class BinaryField(NativeModel):
    """Native binary tagged by Swift Codable."""

    blob: BinaryValue


class NativeValue(RootModel[NullField | IntegerField | RealField | TextField | BinaryField]):
    """Swift Codable's one-key tagged value, including unknown native columns."""


class NativeRow(NativeModel):
    """Messages SQLite row identity plus all observed native columns."""

    row_id: StrictInt = Field(alias="rowID", ge=-(2**63), le=2**63 - 1)
    fields: dict[str, NativeValue]


class NoteRow(NativeModel):
    """Notes Core Data row identity plus all observed native columns."""

    primary_key: StrictInt = Field(alias="primaryKey", ge=-(2**63), le=2**63 - 1)
    fields: dict[str, NativeValue]

    @model_validator(mode="after")
    def coherent_primary_key(self) -> "NoteRow":
        """Require the retained Core Data row key to match its envelope key."""
        if native_integer(self.fields, "Z_PK") != self.primary_key:
            raise ValueError("Notes primary key disagrees with retained row")
        return self


def native_text(fields: dict[str, NativeValue], key: str) -> str | None:
    """Read a text tag; never coerce another native representation."""
    field = fields.get(key)
    return (
        field.root.text.value if field is not None and isinstance(field.root, TextField) else None
    )


def native_integer(fields: dict[str, NativeValue], key: str) -> int | None:
    """Read an exact integer tag; missing/wrong tags stay unknown."""
    field = fields.get(key)
    return (
        field.root.integer.value
        if field is not None and isinstance(field.root, IntegerField)
        else None
    )


class NativeMessage(NativeModel):
    """Versioned Messages observation, distinct from session transcript messages."""

    schema_version: Annotated[int, Field(strict=True, ge=1, le=1)] = Field(alias="schemaVersion")
    guid: StrictStr = Field(min_length=1)
    message: NativeRow
    sender: NativeRow | None = None
    chats: tuple[NativeRow, ...]
    participants: tuple[NativeRow, ...]
    attachments: tuple[NativeRow, ...]
    chat_memberships: tuple[NativeRow, ...] = Field(alias="chatMemberships")
    body_fidelity: dict[
        Literal["nativeTextOnly", "attributedBodyUndecoded", "noTextAvailable"], Empty
    ] = Field(alias="bodyFidelity")

    @property
    def native_id(self) -> str:
        """Original Messages GUID for admission identity comparison."""
        return self.guid

    @model_validator(mode="after")
    def coherent(self) -> "NativeMessage":
        """Require source GUID and fidelity to agree with retained native fields."""
        if native_text(self.message.fields, "guid") != self.guid:
            raise ValueError("Native message GUID disagrees with its row")
        body = self.message.fields.get("attributedBody")
        expected = (
            "attributedBodyUndecoded"
            if body is not None and isinstance(body.root, BinaryField)
            else "noTextAvailable"
            if native_text(self.message.fields, "text") is None
            else "nativeTextOnly"
        )
        if tuple(self.body_fidelity) != (expected,):
            raise ValueError("Native message fidelity disagrees with retained body")
        return self


class NativeNote(NativeModel):
    """Versioned Notes observation with locked-content withholding."""

    schema_version: Annotated[int, Field(strict=True, ge=1, le=1)] = Field(alias="schemaVersion")
    note: NoteRow
    account: NoteRow | None = None
    folder: NoteRow | None = None
    attachments: tuple[NoteRow, ...]
    compressed_body: str | None = Field(default=None, alias="compressedBody")
    fidelity: dict[
        Literal["compressedBodyUndecoded", "bodyUnavailable", "lockedBodyWithheld"], Empty
    ]

    @property
    def native_id(self) -> str:
        """Original Notes identifier, proven nonempty by envelope validation."""
        value = native_text(self.note.fields, "ZIDENTIFIER")
        assert value is not None
        return value

    @property
    def locked(self) -> bool:
        """Local password-protection evidence; not provider deletion."""
        return native_integer(self.note.fields, "ZISPASSWORDPROTECTED") != 0

    @property
    def marked_for_deletion(self) -> bool:
        """Local marked-deletion evidence; not proven global provider deletion."""
        return native_integer(self.note.fields, "ZMARKEDFORDELETION") != 0

    @model_validator(mode="after")
    def coherent(self) -> "NativeNote":
        """Fail closed on absent flags, inconsistent fidelity or withheld body."""
        if not native_text(self.note.fields, "ZIDENTIFIER"):
            raise ValueError("Native note requires its original identifier")
        locked = native_integer(self.note.fields, "ZISPASSWORDPROTECTED")
        deleted = native_integer(self.note.fields, "ZMARKEDFORDELETION")
        if locked is None or deleted is None:
            raise ValueError("Native note lacks lifecycle evidence")
        expected = (
            "lockedBodyWithheld"
            if locked
            else "bodyUnavailable"
            if self.compressed_body is None
            else "compressedBodyUndecoded"
        )
        if tuple(self.fidelity) != (expected,):
            raise ValueError("Native note fidelity disagrees with lifecycle/body")
        if locked:
            # The reader intentionally emits only this closed metadata allowlist.
            safe = {
                "Z_PK",
                "Z_ENT",
                "ZIDENTIFIER",
                "ZFOLDER",
                "ZISPASSWORDPROTECTED",
                "ZMARKEDFORDELETION",
                "ZACCOUNT7",
                "ZACCOUNT4",
                "ZACCOUNT3",
                "ZTITLE1",
                "ZISPINNED",
                "ZMODIFICATIONDATE1",
                "ZCREATIONDATE1",
                "ZCREATIONDATE3",
            }
            if (
                self.compressed_body is not None
                or self.attachments
                or self.note.fields.keys() - safe
                or any(isinstance(field.root, BinaryField) for field in self.note.fields.values())
            ):
                raise ValueError("Locked note contains withheld content")
            safe_related = {
                "Z_PK",
                "Z_ENT",
                "ZIDENTIFIER",
                "ZOWNER",
                "ZPARENT",
                "ZNAME",
                "ZTITLE2",
                "ZUSERRECORDNAME",
                "ZMARKEDFORDELETION",
            }
            if any(
                related is not None
                and (
                    related.fields.keys() - safe_related
                    or any(isinstance(field.root, BinaryField) for field in related.fields.values())
                )
                for related in (self.account, self.folder)
            ):
                raise ValueError("Locked note relationships contain withheld content")
        if self.compressed_body is not None:
            BinaryValue(_0=self.compressed_body)
        return self


class NativeContactHandle(NativeModel):
    """Contacts framework label identity and raw value; no normalization/merging."""

    native_label_id: StrictStr = Field(alias="nativeLabelID", min_length=1)
    label: StrictStr | None = None
    raw_value: StrictStr = Field(alias="rawValue")


class ContactFields(NativeModel):
    """Individual local Contacts identity and precisely requested source fields."""

    native_id: StrictStr = Field(alias="nativeID", min_length=1)
    name_prefix: StrictStr = Field(alias="namePrefix")
    given_name: StrictStr = Field(alias="givenName")
    middle_name: StrictStr = Field(alias="middleName")
    family_name: StrictStr = Field(alias="familyName")
    name_suffix: StrictStr = Field(alias="nameSuffix")
    nickname: StrictStr
    organization_name: StrictStr = Field(alias="organizationName")
    phones: tuple[NativeContactHandle, ...]
    emails: tuple[NativeContactHandle, ...]


class NativeContact(NativeModel):
    """Versioned acquisition wrapper around an individual framework contact."""

    schema_version: Annotated[int, Field(strict=True, ge=1, le=1)] = Field(alias="schemaVersion")
    contact: ContactFields

    @property
    def native_id(self) -> str:
        """Local framework identifier for admission comparison, not a person ID."""
        return self.contact.native_id


DeviceSourceKind = Literal["imessage", "apple_notes", "apple_contacts"]
DeviceOriginal = NativeMessage | NativeNote | NativeContact


class DeviceOriginalEnvelope(NativeModel):
    """Exact backend-admitted ownership envelope around untouched native original."""

    authority: Literal["device"]
    source_kind: DeviceSourceKind
    account_id: StrictStr = Field(min_length=1, max_length=255)
    original: dict[str, JsonValue]


def validate_device_original(
    source_kind: DeviceSourceKind, original: dict[str, JsonValue]
) -> DeviceOriginal:
    """Validate without mutating/reserializing the original; caller compares native_id."""
    if source_kind == "imessage":
        return NativeMessage.model_validate(original)
    if source_kind == "apple_notes":
        return NativeNote.model_validate(original)
    if source_kind == "apple_contacts":
        return NativeContact.model_validate(original)
    raise ValueError("Device source payload version is not supported")
