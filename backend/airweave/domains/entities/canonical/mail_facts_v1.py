"""Version-one Gmail facts decoder, frozen for migration 0014 backfill stability."""

from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import HeaderParser

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError


class MailAddress(BaseModel):
    """RFC mailbox with preserved display name and casefolded exact address."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    address: str = Field(min_length=1)
    name: str = ""


class GmailMetadata(BaseModel):
    """Derived native facts; the retained message remains their authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    native_id: str = Field(min_length=1)
    thread_id: str = Field(min_length=1)
    subject: str
    subject_folded: str
    sender: tuple[MailAddress, ...]
    to: tuple[MailAddress, ...]
    sent_at: AwareDatetime
    labels: tuple[str, ...]
    snippet: str | None


class _Header(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    value: str


class _Payload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    headers: tuple[_Header, ...]


class _Message(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(min_length=1)
    threadId: str = Field(min_length=1)
    internalDate: str = Field(pattern=r"^-?[0-9]{1,18}$")
    labelIds: tuple[str, ...]
    snippet: str | None = None
    payload: _Payload


def _addresses(headers: tuple[_Header, ...], name: str) -> tuple[MailAddress, ...]:
    values = [header.value for header in headers if header.name.casefold() == name]
    if not values:
        return ()
    result = []
    for value in values:
        parsed = HeaderParser(policy=policy.default).parsestr(f"{name}: {value}\n\n")[name]
        if parsed is None or parsed.defects:
            raise ValueError("Malformed retained mailbox header")
        for item in parsed.addresses:
            if not item.username or not item.domain:
                raise ValueError("Retained mailbox has no complete address")
            result.append(MailAddress(address=item.addr_spec.casefold(), name=item.display_name))
    return tuple(result)


def gmail_metadata_v1(
    payload: dict, native_id: str, source_created_at: datetime | None
) -> GmailMetadata | None:
    """Missing/malformed or inconsistent facts stay unknown, never a verified nonmatch."""
    try:
        message = _Message.model_validate(payload)
        sent_at = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            milliseconds=int(message.internalDate)
        )
        if message.id != native_id or source_created_at != sent_at:
            return None
        subject = next(
            (
                header.value
                for header in message.payload.headers
                if header.name.casefold() == "subject"
            ),
            "",
        )
        if subject:
            parsed_subject = HeaderParser(policy=policy.default).parsestr(
                f"Subject: {subject}\n\n"
            )["subject"]
            if parsed_subject is None or parsed_subject.defects:
                return None
            subject = str(parsed_subject)
        sender = _addresses(message.payload.headers, "from")
        if not sender:
            return None
        return GmailMetadata(
            native_id=message.id,
            thread_id=message.threadId,
            subject=subject,
            subject_folded=subject.casefold(),
            sender=sender,
            to=_addresses(message.payload.headers, "to"),
            sent_at=sent_at,
            labels=message.labelIds,
            snippet=message.snippet,
        )
    except (ValidationError, ValueError, OverflowError):
        return None
