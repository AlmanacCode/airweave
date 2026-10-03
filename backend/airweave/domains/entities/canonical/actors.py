"""Exact observed native handles; neither identity resolution nor authorization."""

import hashlib
import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictStr, model_validator
from sqlalchemy import cast, func
from sqlalchemy.dialects.postgresql import JSONB, JSONPATH

from airweave.domains.entities.canonical.apple_payloads import (
    DeviceOriginalEnvelope,
    NativeContact,
    NativeMessage,
    native_text,
)
from airweave.domains.entities.canonical.contact_preparation import phone_endpoint_key

ACTOR_PIPELINE_VERSION = 5
ActorRole = Literal["sender", "current_chat_member", "contact_handle"]
ROLE_SOURCES: dict[ActorRole, str] = {
    "sender": "imessage",
    "current_chat_member": "imessage",
    "contact_handle": "apple_contacts",
}


class ActorFilter(BaseModel):
    """Exact role and raw native spelling; all requested filters must match."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    role: ActorRole
    handle: StrictStr = Field(min_length=1, max_length=1024)

    @property
    def token(self) -> str:
        """Domain-separated, unambiguous SHA256 attribute term, never an access token."""
        return actor_token(self.role, self.handle)


class ActorAnyOf(BaseModel):
    """One observed role with raw or qualified endpoint alternatives; no identity inference."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    role: ActorRole
    match: Literal["raw", "endpoint"] = "raw"
    handles: tuple[Annotated[StrictStr, Field(min_length=1, max_length=1024)], ...] = Field(
        min_length=1, max_length=20
    )

    @model_validator(mode="after")
    def unique_handles(self):
        """Duplicates are rejected without normalizing raw spelling."""
        if len(set(self.handles)) != len(self.handles):
            raise ValueError("Actor any-of handles must be unique")
        return self

    @property
    def tokens(self) -> tuple[str, ...]:
        """Use raw terms or qualified endpoint terms in the existing actor attribute."""
        return tuple(
            sorted({actor_match_token(self.role, handle, self.match) for handle in self.handles})
        )


def actor_token(role: ActorRole, handle: str) -> str:
    """Hash observed values without normalizing or truncating their raw spelling."""
    return hashlib.sha256(
        json.dumps(
            ["native-actor-v1", role, handle], ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def actor_match_token(role: ActorRole, handle: str, match: Literal["raw", "endpoint"]) -> str:
    """Endpoint terms share the existing attribute but have a distinct versioned namespace."""
    endpoint = phone_endpoint_key(handle) if match == "endpoint" else None
    if endpoint is None:
        return actor_token(role, handle)
    return hashlib.sha256(
        json.dumps(
            ["native-actor-endpoint-v1", role, *endpoint], ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _matching_key(handle: str, match: Literal["raw", "endpoint"]) -> tuple[str, str, str | None]:
    endpoint = phone_endpoint_key(handle) if match == "endpoint" else None
    return ("phone", *endpoint) if endpoint is not None else ("raw", handle, None)


class ActorHandles(BaseModel):
    """Narrow SQL observation view; names and original bodies are not selected."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sender_handle: StrictStr | None = None
    current_chat_handles: tuple[StrictStr, ...] = ()
    contact_handles: tuple[StrictStr, ...] = ()

    def matches(self, actor: ActorFilter) -> bool:
        """Compare raw values exactly, preserving casing, spacing and country ambiguity."""
        if actor.role == "sender":
            return self.sender_handle == actor.handle
        if actor.role == "current_chat_member":
            return actor.handle in self.current_chat_handles
        return actor.handle in self.contact_handles

    def matches_any(self, actor: ActorAnyOf) -> bool:
        """Compare current native observations independently of indexed token matches."""
        current = (
            (self.sender_handle,)
            if actor.role == "sender" and self.sender_handle is not None
            else self.current_chat_handles
            if actor.role == "current_chat_member"
            else self.contact_handles
            if actor.role == "contact_handle"
            else ()
        )
        alternatives = {_matching_key(value, actor.match) for value in actor.handles}
        return any(_matching_key(value, actor.match) in alternatives for value in current)

    @property
    def tokens(self) -> tuple[str, ...]:
        """Stable distinct observations, retaining separate role semantics."""
        values = []
        if self.sender_handle:
            values.extend(
                (
                    actor_token("sender", self.sender_handle),
                    actor_match_token("sender", self.sender_handle, "endpoint"),
                )
            )
        for role, handles in (
            ("current_chat_member", self.current_chat_handles),
            ("contact_handle", self.contact_handles),
        ):
            values.extend(
                token
                for value in handles
                if value
                for token in (actor_token(role, value), actor_match_token(role, value, "endpoint"))
            )
        return tuple(sorted(set(values)))


def original_actor_handles(source: str | None, original: dict[str, JsonValue]) -> ActorHandles:
    """Read validated individual native records; never merge contacts by names/handles."""
    if source == "imessage":
        message = NativeMessage.model_validate(original)
        return ActorHandles(
            sender_handle=native_text(message.sender.fields, "id") if message.sender else None,
            current_chat_handles=tuple(
                value
                for row in message.participants
                if (value := native_text(row.fields, "id")) is not None
            ),
        )
    if source == "apple_contacts":
        contact = NativeContact.model_validate(original).contact
        return ActorHandles(
            contact_handles=tuple(handle.raw_value for handle in (*contact.phones, *contact.emails))
        )
    return ActorHandles()


def committed_actor_handles(source: str | None, payload: dict[str, JsonValue]) -> ActorHandles:
    """Require the exact admitted device envelope; no bare-native compatibility path."""
    if source not in ("imessage", "apple_contacts"):
        return ActorHandles()
    envelope = DeviceOriginalEnvelope.model_validate(payload)
    if envelope.source_kind != source:
        raise ValueError("Actor source differs from admitted envelope")
    return original_actor_handles(source, envelope.original)


def actor_sql_columns(payload):
    """Extract only exact JSON scalar handle subfields, without materializing bodies."""
    original = payload["original"]
    return (
        original["sender"]["fields"]["id"]["text"]["_0"].astext.label("sender_handle"),
        func.jsonb_path_query_array(
            original, cast("$.participants[*].fields.id.text._0", JSONPATH), type_=JSONB
        ).label("current_chat_handles"),
        func.jsonb_path_query_array(
            original, cast("$.contact.*[*].rawValue", JSONPATH), type_=JSONB
        ).label("contact_handles"),
    )
