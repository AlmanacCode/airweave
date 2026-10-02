"""Bounded publisher page commands and durable acknowledgements."""

import json
from typing import Literal
from uuid import UUID

from pydantic import Field, JsonValue, field_validator, model_validator

from airweave.domains.entities.canonical.requests import CompletedScope, ScanVersion
from airweave.domains.native_ingestion.models import NativeModel, NativeSnapshot


class CommitNativePage(NativeModel):
    """One in-flight page per scope; preserve this exact body on an uncertain retry."""

    page_id: UUID
    scope: CompletedScope
    expected: ScanVersion
    snapshots: tuple[NativeSnapshot, ...] = Field(max_length=500)
    cursor: dict[str, JsonValue] = Field(default_factory=dict)
    final: bool = False

    @field_validator("cursor")
    @classmethod
    def bounded_cursor(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Leave space for the receipt inside the existing 64KiB scan continuation."""
        if len(json.dumps(value, allow_nan=False).encode()) > 32768:
            raise ValueError("Native page cursor exceeds 32KiB")
        return value

    @model_validator(mode="after")
    def unique_snapshots(self) -> "CommitNativePage":
        """Reject duplicate originals before attempting any page writes."""
        keys = [(item.identity.record_type, item.identity.entity_key) for item in self.snapshots]
        if len(keys) != len(set(keys)):
            raise ValueError("Native page contains duplicate originals")
        return self


class NativePageAck(NativeModel):
    """Durable capture acknowledgement, not search publication or import completion."""

    page_id: UUID
    version: ScanVersion
    phase: Literal["collecting", "reconciling", "complete"]
    sequence: int
    changed: int
    unchanged: int


class NativePageReceipt(NativeModel):
    """Only the most recently committed page is retryable with identical success."""

    schema_version: Literal[1] = 1
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    acknowledgement: NativePageAck
