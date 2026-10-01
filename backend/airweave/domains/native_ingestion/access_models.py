"""Explicit source-access observations, separate from native content versions."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from airweave.domains.entities.canonical.requests import RecordIdentity, ScopeRemovalReason
from airweave.domains.native_ingestion.models import NativeModel, NativeSnapshot


class WithdrawNativeRecord(NativeModel):
    """Publisher attests exact source access loss, never an incomplete-list absence."""

    action: Literal["withdraw"] = "withdraw"
    expected_revision: int = Field(strict=True, ge=1)
    reason: ScopeRemovalReason


class RenewNativeRecord(NativeModel):
    """Publisher freshly read this original after inspecting the retained revision."""

    action: Literal["renew"] = "renew"
    expected_revision: int = Field(strict=True, ge=1)
    expected_parent_epoch: int | None = Field(default=None, strict=True, ge=1)
    snapshot: NativeSnapshot


NativeAccessChange = Annotated[
    WithdrawNativeRecord | RenewNativeRecord, Field(discriminator="action")
]


class NativeRecordAccess(NativeModel):
    """Current state for CAS/recovery, without retained original bodies or credentials."""

    record_id: UUID
    identity: RecordIdentity
    revision: int
    parent_visibility_epoch: int | None
    available: bool
    removal_reason: str | None
