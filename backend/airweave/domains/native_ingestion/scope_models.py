"""Publisher-visible scope progress, without storage receipts or writer credentials."""

from typing import Literal
from uuid import UUID

from pydantic import Field, JsonValue

from airweave.domains.entities.canonical.page_receipts import PageAcknowledgement
from airweave.domains.entities.canonical.requests import CompletedScope, ScanVersion
from airweave.domains.native_ingestion.models import NativeModel


class NativeScopeRef(NativeModel):
    """Exact native container identity; a read does not start or restart work."""

    scope: CompletedScope


class BeginNativeScope(NativeScopeRef):
    """Resume an unchanged scope; explicit same-import restarts require its version."""

    expected: ScanVersion | None = None
    restart: bool = False


class ReconcileNativeScope(NativeScopeRef):
    """Advance bounded reconciliation only after the final page is committed."""

    expected: ScanVersion
    limit: int = Field(default=250, ge=1, le=500)


class NativeScopeState(NativeModel):
    """Capture progress; complete refers to this scope's declared coverage policy."""

    scope: CompletedScope
    cycle_id: UUID
    version: ScanVersion
    phase: Literal["collecting", "reconciling", "complete"]
    coverage: Literal["bounded", "complete"]
    cursor: dict[str, JsonValue]
    last_page: PageAcknowledgement | None = None
