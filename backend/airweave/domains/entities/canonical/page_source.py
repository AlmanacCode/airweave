"""Opted-in scope pages; the existing capture pipeline owns durable progress."""

from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    ScopeRemovalReason,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.storage.file_service import FileService


class CapturePage(BaseModel):
    """One provider response and the continuation committed with its original records."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    records: tuple[CaptureRecord, ...] = Field(max_length=500)
    continuation: ScanContinuation
    final: bool = False


class InvalidScanContinuation(Exception):
    """The provider rejected a saved cursor; restart the whole scope, never reconcile it."""


class ScopeAccessLost(Exception):
    """The source's audited contract says this child container is no longer available."""

    def __init__(
        self,
        message: str,
        *,
        removal_reason: ScopeRemovalReason = "access_revoked",
    ):
        """Slack keeps its confirmed-access default; other sources must choose explicitly."""
        super().__init__(message)
        self.removal_reason = TypeAdapter(ScopeRemovalReason).validate_python(removal_reason)


@runtime_checkable
class CanonicalPageSource(Protocol):
    """A source fetches pages; SQL, attempts, scope discovery and checkpointing stay outside it."""

    canonical_record_types: tuple[str, ...]
    canonical_container_parents: dict[str, str | tuple[str | None, ...]]
    capture_cycle_configuration: CycleConfiguration

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Fetch one page without advancing durable state or performing writes."""
        ...

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Source-owned stable container policy; always retain the full parent identity."""
        ...

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Raise unless an omitted inventory record is confirmed outside accessible scope."""
        ...


@runtime_checkable
class KnownObjectSource(Protocol):
    """Only discovery-with-validation sources implement exact current reads."""

    async def refresh_known(self, record: SourceRecord, *, files: FileService) -> CaptureRecord:
        """Return exact fresh state or explicit unavailability; errors never mean absence."""
        ...
