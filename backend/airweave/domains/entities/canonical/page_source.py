"""Opted-in whole-scope pages; the existing capture pipeline owns durable progress."""

from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation


class CapturePage(BaseModel):
    """One provider response and the continuation committed with its original records."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    records: tuple[CaptureRecord, ...] = Field(max_length=500)
    continuation: ScanContinuation
    final: bool = False


class InvalidScanContinuation(Exception):
    """The provider rejected a saved cursor; restart the whole scope, never reconcile it."""


class ScopeAccessLost(Exception):
    """Provider explicitly confirmed access loss for the requested child container."""


@runtime_checkable
class CanonicalPageSource(Protocol):
    """A source fetches pages; SQL, attempts, scope discovery and checkpointing stay outside it."""

    canonical_record_types: tuple[str, ...]
    canonical_container_parents: dict[str, str]
    capture_cycle_configuration: CycleConfiguration

    async def capture_page(
        self, scope: CompletedScope, continuation: ScanContinuation
    ) -> CapturePage:
        """Fetch one page without advancing durable state or performing writes."""
        ...

    async def confirm_root_absent(self, native_id: str) -> None:
        """Raise unless an omitted previously captured root is confirmed inaccessible."""
        ...
