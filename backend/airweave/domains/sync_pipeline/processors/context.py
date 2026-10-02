"""Minimal processing dependencies shared by capture-era and replay processing."""

from typing import Protocol

from airweave.core.logging import ContextualLogger


class ProcessingContext(Protocol):
    """Identity and diagnostics needed by text conversion/chunking."""

    logger: ContextualLogger
    source_short_name: str


class ProcessingTracker(Protocol):
    """Existing processing skip accounting interface."""

    async def record_skipped(self, count: int) -> None:
        """Record that input could not be converted."""
        ...


class ProcessingRuntime(Protocol):
    """Processing never requires a live source or its credentials."""

    entity_tracker: ProcessingTracker
