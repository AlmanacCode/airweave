"""Test-only process-loss injection after a verified production page commit."""

import json
import os
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import CaptureCycle
from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.models.capture_scan import CaptureScan
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob


class ResumeTarget(BaseModel):
    """Private test evidence references SQL state; no duplicated cursor or record payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    scan_id: UUID
    cycle_id: UUID
    sweep_id: UUID
    revision: int


class PageResumeProbe:
    """The child exits before its next provider call, leaving the actual job RUNNING."""

    def __init__(
        self,
        sessions,
        organization_id,
        sync_id,
        job_id,
        attempt_id,
        target_path: Path,
        mode: Literal["interrupt", "resume"],
        counters,
    ):
        self.sessions = sessions
        self.organization_id, self.sync_id = organization_id, sync_id
        self.job_id, self.attempt_id = job_id, attempt_id
        self.target_path, self.mode, self.counters = target_path, mode, counters
        self.resumed_saved_page = False
        self.saved_cursor_expired = False
        self._checking_saved_page = False
        self.target = (
            ResumeTarget.model_validate_json(target_path.read_text()) if mode == "resume" else None
        )

    async def before_page(self, scope: CompletedScope, continuation: ScanContinuation):
        """Verify committed pending replies and writer/cycle ownership before fault injection."""
        self._checking_saved_page = False
        if scope.record_type != "message" or not continuation.value.get("pending_threads"):
            return
        async with self.sessions() as db:
            row = await db.scalar(
                select(CaptureScan).where(
                    CaptureScan.organization_id == self.organization_id,
                    CaptureScan.sync_id == self.sync_id,
                    CaptureScan.record_type == scope.record_type,
                    CaptureScan.container_id == scope.container_id,
                )
            )
            sync = await db.get(Sync, self.sync_id)
            job = await db.get(SyncJob, self.job_id)
            cursor = await db.scalar(
                select(SyncCursor).where(
                    SyncCursor.organization_id == self.organization_id,
                    SyncCursor.sync_id == self.sync_id,
                )
            )
            if row is None or sync is None or job is None or cursor is None:
                raise AssertionError("Durable interruption state is missing")
            cycle = CaptureCycle.model_validate(cursor.cursor_data["canonical_cycle"])
            assert sync.organization_id == self.organization_id
            assert sync.writer_job_id == self.job_id and sync.writer_attempt_id == self.attempt_id
            assert job.organization_id == self.organization_id and job.sync_id == self.sync_id
            assert job.status == "running" and cycle.phase == "active"
            root = await db.scalar(
                select(CaptureScan).where(
                    CaptureScan.organization_id == self.organization_id,
                    CaptureScan.sync_id == self.sync_id,
                    CaptureScan.cycle_id == cycle.version.cycle_id,
                    CaptureScan.record_type == "channel",
                    CaptureScan.parent_record_id.is_(None),
                    CaptureScan.container_id.is_(None),
                )
            )
            assert root is not None and root.phase == "complete"
            assert root.membership_attempt_id == self.attempt_id
            assert row.cycle_id == cycle.version.cycle_id and row.phase == "collecting"
            assert row.continuation == continuation.value
            target = ResumeTarget(
                scan_id=row.id, cycle_id=row.cycle_id, sweep_id=row.sweep_id, revision=row.revision
            )
        if self.mode == "interrupt":
            self.target_path.write_text(target.model_dump_json())
            os.chmod(self.target_path, 0o600)
            print(
                json.dumps(
                    {
                        "failed": False,
                        "counters_complete": True,
                        "intentional_interruption": True,
                        "durable_pending_threads_verified": True,
                        "job_status": "running",
                        "full_scope_completed": False,
                        "checkpoint_saved": False,
                        **self.counters,
                    }
                ),
                flush=True,
            )
            os._exit(75)  # Test child only: simulate process loss without cancellation transitions.
        if target == self.target:
            self.resumed_saved_page = True
            self._checking_saved_page = True

    def cursor_expired(self):
        """A resumed request that is rejected is distinct from successful cursor reuse."""
        if self._checking_saved_page:
            self.saved_cursor_expired = True


async def inspect_resume_progress(sessions, organization_id, sync_id, job_id, target_path: Path):
    """Parent-side readback survives child death without copying provider content or cursors."""
    target = ResumeTarget.model_validate_json(target_path.read_text())
    async with sessions() as db:
        row = await db.scalar(
            select(CaptureScan).where(
                CaptureScan.id == target.scan_id,
                CaptureScan.organization_id == organization_id,
                CaptureScan.sync_id == sync_id,
            )
        )
        sync = await db.get(Sync, sync_id)
        job = await db.get(SyncJob, job_id)
        advanced = bool(
            row is not None
            and sync is not None
            and job is not None
            and sync.organization_id == organization_id
            and job.organization_id == organization_id
            and job.sync_id == sync_id
            and sync.writer_job_id == job_id
            and sync.writer_attempt_number == 2
            and row.cycle_id == target.cycle_id
            and row.sweep_id == target.sweep_id
            and row.revision > target.revision
        )
    return {"resumed_page_committed": advanced}
