"""Test-only process loss after an acknowledged Gmail page; never copies provider state."""

import json
import os

from capture_resume import ResumeTarget
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import CaptureCycle
from airweave.models.capture_scan import CaptureScan
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob


class GmailResumeProbe:
    """Interrupt at the next page boundary; verify one further commit after a fresh process."""

    def __init__(
        self, sessions, organization_id, sync_id, job_id, attempt_id, target_path, mode, counters
    ):
        self.sessions = sessions
        self.organization_id, self.sync_id = organization_id, sync_id
        self.job_id, self.attempt_id = job_id, attempt_id
        self.target_path, self.mode, self.counters = target_path, mode, counters
        self.target = (
            ResumeTarget.model_validate_json(target_path.read_text()) if mode == "resume" else None
        )
        self.resumed_saved_page = False
        self.saved_cursor_expired = False

    async def before_page(self, scope, continuation):
        if scope.record_type != "message" or scope.parent is not None:
            raise AssertionError("Gmail recovery requires the independent message scope")
        async with self.sessions() as db:
            row = await db.scalar(
                select(CaptureScan).where(
                    CaptureScan.organization_id == self.organization_id,
                    CaptureScan.sync_id == self.sync_id,
                    CaptureScan.record_type == "message",
                    CaptureScan.container_id.is_(None),
                    CaptureScan.parent_record_id.is_(None),
                )
            )
            cursor = await db.scalar(
                select(SyncCursor).where(
                    SyncCursor.organization_id == self.organization_id,
                    SyncCursor.sync_id == self.sync_id,
                )
            )
            sync, job = await db.get(Sync, self.sync_id), await db.get(SyncJob, self.job_id)
            assert row is not None and cursor is not None and sync is not None and job is not None
            cycle = CaptureCycle.model_validate(cursor.cursor_data["canonical_cycle"])
            assert sync.organization_id == job.organization_id == self.organization_id
            assert job.sync_id == self.sync_id and job.status == "running"
            assert sync.writer_job_id == self.job_id and sync.writer_attempt_id == self.attempt_id
            assert cycle.phase == "active" and cycle.mode == "full"
            assert cycle.starting_checkpoint is not None
            assert row.cycle_id == cycle.version.cycle_id and row.phase == "collecting"
            assert row.continuation == continuation.value
            target = ResumeTarget(
                scan_id=row.id, cycle_id=row.cycle_id, sweep_id=row.sweep_id, revision=row.revision
            )
        if self.mode == "interrupt":
            if not self.counters["records_observed"]:
                return
            self.target_path.write_text(target.model_dump_json())
            os.chmod(self.target_path, 0o600)
            self._stop(False)
        assert self.counters["capture_profile_requests"] == 0, (
            "Resume fetched another starting boundary"
        )
        if target == self.target:
            self.resumed_saved_page = True
        elif self.resumed_saved_page and not self.saved_cursor_expired:
            assert (target.scan_id, target.cycle_id, target.sweep_id) == (
                self.target.scan_id,
                self.target.cycle_id,
                self.target.sweep_id,
            )
            assert target.revision > self.target.revision
            self._stop(True)

    def cursor_expired(self):
        self.saved_cursor_expired = True

    def _stop(self, verified):
        print(
            json.dumps(
                {
                    "failed": False,
                    "counters_complete": True,
                    "intentional_interruption": not verified,
                    "gmail_recovery_verified": verified,
                    "resumed_saved_page": self.resumed_saved_page,
                    "saved_cursor_expired": self.saved_cursor_expired,
                    "full_scope_completed": False,
                    "checkpoint_saved": False,
                    "job_status": "running",
                    **self.counters,
                }
            ),
            flush=True,
        )
        os._exit(76 if verified else 75)
