"""Test-only committed-body interruption; no provider content in evidence or output."""

import hashlib
import json
import os
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob


class CompletedBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    scan_id: UUID
    revision: int
    native_digest: str


class WisprTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    bodies: tuple[CompletedBody, ...]


class WisprResumeProbe:
    """Stop before another call, only after SQL proves prior bodies committed."""

    resumed_saved_page = False
    saved_cursor_expired = False

    def __init__(
        self, sessions, organization_id, sync_id, job_id, attempt_id, target_path, mode, counters
    ):
        self.sessions = sessions
        self.organization_id, self.sync_id = organization_id, sync_id
        self.job_id, self.attempt_id = job_id, attempt_id
        self.target_path, self.mode, self.counters = target_path, mode, counters
        self.target = (
            WisprTarget.model_validate_json(target_path.read_text()) if mode == "resume" else None
        )

    async def before_page(self, scope, continuation):
        if scope.record_type != "meeting":
            return
        async with self.sessions() as db:
            sync, job = await db.get(Sync, self.sync_id), await db.get(SyncJob, self.job_id)
            assert sync.organization_id == job.organization_id == self.organization_id
            assert sync.writer_job_id == job.id and sync.writer_attempt_id == self.attempt_id
            assert job.status == "running"
            rows = (
                await db.execute(
                    select(CaptureScan, Entity.native_id)
                    .join(Entity, Entity.id == CaptureScan.parent_record_id)
                    .where(
                        CaptureScan.organization_id == self.organization_id,
                        CaptureScan.sync_id == self.sync_id,
                        CaptureScan.record_type == "meeting",
                        CaptureScan.phase == "complete",
                    )
                )
            ).all()
        bodies = tuple(
            CompletedBody(
                scan_id=row.id,
                revision=row.revision,
                native_digest=hashlib.sha256(native.encode()).hexdigest(),
            )
            for row, native in rows
        )
        cycles = {row.cycle_id for row, _ in rows}
        if self.target is not None:
            assert cycles == {self.target.cycle_id}
            assert all(body in bodies for body in self.target.bodies), "Completed sibling changed"
            digest = hashlib.sha256(scope.parent.native_id.encode()).hexdigest()
            assert digest not in {body.native_digest for body in self.target.bodies}, (
                "Completed body repeated"
            )
            if len(bodies) == len(self.target.bodies) + 1:
                self._stop(True)
        elif len(bodies) == 3:
            assert len(cycles) == 1
            self.target_path.write_text(
                WisprTarget(cycle_id=next(iter(cycles)), bodies=bodies).model_dump_json()
            )
            os.chmod(self.target_path, 0o600)
            self._stop(False)

    def _stop(self, verified):
        print(
            json.dumps(
                {
                    "failed": False,
                    "counters_complete": True,
                    "intentional_interruption": not verified,
                    "wispr_recovery_verified": verified,
                    "full_scope_completed": False,
                    "completed_bodies": 4 if verified else 3,
                    **self.counters,
                }
            ),
            flush=True,
        )
        os._exit(76 if verified else 75)
