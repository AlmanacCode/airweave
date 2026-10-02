"""Test-only committed-body interruption; no provider content in evidence or output."""

import hashlib
import json
import os
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import CaptureCycle
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob


class CompletedBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    scan_id: UUID
    revision: int
    record_type: Literal["meeting", "scratchpad_note"]
    native_digest: str = Field(pattern=r"^[a-f0-9]{64}$")


class WisprTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[3]
    configuration_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
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
        try:
            self.target = (
                WisprTarget.model_validate_json(target_path.read_text())
                if mode == "resume"
                else None
            )
        except ValidationError:
            raise ValueError(
                "Incompatible Wispr resume target; start a new private v3 trial"
            ) from None

    async def before_page(self, scope, continuation):
        if scope.record_type not in {"meeting", "scratchpad_note"}:
            return
        async with self.sessions() as db:
            sync, job = await db.get(Sync, self.sync_id), await db.get(SyncJob, self.job_id)
            assert sync.organization_id == job.organization_id == self.organization_id
            assert sync.writer_job_id == job.id and sync.writer_attempt_id == self.attempt_id
            assert job.status == "running"
            cursor = await db.scalar(
                select(SyncCursor).where(
                    SyncCursor.organization_id == self.organization_id,
                    SyncCursor.sync_id == self.sync_id,
                )
            )
            if cursor is None:
                raise ValueError("Wispr resume requires a persisted capture cycle")
            cycle = CaptureCycle.model_validate(cursor.cursor_data.get("canonical_cycle"))
            if cycle.configuration.parents != {
                "meeting_listing": (None,),
                "meeting": ("meeting_listing",),
                "scratchpad_listing": (None,),
                "scratchpad_note": ("scratchpad_listing",),
            }:
                raise ValueError("Wispr resume requires the v3 mixed-resource capture topology")
            rows = (
                await db.execute(
                    select(CaptureScan, Entity.native_id)
                    .join(Entity, Entity.id == CaptureScan.parent_record_id)
                    .where(
                        CaptureScan.organization_id == self.organization_id,
                        CaptureScan.sync_id == self.sync_id,
                        CaptureScan.record_type.in_(("meeting", "scratchpad_note")),
                        CaptureScan.cycle_id == cycle.version.cycle_id,
                        CaptureScan.phase == "complete",
                    )
                )
            ).all()
        bodies = tuple(
            CompletedBody(
                scan_id=row.id,
                revision=row.revision,
                record_type=row.record_type,
                native_digest=hashlib.sha256(native.encode()).hexdigest(),
            )
            for row, native in rows
        )
        cycles = {row.cycle_id for row, _ in rows}
        if self.target is not None:
            assert cycles == {self.target.cycle_id}
            assert cycle.configuration.digest() == self.target.configuration_digest, (
                "Capture configuration changed"
            )
            assert all(body in bodies for body in self.target.bodies), "Completed sibling changed"
            digest = hashlib.sha256(scope.parent.native_id.encode()).hexdigest()
            assert (scope.record_type, digest) not in {
                (body.record_type, body.native_digest) for body in self.target.bodies
            }, "Completed body repeated"
            if len(bodies) == len(self.target.bodies) + 1:
                self._stop(True, bodies)
        elif len(bodies) == 3:
            assert len(cycles) == 1
            self.target_path.write_text(
                WisprTarget(
                    schema_version=3,
                    configuration_digest=cycle.configuration.digest(),
                    cycle_id=next(iter(cycles)),
                    bodies=bodies,
                ).model_dump_json()
            )
            os.chmod(self.target_path, 0o600)
            self._stop(False, bodies)

    def _stop(self, verified, bodies):
        print(
            json.dumps(
                {
                    "failed": False,
                    "counters_complete": True,
                    "intentional_interruption": not verified,
                    "wispr_recovery_verified": verified,
                    "full_scope_completed": False,
                    "completed_bodies": len(bodies),
                    "completed_body_kinds": {
                        kind: sum(body.record_type == kind for body in bodies)
                        for kind in ("meeting", "scratchpad_note")
                    },
                    **self.counters,
                }
            ),
            flush=True,
        )
        os._exit(76 if verified else 75)
