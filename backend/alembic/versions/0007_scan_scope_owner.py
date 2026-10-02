"""Exact parent ownership for nested capture scopes.

Revision ID: 0007
Revises: 0006
"""

import json
from typing import Annotated, Literal
from uuid import UUID

import sqlalchemy as sa
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    model_validator,
)

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None

Kind = Annotated[StrictStr, Field(min_length=1, max_length=200)]


class LegacyConfiguration(BaseModel):
    """Frozen schema1 migration contract, independent of future runtime models."""

    model_config = ConfigDict(extra="forbid")
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    root_record_type: Kind
    child_record_types: list[Kind] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def distinct(self):
        """A flat root cannot be its own child or have duplicate child declarations."""
        if self.root_record_type in self.child_record_types or len(
            set(self.child_record_types)
        ) != len(self.child_record_types):
            raise ValueError("Invalid legacy flat topology")
        return self


class LegacyVersion(BaseModel):
    """Exact existing checkpoint boundary."""

    model_config = ConfigDict(extra="forbid")
    cycle_id: UUID
    revision: StrictInt = Field(ge=1)


class LegacyCycle(BaseModel):
    """Only active schema1 snapshots authorize ownership backfill."""

    model_config = ConfigDict(extra="forbid")
    schema_version: StrictInt = Field(ge=1, le=1)
    version: LegacyVersion
    configuration: LegacyConfiguration
    phase: Literal["active"]
    root_writer_attempt_id: UUID | None = None
    completed_job_id: UUID | None = None


def bind_legacy_scans(connection):
    """Unresolved or malformed rows retain their original bytes for explicit recovery."""
    cursors = connection.execute(
        sa.text("SELECT organization_id,sync_id,cursor_data FROM sync_cursor")
    ).mappings()
    for cursor in cursors:
        value = cursor["cursor_data"]
        if not isinstance(value, dict):
            continue
        try:
            cycle = LegacyCycle.model_validate(value.get("canonical_cycle"))
        except ValidationError:
            continue
        scope = {
            "org": cursor["organization_id"],
            "sync": cursor["sync_id"],
            "cycle": cycle.version.cycle_id,
        }
        rows = (
            connection.execute(
                sa.text("""
            SELECT id,scope_key,record_type,container_id FROM capture_scan
            WHERE organization_id=:org AND sync_id=:sync AND cycle_id=:cycle
        """),
                scope,
            )
            .mappings()
            .all()
        )
        for row in rows:
            expected = json.dumps(
                [row["record_type"], row["container_id"]], ensure_ascii=False, separators=(",", ":")
            )
            if row["scope_key"] != expected:
                continue
            if (
                row["container_id"] is None
                and row["record_type"] == cycle.configuration.root_record_type
            ):
                connection.execute(
                    sa.text("UPDATE capture_scan SET membership_attempt_id=:attempt WHERE id=:id"),
                    {"attempt": cycle.root_writer_attempt_id, "id": row["id"]},
                )
            elif (
                row["container_id"] is not None
                and row["record_type"] in cycle.configuration.child_record_types
            ):
                parent = (
                    connection.execute(
                        sa.text("""
                    SELECT id,visibility_epoch FROM entity
                    WHERE organization_id=:org AND sync_id=:sync
                    AND entity_definition_short_name=:kind AND native_id=:native
                    AND container_id IS NULL AND record_revision>0
                """),
                        {
                            "org": scope["org"],
                            "sync": scope["sync"],
                            "kind": cycle.configuration.root_record_type,
                            "native": row["container_id"],
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if parent is not None:
                    connection.execute(
                        sa.text("""
                        UPDATE capture_scan SET parent_record_id=:parent,
                        parent_visibility_epoch=:epoch,
                        scope_key=:key WHERE id=:id
                    """),
                        {
                            "parent": parent["id"],
                            "epoch": parent["visibility_epoch"],
                            "key": expected + "|" + str(parent["id"]),
                            "id": row["id"],
                        },
                    )


def upgrade():
    """Bind old flat scans only to validated, declared tenant-scoped root records."""
    op.add_column(
        "capture_scan",
        sa.Column("parent_record_id", sa.Uuid(), sa.ForeignKey("entity.id", ondelete="CASCADE")),
    )
    op.add_column("capture_scan", sa.Column("parent_visibility_epoch", sa.BigInteger()))
    op.add_column("capture_scan", sa.Column("membership_attempt_id", sa.Uuid()))
    bind_legacy_scans(op.get_bind())
    op.create_index(
        "idx_capture_scan_parent",
        "capture_scan",
        ["sync_id", "parent_record_id", "record_type"],
        unique=True,
        postgresql_where=sa.text("parent_record_id IS NOT NULL"),
    )


def downgrade():
    """Nested recovery cannot safely be interpreted by the flat engine."""
    raise RuntimeError("Nested capture scopes require explicit retirement before downgrade")
