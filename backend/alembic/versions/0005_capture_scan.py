"""Durable whole-scope capture progress.

Revision ID: 0005
Revises: 0004
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade():
    """Keep one bounded continuation per source scope, not per job."""
    op.create_table(
        "capture_scan",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("modified_at", sa.DateTime(), nullable=False),
        sa.Column(
            "organization_id",
            sa.Uuid(),
            sa.ForeignKey("organization.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "sync_id", sa.Uuid(), sa.ForeignKey("sync.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("scope_key", sa.String(), nullable=False),
        sa.Column("record_type", sa.String(), nullable=False),
        sa.Column("container_id", sa.String()),
        sa.Column("cycle_id", sa.Uuid(), nullable=False),
        sa.Column("sweep_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("phase", sa.String(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("continuation", postgresql.JSONB(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("sync_id", "scope_key", name="uq_capture_scan_scope"),
        sa.CheckConstraint(
            "phase IN ('collecting','reconciling','complete')", name="ck_capture_scan_phase"
        ),
        sa.CheckConstraint("revision >= 1", name="ck_capture_scan_revision"),
    )
    op.create_index("ix_capture_scan_organization_id", "capture_scan", ["organization_id"])


def downgrade():
    """Active recovery obligations require an explicit operator decision."""
    raise RuntimeError("Capture scans require explicit completion or abandonment before removal")
