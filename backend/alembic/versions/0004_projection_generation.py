"""Durable prepared feed manifests and repeatable retired-generation cleanup.

Revision ID: 0004
Revises: 0003
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade():
    """Keep body-free ownership IDs without cascading FKs so deletion survives source removal."""
    op.create_table(
        "projection_generation",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("modified_at", sa.DateTime(), nullable=False),
        *(
            sa.Column(name, sa.Uuid(), nullable=False)
            for name in ("organization_id", "sync_id", "collection_id", "record_id")
        ),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("pipeline_version", sa.BigInteger(), nullable=False),
        sa.Column("documents", postgresql.JSONB(), nullable=False),
        sa.Column("retired_at", sa.DateTime(timezone=True)),
        sa.Column("next_gc_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_gc_at", sa.DateTime(timezone=True)),
        sa.Column("delete_cursor", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("gc_passes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("gc_attempt", sa.Uuid()),
        sa.Column("gc_error", sa.String(200)),
    )
    for name in ("organization_id", "sync_id", "record_id", "next_gc_at"):
        op.create_index(f"ix_projection_generation_{name}", "projection_generation", [name])
    op.create_index("idx_projection_gc_due", "projection_generation", ["next_gc_at", "id"])


def downgrade():
    """Do not silently discard cleanup obligations."""
    raise RuntimeError("Projection generation manifests require explicit cleanup before removal")
