"""Exact owner verification on the existing scope scan.

Revision ID: 0012
Revises: 0011
"""

import sqlalchemy as sa

from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade():
    """Old scans have no verification; never backfill unproven permission."""
    op.add_column("capture_scan", sa.Column("parent_verified_attempt_id", sa.Uuid(), nullable=True))
    op.add_column(
        "capture_scan", sa.Column("parent_verified_revision", sa.BigInteger(), nullable=True)
    )


def downgrade():
    """Remove only admission receipts, not captured originals."""
    op.drop_column("capture_scan", "parent_verified_revision")
    op.drop_column("capture_scan", "parent_verified_attempt_id")
