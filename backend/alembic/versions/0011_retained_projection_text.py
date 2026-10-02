"""Retain derived text descriptors with their projection publication generation."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade():
    """Old generations have unknown representation coverage until reprojected."""
    op.add_column(
        "projection_generation",
        sa.Column(
            "text_representations",
            postgresql.JSONB(none_as_null=True),
            nullable=True,
        ),
    )


def downgrade():
    """Remove descriptors; no source originals are affected."""
    op.drop_column("projection_generation", "text_representations")
