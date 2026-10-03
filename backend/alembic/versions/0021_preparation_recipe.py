"""Preserve immutable attempt provenance; old generations remain unknown."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade():
    """No default or backfill can truthfully reconstruct a historical recipe."""
    op.add_column(
        "projection_generation",
        sa.Column("preparation_recipe", postgresql.JSONB(none_as_null=True), nullable=True),
    )


def downgrade():
    """Remove provenance without modifying originals or publication identities."""
    op.drop_column("projection_generation", "preparation_recipe")
