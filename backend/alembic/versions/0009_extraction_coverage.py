"""Generation-owned extraction coverage; legacy publications remain unknown."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade():
    """Old generations remain explicitly unknown until reprojection."""
    op.add_column(
        "projection_generation", sa.Column("extraction_coverage", postgresql.JSONB(), nullable=True)
    )


def downgrade():
    """Remove derived evidence only on explicit downgrade."""
    op.drop_column("projection_generation", "extraction_coverage")
