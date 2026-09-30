"""Retain completed scope evidence independently of current page progress."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade():
    """Keep existing rows explicitly without attested mixed-scope history."""
    op.add_column("capture_scan", sa.Column("execution_state", postgresql.JSONB(), nullable=True))


def downgrade():
    """Remove mixed execution evidence only on an explicit schema downgrade."""
    op.drop_column("capture_scan", "execution_state")
