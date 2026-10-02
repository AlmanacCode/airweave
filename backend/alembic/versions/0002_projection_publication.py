"""Revision-safe canonical index publication pointer.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    """Add a publication pointer; old index state remains untrusted until rebuilt."""
    op.add_column("entity", sa.Column("indexed_generation", sa.Uuid(), nullable=True))
    op.add_column("entity", sa.Column("indexed_chunk_count", sa.BigInteger(), nullable=True))


def downgrade():
    """Remove only derived publication state, preserving source records."""
    op.drop_column("entity", "indexed_chunk_count")
    op.drop_column("entity", "indexed_generation")
