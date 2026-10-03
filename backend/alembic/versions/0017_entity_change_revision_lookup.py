"""Bound exact historical revision lookup on the existing capture journal."""

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade():
    """Index existing immutable snapshots without rewriting them."""
    op.create_index(
        "ix_entity_change_revision_lookup",
        "entity_change",
        ["organization_id", "sync_id", "entity_record_id", "record_revision"],
    )


def downgrade():
    """Remove only the lookup index."""
    op.drop_index("ix_entity_change_revision_lookup", table_name="entity_change")
