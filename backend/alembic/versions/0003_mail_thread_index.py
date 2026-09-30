"""Index account-scoped Gmail thread traversal over original provider JSON."""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Keep thread lookup proportional to one account/thread, not its entire mailbox."""
    op.execute("""
        CREATE INDEX ix_entity_mail_thread ON entity
        (organization_id, sync_id, (source_payload ->> 'threadId'),
         source_created_at ASC NULLS LAST, id)
        WHERE entity_definition_short_name = 'message'
          AND record_revision > 0 AND deleted_at IS NULL
    """)


def downgrade() -> None:
    """Drop only the derived lookup index; canonical payloads remain unchanged."""
    op.drop_index("ix_entity_mail_thread", table_name="entity")
