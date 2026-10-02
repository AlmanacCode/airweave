"""Validated native Wispr meeting start on the existing canonical row."""

import sqlalchemy as sa

from airweave.domains.entities.canonical.wispr_facts_v1 import meeting_started_at_v1
from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade():
    """Bounded versioned backfill preserves all originals, revisions and creation dates."""
    op.add_column("entity", sa.Column("meeting_started_at", sa.DateTime(timezone=True)))
    op.create_index(
        "idx_entity_wispr_meetings",
        "entity",
        ["sync_id", "meeting_started_at", "id"],
        postgresql_where=sa.text(
            "record_revision > 0 AND entity_definition_short_name = 'meeting' "
            "AND deleted_at IS NULL AND meeting_started_at IS NOT NULL"
        ),
    )
    connection = op.get_bind()
    after = None
    while True:
        rows = (
            connection.execute(
                sa.text("""
            SELECT e.id,e.native_id,e.source_payload
            FROM entity e JOIN source_connection s
              ON s.sync_id=e.sync_id AND s.organization_id=e.organization_id
            WHERE s.short_name='wispr' AND e.entity_definition_short_name='meeting'
              AND e.record_revision>0
              AND (CAST(:after AS uuid) IS NULL OR e.id>CAST(:after AS uuid))
            ORDER BY e.id LIMIT 500
        """),
                {"after": after},
            )
            .mappings()
            .all()
        )
        if not rows:
            break
        for row in rows:
            connection.execute(
                sa.text("UPDATE entity SET meeting_started_at=:start WHERE id=:id").bindparams(
                    sa.bindparam("start", type_=sa.DateTime(timezone=True))
                ),
                {
                    "id": row["id"],
                    "start": meeting_started_at_v1(row["source_payload"] or {}, row["native_id"]),
                },
            )
        after = rows[-1]["id"]


def downgrade():
    """Remove only the rebuildable meeting query projection."""
    op.drop_index("idx_entity_wispr_meetings", "entity")
    op.drop_column("entity", "meeting_started_at")
