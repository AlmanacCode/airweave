"""Revision-bound Gmail inventory and independent prepared body query facts."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from airweave.domains.entities.canonical.mail_facts_v1 import gmail_metadata_v1
from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade():
    """Versioned decoder backfills bounded pages; originals and journal stay unchanged."""
    op.add_column("entity", sa.Column("gmail_metadata", postgresql.JSONB(none_as_null=True)))
    op.add_column("entity", sa.Column("gmail_metadata_revision", sa.BigInteger()))
    op.add_column(
        "sync", sa.Column("mail_text_sequence", sa.BigInteger(), nullable=False, server_default="0")
    )
    op.add_column("projection_generation", sa.Column("mail_body_text", sa.Text()))
    op.add_column("projection_generation", sa.Column("mail_body_status", sa.String(16)))
    op.alter_column("projection_generation", "documents", nullable=True)
    op.create_check_constraint(
        "ck_projection_mail_body",
        "projection_generation",
        "(mail_body_text IS NULL AND mail_body_status IS NULL) OR "
        "(mail_body_text IS NOT NULL AND mail_body_status IS NOT NULL "
        "AND mail_body_status IN ('complete', 'partial'))",
    )
    op.create_index(
        "idx_entity_gmail_metadata",
        "entity",
        ["gmail_metadata"],
        postgresql_using="gin",
        postgresql_ops={"gmail_metadata": "jsonb_path_ops"},
    )
    op.create_index(
        "idx_entity_gmail_inventory",
        "entity",
        ["sync_id", "source_created_at", "id"],
        postgresql_where=sa.text(
            "record_revision > 0 AND entity_definition_short_name = 'message' "
            "AND deleted_at IS NULL"
        ),
    )
    op.create_index(
        "idx_generation_mail_body",
        "projection_generation",
        ["record_id", "revision", "pipeline_version", "created_at", "id"],
        postgresql_where=sa.text("mail_body_text IS NOT NULL AND retired_at IS NULL"),
    )
    connection = op.get_bind()
    after = None
    update = sa.text(
        "UPDATE entity SET gmail_metadata=:facts, gmail_metadata_revision=:revision WHERE id=:id"
    ).bindparams(sa.bindparam("facts", type_=postgresql.JSONB(none_as_null=True)))
    while True:
        rows = (
            connection.execute(
                sa.text("""
            SELECT e.id,e.native_id,e.source_payload,e.source_created_at,e.record_revision
            FROM entity e JOIN source_connection s
              ON s.sync_id=e.sync_id AND s.organization_id=e.organization_id
            WHERE s.short_name='gmail' AND e.entity_definition_short_name='message'
              AND e.record_revision>0
              AND (CAST(:after AS uuid) IS NULL OR e.id>CAST(:after AS uuid))
            ORDER BY e.id LIMIT 500"""),
                {"after": after},
            )
            .mappings()
            .all()
        )
        if not rows:
            break
        for row in rows:
            facts = gmail_metadata_v1(
                row["source_payload"] or {}, row["native_id"], row["source_created_at"]
            )
            connection.execute(
                update,
                {
                    "id": row["id"],
                    "revision": row["record_revision"],
                    "facts": facts.model_dump(mode="json") if facts is not None else None,
                },
            )
        after = rows[-1]["id"]


def downgrade():
    """Prepared-only rows must be retired before rolling back the stage contract."""
    op.alter_column("projection_generation", "documents", nullable=False)
    op.drop_index("idx_generation_mail_body", "projection_generation")
    op.drop_index("idx_entity_gmail_inventory", "entity")
    op.drop_index("idx_entity_gmail_metadata", "entity")
    op.drop_constraint("ck_projection_mail_body", "projection_generation", type_="check")
    op.drop_column("projection_generation", "mail_body_status")
    op.drop_column("projection_generation", "mail_body_text")
    op.drop_column("sync", "mail_text_sequence")
    op.drop_column("entity", "gmail_metadata_revision")
    op.drop_column("entity", "gmail_metadata")
