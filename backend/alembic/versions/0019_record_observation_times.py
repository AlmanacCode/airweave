"""Exact journal-backed observation projections; unknown history stays unknown."""

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from pydantic import AwareDatetime, BaseModel, ValidationError

from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


class SnapshotTimes(BaseModel):
    """Versioned historical evidence, independent of the evolving read DTO."""

    id: UUID
    sync_id: UUID
    revision: int
    observed_at: AwareDatetime
    first_stored_at: AwareDatetime | None = None


def exact_snapshot(candidates, record_id, sync_id, revision_number):
    """Only one well-formed matching historical revision is authoritative."""
    if len(candidates) != 1:
        return None
    try:
        snapshot = SnapshotTimes.model_validate(candidates[0])
    except ValidationError:
        return None
    if (snapshot.id, snapshot.sync_id, snapshot.revision) != (
        record_id,
        sync_id,
        revision_number,
    ):
        return None
    return snapshot


def upgrade():
    """Project only unique exact revision evidence without rewriting the journal."""
    for name in ("first_observed_at", "revision_observed_at", "first_stored_at"):
        op.add_column("entity", sa.Column(name, sa.DateTime(timezone=True), nullable=True))
    connection = op.get_bind()
    after = None
    while True:
        rows = (
            connection.execute(
                sa.text("""
                SELECT id, organization_id, sync_id, record_revision
                FROM entity WHERE record_revision > 0
                  AND (CAST(:after AS uuid) IS NULL OR id > CAST(:after AS uuid))
                ORDER BY id LIMIT 500
            """),
                {"after": after},
            )
            .mappings()
            .all()
        )
        if not rows:
            break
        evidence = (
            connection.execute(
                sa.text("""
                SELECT c.entity_record_id, c.record_revision, c.snapshot
                FROM entity_change c JOIN entity e ON e.id = c.entity_record_id
                  AND e.organization_id = c.organization_id AND e.sync_id = c.sync_id
                WHERE e.id = ANY(:ids)
                  AND c.record_revision IN (1, e.record_revision)
            """).bindparams(sa.bindparam("ids", type_=sa.ARRAY(sa.Uuid()))),
                {"ids": [row["id"] for row in rows]},
            )
            .mappings()
            .all()
        )
        by_revision = {}
        for item in evidence:
            by_revision.setdefault((item["entity_record_id"], item["record_revision"]), []).append(
                item["snapshot"]
            )
        for row in rows:
            values: dict[str, datetime | UUID | None] = {
                "id": row["id"],
                "first": None,
                "current": None,
                "stored": None,
            }
            for rev in {1, row["record_revision"]}:
                candidates = by_revision.get((row["id"], rev), [])
                snapshot = exact_snapshot(candidates, row["id"], row["sync_id"], rev)
                if snapshot is None:
                    continue
                if rev == 1:
                    values["first"] = snapshot.observed_at
                    # Old journal.created_at came from a Python clock. It is not
                    # evidence of DB transaction time and cannot be substituted.
                    values["stored"] = snapshot.first_stored_at
                if rev == row["record_revision"]:
                    values["current"] = snapshot.observed_at
            connection.execute(
                sa.text("""
                UPDATE entity SET first_observed_at=:first,
                    revision_observed_at=:current, first_stored_at=:stored WHERE id=:id
            """).bindparams(
                    *[
                        sa.bindparam(name, type_=sa.DateTime(timezone=True))
                        for name in ("first", "current", "stored")
                    ]
                ),
                values,
            )
        after = rows[-1]["id"]


def downgrade():
    """Remove query projections only; immutable journal evidence is retained."""
    for name in ("first_stored_at", "revision_observed_at", "first_observed_at"):
        op.drop_column("entity", name)
