"""Prepared Gmail body facts share projection generation ownership and retirement."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.sync_pipeline.pipeline.text_models import BuiltText
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.sync import Sync


class PreparedMailBody(BaseModel):
    """Complete converter body (possibly partial originals), never generated headers."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    text: str
    status: Literal["complete", "partial"]


def prepared_mail_body(
    built: tuple[BuiltText, ...], generation: UUID, completeness: str
) -> PreparedMailBody | None:
    """Select only Gmail message part zero, after its converter-content boundary."""
    for item in built:
        locator = ProjectionLocator.parse(item.entity_id)
        if locator is None or locator.generation != generation or locator.part_index != 0:
            continue
        if item.content_start is None or item.kind == "generated_text":
            return None
        if item.content_start > len(item.text):
            raise ValueError("Gmail body boundary exceeds converter output")
        return PreparedMailBody(
            text=item.text[item.content_start :].casefold(),
            status="complete" if completeness == "complete" else "partial",
        )
    return None


def current_mail_body():
    """Exactly one validated prepared attempt for this canonical revision and pipeline.

    An attempt failing before body preparation cannot hide an earlier valid body.
    The newest validated fact is designated, independent of index publication.
    """
    return (
        select(
            ProjectionGeneration.id,
            ProjectionGeneration.mail_body_text,
            ProjectionGeneration.mail_body_status,
        )
        .where(
            ProjectionGeneration.organization_id == Entity.organization_id,
            ProjectionGeneration.sync_id == Entity.sync_id,
            ProjectionGeneration.record_id == Entity.id,
            ProjectionGeneration.revision == Entity.record_revision,
            ProjectionGeneration.pipeline_version == Sync.index_pipeline_version,
            ProjectionGeneration.mail_body_text.is_not(None),
            ProjectionGeneration.retired_at.is_(None),
        )
        .order_by(ProjectionGeneration.created_at.desc(), ProjectionGeneration.id.desc())
        .limit(1)
        .correlate(Entity, Sync)
    )
