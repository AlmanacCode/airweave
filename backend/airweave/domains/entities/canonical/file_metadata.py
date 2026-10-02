"""Searchable file descriptors, independent of successful content extraction."""

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.sync_pipeline.processors.entity_fields import populate_base_fields
from airweave.platform.entities._airweave_field import AirweaveField
from airweave.platform.entities._base import BaseEntity


class FileFacts(BaseModel):
    """Only descriptive fields enter search; private URLs and tokens never do."""

    model_config = ConfigDict(extra="ignore", strict=True)
    name: str | None = None
    title: str | None = None
    description: str | None = None
    media_type: str | None = Field(
        default=None, validation_alias=AliasChoices("mimeType", "mimetype")
    )


class FileMetadataEntity(BaseEntity):
    """A metadata match says nothing about extracted file contents."""

    file_id: str = AirweaveField(..., is_entity_id=True, embeddable=False)
    title: str = AirweaveField(..., is_name=True, embeddable=True)
    filename: str = AirweaveField(..., embeddable=True)
    description: str | None = AirweaveField(None, embeddable=True)
    media_type: str | None = AirweaveField(None, embeddable=True)


def with_file_metadata(record: SourceRecord, mapped: ProjectionInputs) -> ProjectionInputs:
    """Append without renumbering original content parts or altering retained bytes."""
    facts = FileFacts.model_validate(record.payload)
    if facts.media_type == "application/vnd.google-apps.folder":
        return mapped
    name = facts.name or facts.title or record.identity.native_id
    entity = FileMetadataEntity(
        file_id=record.identity.native_id,
        breadcrumbs=[],
        title=f"File metadata: {name}",
        filename=name,
        description=facts.description,
        media_type=facts.media_type,
    )
    populate_base_fields(entity)
    return ProjectionInputs(
        parts=(
            *mapped.parts,
            ProjectionInput(
                part=ExtractionPart(
                    part_index=len(mapped.parts), key="file_metadata", kind="metadata"
                ),
                entity=entity,
            ),
        )
    )
