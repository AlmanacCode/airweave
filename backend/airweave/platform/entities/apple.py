"""Search views of retained Apple records; original native rows remain canonical."""

from datetime import datetime

from pydantic import computed_field

from airweave.platform.entities._airweave_field import AirweaveField
from airweave.platform.entities._base import BaseEntity, FileEntity


class AppleRecordEntity(BaseEntity):
    """One native record's readable body without invented URLs or account identities."""

    native_id: str = AirweaveField(..., description="Native record identifier", is_entity_id=True)
    title: str = AirweaveField(
        ..., description="Native title or source label", is_name=True, embeddable=True
    )
    content: str = AirweaveField(..., description="Prepared retained text", embeddable=True)
    created_at: datetime | None = AirweaveField(
        None, description="Known source creation time", is_created_at=True
    )
    modified_at: datetime | None = AirweaveField(
        None, description="Known source modification time", is_updated_at=True
    )

    @computed_field(return_type=str)
    def web_url(self) -> str:
        """Native records have no verified browser URL; use the retained record locator."""
        return ""


class AppleAttachmentEntity(FileEntity):
    """Disposable extraction view of one committed native attachment occurrence."""

    attachment_key: str = AirweaveField(
        ..., description="Record-scoped native attachment identity", is_entity_id=True
    )
    filename: str = AirweaveField(
        ..., description="Native filename label or attachment identity", is_name=True
    )
