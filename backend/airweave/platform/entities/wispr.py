"""Search projection of Wispr's captured meeting representations."""

from datetime import datetime

from pydantic import computed_field

from airweave.platform.entities._airweave_field import AirweaveField
from airweave.platform.entities._base import BaseEntity


class WisprMeetingEntity(BaseEntity):
    """Selected meeting fields; original response ranges stay in canonical storage."""

    meeting_id: str = AirweaveField(..., description="Native meeting ID", is_entity_id=True)
    title: str = AirweaveField(..., description="Meeting title", is_name=True, embeddable=True)
    notes: str = AirweaveField(..., description="Captured markdown notes", embeddable=True)
    summary: str = AirweaveField(..., description="Provider summary", embeddable=True)
    transcript: str = AirweaveField(..., description="Captured transcript ranges", embeddable=True)
    starts_at: datetime | None = AirweaveField(
        None, description="Meeting start", is_created_at=True
    )
    modified_at: datetime | None = AirweaveField(
        None, description="Provider modification", is_updated_at=True
    )
    share_link: str | None = AirweaveField(
        None, description="Provider notes page", embeddable=False
    )

    @computed_field(return_type=str)
    def web_url(self) -> str:
        """Use only the provider-supplied sharing link."""
        return self.share_link or ""
