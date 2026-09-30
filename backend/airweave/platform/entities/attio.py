"""Offline search schema for retained Attio originals."""

from datetime import datetime
from typing import Literal

from pydantic import computed_field

from airweave.platform.entities._airweave_field import AirweaveField
from airweave.platform.entities._base import BaseEntity


class AttioOriginalEntity(BaseEntity):
    """Search text derived from one retained Attio original, without provider reads."""

    native_id: str = AirweaveField(..., description="Native Attio UUID", is_entity_id=True)
    original_kind: Literal["object", "record", "list", "entry", "note"] = AirweaveField(
        ..., description="Native record kind"
    )
    title: str = AirweaveField(..., description="Original label", is_name=True, embeddable=True)
    text: str = AirweaveField(..., description="Retained original content", embeddable=True)
    content_coverage: str = AirweaveField(..., description="Included and omitted content")
    native_url: str | None = AirweaveField(None, description="Verified retained native URL")
    created_at: datetime | None = AirweaveField(
        None, description="Native creation time", is_created_at=True
    )
    updated_at: datetime | None = AirweaveField(
        None, description="Native modification time", is_updated_at=True
    )

    @computed_field(return_type=str)
    def web_url(self) -> str:
        """Native navigation requires a verified URL; do not synthesize one."""
        return self.native_url or ""
