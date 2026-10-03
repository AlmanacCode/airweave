"""WhatsApp search views; native originals remain in canonical source records."""

from datetime import datetime

from airweave.platform.entities._airweave_field import AirweaveField
from airweave.platform.entities._base import BaseEntity, FileEntity


class WhatsAppMessageEntity(BaseEntity):
    """One authored body or received quoted snapshot, with distinct provenance."""

    projection_key: str = AirweaveField(
        ..., description="Chat/message/part identity", is_entity_id=True
    )
    title: str = AirweaveField(..., description="Message part label", is_name=True)
    text: str = AirweaveField(..., description="Exact source-language text", embeddable=True)
    message_id: str = AirweaveField(..., description="Native message ID")
    owner_message_id: str = AirweaveField(..., description="Message whose original owns this part")
    chat_id: str = AirweaveField(..., description="Exact native conversation ID")
    source_path: str = AirweaveField(..., description="Part path in retained native JSON")
    sender_id: str | None = AirweaveField(None, description="Exact native sender ID, when supplied")
    is_sender: bool | None = AirweaveField(
        None, description="Provider-attested account owner sender"
    )
    sender_display_name: str | None = AirweaveField(
        None, description="Provider profile display name"
    )
    sender_contact_name: str | None = AirweaveField(None, description="Owner's address-book label")
    sender_public_identifier: str | None = AirweaveField(
        None, description="Optional public phone ID"
    )
    sent_at: datetime | None = AirweaveField(
        None, description="Native time, not capture time", is_created_at=True
    )


class WhatsAppAttachmentEntity(FileEntity):
    """Verified retained original passed to the shared media/document preparation."""

    attachment_key: str = AirweaveField(
        ..., description="Chat/message/attachment identity", is_entity_id=True
    )
    filename: str = AirweaveField(..., description="Native filename or attachment ID", is_name=True)
    attachment_id: str = AirweaveField(..., description="Exact provider attachment ID")
    message_id: str = AirweaveField(..., description="Owning native message ID")
    chat_id: str = AirweaveField(..., description="Exact native chat ID")
    source_path: str = AirweaveField(..., description="Attachment path in retained JSON")
    attachment_type: str = AirweaveField(..., description="Provider media classification")
    voice_note: bool | None = AirweaveField(None, description="Provider voice-note classification")
    sticker: bool | None = AirweaveField(None, description="Provider sticker classification")
