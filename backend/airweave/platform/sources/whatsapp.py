"""Opt-in internal WhatsApp v2 capture; live provider qualification is pending."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.credential import DirectCredentialProvider
from airweave.domains.sources.token_providers.protocol import SourceAuthProvider
from airweave.platform.configs.auth import WhatsAppAuthConfig
from airweave.platform.configs.config import WhatsAppCaptureConfig
from airweave.platform.decorators import source
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.sources._base import BaseSource
from airweave.schemas.source_connection import AuthenticationMethod

if TYPE_CHECKING:
    from airweave.platform.sources.whatsapp_capture import WhatsAppCapture


@source(
    name="WhatsApp (Unipile v2 experimental)",
    short_name="whatsapp",
    auth_methods=[AuthenticationMethod.DIRECT],
    auth_config_class=WhatsAppAuthConfig,
    config_class=WhatsAppCaptureConfig,
    labels=["Messaging", "Experimental"],
    supports_continuous=False,
    rate_limit_level=RateLimitLevel.CONNECTION,
    internal=True,
)
class WhatsAppSource(BaseSource):
    """Canonical acquisition only, discoverable with ENABLE_INTERNAL_SOURCES."""

    canonical_record_types = (
        "whatsapp_chat",
        "whatsapp_message",
        "whatsapp_chat_participants",
        "whatsapp_message_reactions",
    )
    canonical_container_parents = {
        "whatsapp_message": "whatsapp_chat",
        "whatsapp_chat_participants": "whatsapp_chat",
        "whatsapp_message_reactions": "whatsapp_message",
    }
    _capture: WhatsAppCapture

    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: WhatsAppCaptureConfig,
    ) -> WhatsAppSource:
        """Use the existing decrypted credential provider, never broker auth or enrollment."""
        from airweave.platform.http_client.unipile_transport import UnipileWhatsAppClient
        from airweave.platform.sources.whatsapp_capture import WhatsAppCapture

        if not isinstance(auth, DirectCredentialProvider):
            raise SourceError(
                "Experimental WhatsApp requires direct Unipile v2 credentials",
                source_short_name="whatsapp",
            )
        credentials = WhatsAppAuthConfig.model_validate(auth.credentials.model_dump())
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance._capture = WhatsAppCapture(
            UnipileWhatsAppClient(
                http_client,
                account_id=config.account_id,
                api_key=credentials.api_key,
            ),
            config,
        )
        instance._capture_page_source = instance._capture
        return instance

    async def validate(self) -> None:
        """Validate exact bound principal; this does not qualify full-history sync."""
        await self._capture.validate()
