"""Local OCR is opt-in and precedes metered fallback without duplicate processing."""

from unittest.mock import patch

import pytest

from airweave.adapters.circuit_breaker.fake import FakeCircuitBreaker
from airweave.core.config.settings import Settings
from airweave.core.container.factory import _create_ocr_provider
from airweave.domains.ocr.fakes.provider import FakeOcrProvider


def test_no_configured_ocr_remains_disabled():
    settings = Settings.model_construct(
        LOCAL_OCR_TESSDATA_PATH=None, MISTRAL_API_KEY=None, DOCLING_BASE_URL=None
    )
    assert _create_ocr_provider(FakeCircuitBreaker(), settings) is None


@pytest.mark.asyncio
async def test_local_ocr_precedes_cloud_and_receives_configured_languages():
    settings = Settings.model_construct(
        LOCAL_OCR_TESSDATA_PATH="/models",
        LOCAL_OCR_LANGUAGES=("eng", "hin"),
        MISTRAL_API_KEY="test-only",
        DOCLING_BASE_URL=None,
    )
    local = FakeOcrProvider(default_markdown="local result")
    cloud = FakeOcrProvider(default_markdown="cloud result")
    with (
        patch(
            "airweave.core.container.factory.LocalOcrProvider", return_value=local
        ) as constructor,
        patch("airweave.core.container.factory.MistralOCR", return_value=cloud),
    ):
        provider = _create_ocr_provider(FakeCircuitBreaker(), settings)
    constructor.assert_called_once_with(tessdata_path="/models", languages=("eng", "hin"))
    assert await provider.convert_batch(["scan.pdf"]) == {"scan.pdf": "local result"}
    assert cloud.calls == []
