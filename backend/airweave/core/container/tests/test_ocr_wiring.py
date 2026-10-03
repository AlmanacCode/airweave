"""Local OCR is opt-in and precedes metered fallback without duplicate processing."""

from unittest.mock import patch

import pytest

from airweave.adapters.circuit_breaker.fake import FakeCircuitBreaker
from airweave.core.config.settings import Settings
from airweave.core.container.factory import _create_ocr_provider
from airweave.domains.ocr.fakes.provider import FakeOcrProvider
from airweave.domains.ocr.local import LocalOcrProvider


def test_no_configured_ocr_remains_disabled():
    settings = Settings.model_construct(
        LOCAL_OCR_TESSDATA_PATH=None, MISTRAL_API_KEY=None, DOCLING_BASE_URL=None
    )
    assert _create_ocr_provider(FakeCircuitBreaker(), settings) is None


@pytest.mark.asyncio
async def test_local_ocr_precedes_cloud_and_receives_configured_languages(tmp_path):
    for language in ("eng", "hin"):
        (tmp_path / f"{language}.traineddata").write_bytes(b"synthetic language artifact")
    settings = Settings.model_construct(
        LOCAL_OCR_TESSDATA_PATH=str(tmp_path),
        LOCAL_OCR_LANGUAGES=("eng", "hin"),
        MISTRAL_API_KEY="test-only",
        DOCLING_BASE_URL=None,
    )
    local = LocalOcrProvider(tmp_path, ("eng", "hin"))
    cloud = FakeOcrProvider(default_markdown="cloud result")
    with (
        patch(
            "airweave.core.container.factory.LocalOcrProvider", return_value=local
        ) as constructor,
        patch("airweave.core.container.factory.MistralOCR", return_value=cloud),
        patch.object(local, "convert_batch", return_value={"scan.pdf": "local result"}),
    ):
        provider = _create_ocr_provider(FakeCircuitBreaker(), settings)
        assert await provider.convert_batch(["scan.pdf"]) == {"scan.pdf": "local result"}
    constructor.assert_called_once_with(tessdata_path=str(tmp_path), languages=("eng", "hin"))
    assert cloud.calls == []
    assert tuple(step.provider for step in provider.configured_policy) == (
        "local-tesseract",
        "mistral-ocr",
    )
    assert all(model.sha256 for model in provider.configured_policy[0].models)
    assert provider.configured_policy[1].models[0].resolution == "mutable_alias"
    assert str(tmp_path) not in str(provider.configured_policy)
