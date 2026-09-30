"""OCR capability does not prevent the real worker composition from starting."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import airweave.core.container as container_mod
from airweave.core.config import settings
from airweave.core.container import factory
from airweave.domains.embedders import config as embedding_config
from airweave.domains.temporal import worker


@pytest.mark.asyncio
@pytest.mark.parametrize("health_status", [200, 503])
async def test_worker_composes_without_ocr_and_starts_supported_activities(
    monkeypatch, tmp_path, health_status
):
    """Real DI composition; substitute Temporal lifecycle and inference health HTTP."""
    configured = settings.model_copy(
        update={
            "MISTRAL_API_KEY": "",
            "DOCLING_BASE_URL": "",
            "STORAGE_BACKEND": "filesystem",
            "STORAGE_PATH": str(tmp_path),
            "STRIPE_ENABLED": False,
            "ANALYTICS_ENABLED": False,
        }
    )
    monkeypatch.setattr(worker, "settings", configured)
    monkeypatch.setattr(factory, "DENSE_EMBEDDER", "local_minilm")
    monkeypatch.setattr(factory, "EMBEDDING_DIMENSIONS", 384)
    monkeypatch.setattr(embedding_config, "DENSE_EMBEDDER", "local_minilm")
    monkeypatch.setattr(embedding_config, "EMBEDDING_DIMENSIONS", 384)
    # Stub only the external inference health HTTP call, not embedding validation.
    original_client = httpx.Client

    def health_client(*args, **kwargs):
        return original_client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(health_status)
                if request.url.path == "/.well-known/ready"
                else httpx.Response(503)
            ),
            **kwargs,
        )

    monkeypatch.setattr(
        embedding_config,
        "httpx",
        SimpleNamespace(
            Client=health_client,
            Timeout=httpx.Timeout,
            ConnectError=httpx.ConnectError,
            TimeoutException=httpx.TimeoutException,
            HTTPStatusError=httpx.HTTPStatusError,
        ),
    )
    monkeypatch.setattr(container_mod, "container", None)
    lifecycle = MagicMock(start=AsyncMock(), stop=AsyncMock())
    monkeypatch.setattr(worker, "TemporalWorker", lambda _: lifecycle)
    monkeypatch.setattr(worker.signal, "signal", lambda *_: None)
    if health_status != 200:
        with pytest.raises(embedding_config.EmbeddingConfigError, match="not reachable"):
            await worker.main()
        lifecycle.start.assert_not_awaited()
        return
    await worker.main()
    assert container_mod.container.ocr_provider is None
    assert container_mod.container.converter_registry.for_extension(".txt") is not None
    assert container_mod.container.converter_registry.for_extension(".pdf") is not None
    assert container_mod.container.converter_registry.for_extension(".png") is None
    lifecycle.start.assert_awaited_once()
    lifecycle.stop.assert_awaited_once()
