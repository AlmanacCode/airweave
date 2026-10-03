"""Opt-in registry and direct credential wiring; no account activation."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from airweave.domains.sources.exceptions import SourceError
from airweave.domains.sources.token_providers.credential import DirectCredentialProvider
from airweave.platform.configs.auth import WhatsAppAuthConfig
from airweave.platform.configs.config import WhatsAppCaptureConfig
from airweave.platform.sources.whatsapp import WhatsAppSource
from airweave.schemas.source_connection import AuthenticationMethod

CONFIG = {
    "account_id": "acc_bound",
    "account_user_id": "self@lid",
    "native_user_id": "self@lid",
    "pagination": "cursor",
    "page_size": 20,
    "max_pages_per_scope": 100,
    "maximum_attachment_bytes": 1000,
    "participant_pagination": "offset",
    "maximum_participant_pages": 10,
    "maximum_participant_items": 100,
    "maximum_participant_bytes": 100000,
    "reaction_pagination": "offset",
    "maximum_reaction_pages": 10,
    "maximum_reaction_items": 100,
    "maximum_reaction_bytes": 100000,
}


@pytest.mark.asyncio
async def test_factory_contract_reuses_direct_credentials_and_canonical_pages():
    auth = DirectCredentialProvider(WhatsAppAuthConfig(api_key="private-test-key"))
    http = Mock()
    source = await WhatsAppSource.create(
        auth=auth,
        logger=Mock(),
        http_client=http,
        config=WhatsAppCaptureConfig(**CONFIG),
    )
    assert source.auth is auth and source.http_client is http
    assert source.capture_page_source.client.account_id == "acc_bound"
    assert source.capture_page_source.config.native_user_id == "self@lid"
    assert source.capture_page_source.capture_cycle_configuration.completion_policies == {
        "whatsapp_chat": "discovery_only",
        "whatsapp_message": "discovery_only",
        "whatsapp_chat_participants": "discovery_only",
        "whatsapp_message_reactions": "discovery_only",
    }
    source.capture_page_source.validate = AsyncMock()
    await source.validate()
    source.capture_page_source.validate.assert_awaited_once()
    assert "private-test-key" not in repr(auth.credentials)
    # Existing encrypted credential persistence serializes the real value.
    assert auth.credentials.model_dump()["api_key"] == "private-test-key"


@pytest.mark.asyncio
async def test_managed_or_static_auth_rejected_before_io():
    http = Mock()
    with pytest.raises(SourceError, match="direct Unipile"):
        await WhatsAppSource.create(
            auth=Mock(),
            logger=Mock(),
            http_client=http,
            config=WhatsAppCaptureConfig(**CONFIG),
        )
    http.stream.assert_not_called()


def test_experimental_metadata_and_required_budgets():
    assert WhatsAppSource.internal
    assert WhatsAppSource.auth_methods == [AuthenticationMethod.DIRECT]
    assert not WhatsAppSource.supports_continuous
    assert "experimental" in WhatsAppSource.source_name
    for field in (
        "account_user_id",
        "native_user_id",
        "pagination",
        "max_pages_per_scope",
        "maximum_attachment_bytes",
        "participant_pagination",
        "maximum_participant_pages",
        "maximum_participant_items",
        "maximum_participant_bytes",
        "reaction_pagination",
        "maximum_reaction_pages",
        "maximum_reaction_items",
        "maximum_reaction_bytes",
    ):
        with pytest.raises(ValidationError):
            WhatsAppCaptureConfig(**{key: value for key, value in CONFIG.items() if key != field})
    with pytest.raises(ValidationError):
        WhatsAppCaptureConfig(**(CONFIG | {"api_key": "must-not-go-in-config"}))


def test_actual_registry_hides_internal_source_until_enabled(monkeypatch):
    import airweave.domains.sources.registry as registry_module

    auth_registry = Mock()
    auth_registry.list_all.return_value = []
    entities = Mock()
    entities.list_for_source.return_value = []
    monkeypatch.setattr(registry_module, "ALL_SOURCES", [WhatsAppSource])
    monkeypatch.setattr(registry_module.settings, "ENABLE_INTERNAL_SOURCES", False)
    hidden = registry_module.SourceRegistry(auth_registry, entities)
    hidden.build()
    with pytest.raises(KeyError):
        hidden.get("whatsapp")
    monkeypatch.setattr(registry_module.settings, "ENABLE_INTERNAL_SOURCES", True)
    enabled = registry_module.SourceRegistry(auth_registry, entities)
    enabled.build()
    entry = enabled.get("whatsapp")
    assert entry.source_class_ref is WhatsAppSource
    assert entry.auth_config_ref is WhatsAppAuthConfig
    assert entry.config_ref is WhatsAppCaptureConfig
    assert entry.runtime_auth_all_fields == ["api_key"]


def test_fresh_registered_package_import_orders_avoid_cycle():
    import subprocess
    import sys
    from pathlib import Path

    backend = Path(__file__).resolve().parents[4]
    for module in (
        "airweave.platform.http_client.unipile_transport",
        "airweave.platform.sources.whatsapp_capture",
        "airweave.platform.sources.whatsapp",
    ):
        script = f"""
import ast, importlib, sys, types
from pathlib import Path
sys.path[:] = {sys.path!r}
backend = Path({str(backend)!r})
# Stub unrelated provider implementations; execute the real eager package
# __init__ and preserve the real WhatsApp/transport/capture import boundary.
init = backend / 'airweave/platform/sources/__init__.py'
for node in ast.parse(init.read_text()).body:
    if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module != 'whatsapp':
        name = 'airweave.platform.sources.' + node.module
        stub = types.ModuleType(name)
        for alias in node.names:
            setattr(stub, alias.name, type(alias.name, (), {{}}))
        sys.modules[name] = stub
importlib.import_module({module!r})
from airweave.platform.sources import ALL_SOURCES
from airweave.platform.sources.whatsapp import WhatsAppSource
assert WhatsAppSource in ALL_SOURCES
"""
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr


def test_registered_entities_are_available_to_real_source_registry(monkeypatch):
    import airweave.domains.sources.registry as registry_module
    from airweave.domains.entities.registry import EntityDefinitionRegistry

    entities = EntityDefinitionRegistry()
    entities.build()
    names = {entry.short_name for entry in entities.list_for_source("whatsapp")}
    assert names == {"whats_app_message_entity", "whats_app_attachment_entity"}
    auth_registry = Mock()
    auth_registry.list_all.return_value = [Mock(short_name="broker", blocked_sources=[])]
    monkeypatch.setattr(registry_module, "ALL_SOURCES", [WhatsAppSource])
    monkeypatch.setattr(registry_module.settings, "ENABLE_INTERNAL_SOURCES", True)
    registry = registry_module.SourceRegistry(auth_registry, entities)
    registry.build()
    entry = registry.get("whatsapp")
    assert set(entry.output_entity_definitions) == names
    assert entry.supported_auth_providers == []
