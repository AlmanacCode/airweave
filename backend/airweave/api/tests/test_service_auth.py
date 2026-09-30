"""Service-key mode never falls back to mock users or stale credential caches."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from airweave.api.context_resolver import AuthResult, ContextResolver
from airweave.core.config import AuthMode, settings
from airweave.core.config.settings import Settings
from airweave.core.exceptions import NotFoundException, PermissionException
from airweave.core.shared_models import AuthMethod


def resolver():
    return ContextResolver(
        cache=MagicMock(),
        rate_limiter=MagicMock(),
        user_repo=MagicMock(),
        api_key_repo=SimpleNamespace(get_by_key=AsyncMock()),
        org_repo=MagicMock(),
    )


def test_auth_mode_defaults_and_legacy_migration():
    assert Settings.model_fields["AUTH_MODE"].default == AuthMode.API_KEY
    assert Settings.migrate_auth_mode({"AUTH_ENABLED": True})["AUTH_MODE"] == "auth0"
    with pytest.raises(ValueError, match="conflicts"):
        Settings.migrate_auth_mode({"AUTH_ENABLED": False, "AUTH_MODE": "api_key"})


def test_local_mode_cannot_run_in_production():
    configuration = settings.model_dump()
    configuration.update(AUTH_MODE="local", AUTH_ENABLED=None, ENVIRONMENT="prd")
    with pytest.raises(ValueError, match="only allowed"):
        Settings(**configuration)


@pytest.mark.asyncio
async def test_api_key_mode_rejects_missing_key_and_user_auth(monkeypatch):
    monkeypatch.setattr(settings, "AUTH_MODE", AuthMode.API_KEY)
    service = resolver()
    with pytest.raises(HTTPException) as error:
        await service._authenticate(
            MagicMock(), SimpleNamespace(email="fake@example.com"), None, MagicMock()
        )
    assert error.value.status_code == 401
    with pytest.raises(HTTPException) as error:
        await service.authenticate_user_only(MagicMock(), SimpleNamespace(email="fake@example.com"))
    assert error.value.status_code == 401
    service._users.get_by_email.assert_not_called()


@pytest.mark.asyncio
async def test_key_validated_each_request_and_real_identity_preserved():
    service = resolver()
    org, key = uuid4(), uuid4()
    service._api_keys.get_by_key.return_value = SimpleNamespace(
        organization_id=org, id=key, created_by_email="operator@example.com"
    )
    request = MagicMock()
    request.url.path = "/search"
    first = await service._authenticate_api_key(MagicMock(), "opaque-key", request)
    assert first.api_key_org_id == str(org)
    assert first.metadata["api_key_id"] == str(key)
    service._api_keys.get_by_key.side_effect = NotFoundException("revoked")
    with pytest.raises(HTTPException) as error:
        await service._authenticate_api_key(MagicMock(), "opaque-key", request)
    assert error.value.status_code == 403
    assert service._api_keys.get_by_key.await_count == 2
    service._cache.get_api_key_org_id.assert_not_called()


@pytest.mark.asyncio
async def test_expired_key_maps_to_auth_error():
    service = resolver()
    service._api_keys.get_by_key.side_effect = PermissionException("API key has expired")
    with pytest.raises(HTTPException) as error:
        await service._authenticate_api_key(MagicMock(), "opaque-key", MagicMock())
    assert error.value.status_code == 403


@pytest.mark.asyncio
async def test_key_cannot_override_organization():
    service = resolver()
    auth = AuthResult(method=AuthMethod.API_KEY, api_key_org_id=str(uuid4()))
    with pytest.raises(HTTPException) as error:
        await service._validate_organization_access(MagicMock(), str(uuid4()), auth, "opaque-key")
    assert error.value.status_code == 403
    service._api_keys.get_by_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_key_mode_user_dependencies_never_supply_demo_identity(monkeypatch):
    from airweave.api import auth, deps

    monkeypatch.setattr(settings, "AUTH_MODE", AuthMode.API_KEY)
    assert await auth.get_user_from_token("anything") is None
    assert await auth.auth0.get_user() is None
    assert await deps.get_user_from_token("Bearer anything", MagicMock()) is None


@pytest.mark.asyncio
async def test_service_bootstrap_refuses_demo_superuser(monkeypatch):
    from airweave.db import bootstrap

    monkeypatch.setattr(settings, "AUTH_MODE", AuthMode.API_KEY)
    session_factory = MagicMock()
    monkeypatch.setattr(bootstrap, "AsyncSessionLocal", session_factory)
    with pytest.raises(ValueError, match="requires AUTH_MODE=local"):
        await bootstrap.bootstrap(local_superuser=True)
    session_factory.assert_not_called()


def test_api_key_configuration_does_not_require_auth0():
    configuration = settings.model_dump()
    configuration.update(AUTH_MODE="api_key", AUTH_ENABLED=None)
    for field in configuration:
        if field.startswith("AUTH0_"):
            configuration[field] = None
    assert Settings(**configuration).AUTH_MODE == AuthMode.API_KEY
    configuration["AUTH_MODE"] = "auth0"
    with pytest.raises(ValueError, match="must be set when AUTH_MODE=auth0"):
        Settings(**configuration)


@pytest.mark.parametrize("value", [True, "true", "yes", "on", "1"])
def test_legacy_truthy_auth_never_becomes_local(value):
    assert Settings.migrate_auth_mode({"AUTH_ENABLED": value})["AUTH_MODE"] == AuthMode.AUTH0


@pytest.mark.parametrize("value", ["typo", "", "sometimes"])
def test_invalid_legacy_auth_rejected(value):
    with pytest.raises(ValueError):
        Settings.migrate_auth_mode({"AUTH_ENABLED": value})


def test_service_settings_require_no_demo_credentials():
    configuration = settings.model_dump()
    configuration.update(
        AUTH_MODE="api_key",
        AUTH_ENABLED=None,
        FIRST_SUPERUSER=None,
        FIRST_SUPERUSER_PASSWORD=None,
    )
    result = Settings(**configuration)
    assert result.FIRST_SUPERUSER is None
    assert result.FIRST_SUPERUSER_PASSWORD is None


@pytest.mark.asyncio
async def test_startup_missing_embedding_metadata_never_seeds():
    from airweave.domains.embedders import config

    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    db = MagicMock(execute=AsyncMock(return_value=result), commit=AsyncMock())
    with pytest.raises(config.EmbeddingConfigError, match="bootstrap"):
        await config.validate_embedding_config(db)
    db.add.assert_not_called()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_bootstrap_initializes_embedding_metadata():
    from airweave.domains.embedders import config

    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    db = MagicMock(execute=AsyncMock(return_value=result), commit=AsyncMock())
    await config.initialize_embedding_config(db)
    row = db.add.call_args.args[0]
    assert row.dense_embedder == config.DENSE_EMBEDDER
    assert row.embedding_dimensions == config.EMBEDDING_DIMENSIONS
    db.commit.assert_awaited_once()
