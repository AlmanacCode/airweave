"""Managed access requires the selected account and any explicit user binding."""

from unittest.mock import AsyncMock

import pytest

from airweave.domains.auth_provider.exceptions import AuthProviderConfigError
from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source,toolkit",
    [
        ("gmail", "gmail"),
        ("google_calendar", "googlecalendar"),
        ("google_drive", "googledrive"),
        ("slack", "slack"),
        ("linear", "linear"),
        ("github", "github"),
        ("notion", "notion"),
        ("wispr", "wispr_flow_mcp"),
    ],
)
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "wrong-account"),
        ("id", None),
        ("user_id", "wrong-user"),
        ("user_id", None),
    ],
)
async def test_managed_binding_rejects_wrong_or_missing_identity(
    monkeypatch,
    source,
    toolkit,
    field,
    value,
):
    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "secret"},
        config={"account_id": "selected", "user_id": "owner"},
    )
    account = {
        "id": "selected",
        "user_id": "owner",
        "toolkit": {"slug": toolkit},
        "status": "ACTIVE",
    }
    if value is None:
        account.pop(field)
    else:
        account[field] = value
    monkeypatch.setattr(provider, "_get_with_auth", AsyncMock(return_value=account))
    with pytest.raises(AuthProviderConfigError, match="identity" if field == "id" else "user"):
        await provider.get_auth_result(source, [])
    assert provider._last_credential_blob is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source,user_id",
    [
        ("notion", None),
        ("notion", "owner"),
        ("wispr", "owner"),
    ],
)
async def test_managed_binding_accepts_exact_identity_without_requiring_optional_user(
    monkeypatch,
    source,
    user_id,
):
    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "secret"},
        config={"account_id": "selected", "user_id": user_id},
    )
    account = {
        "id": "selected",
        "user_id": "owner",
        "toolkit": {"slug": "wispr_flow_mcp" if source == "wispr" else source},
        "status": "ACTIVE",
    }
    monkeypatch.setattr(provider, "_get_with_auth", AsyncMock(return_value=account))
    result = await provider.get_auth_result(source, [])
    assert result.credentials is None
    assert result.managed_auth.connected_account_id == "selected"


@pytest.mark.asyncio
async def test_wispr_still_requires_configured_user(monkeypatch):
    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "secret"},
        config={"account_id": "selected"},
    )
    monkeypatch.setattr(
        provider,
        "_get_with_auth",
        AsyncMock(
            return_value={
                "id": "selected",
                "user_id": "owner",
                "toolkit": {"slug": "wispr_flow_mcp"},
                "status": "ACTIVE",
            }
        ),
    )
    with pytest.raises(AuthProviderConfigError, match="explicit Composio user"):
        await provider.get_auth_result("wispr", [])
