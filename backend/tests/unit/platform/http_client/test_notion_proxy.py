"""Notion uses the existing account-bound proxy without exposing provider tokens."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from airweave.domains.auth_provider.exceptions import AuthProviderConfigError
from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider
from airweave.platform.http_client.composio_transport import ComposioProxyError, ComposioTransport


@pytest.mark.asyncio
async def test_notion_binding_preserves_version_body_and_native_error(monkeypatch):
    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "project-secret"},
        config={"account_id": "ca_notion", "auth_config_id": "ac_notion"},
    )
    fetch = AsyncMock(
        return_value={
            "id": "ca_notion",
            "toolkit": {"slug": "notion"},
            "status": "ACTIVE",
            "auth_config": {"id": "ac_notion"},
        }
    )
    monkeypatch.setattr(provider, "_get_with_auth", fetch)
    result = await provider.get_auth_result("notion", ["access_token"])
    assert result.credentials is None
    auth = result.managed_auth
    assert auth.allowed_hosts == frozenset({"api.notion.com"})
    assert fetch.call_args.args[1].endswith("/connected_accounts/ca_notion")
    body = {"filter": {"value": "page", "property": "object"}, "start_cursor": "opaque"}
    error = {
        "object": "error",
        "status": 400,
        "code": "validation_error",
        "message": "Invalid cursor",
    }

    async def proxy(request):
        payload = json.loads(request.content)
        assert payload["connected_account_id"] == "ca_notion"
        assert payload["endpoint"] == "https://api.notion.com/v1/search"
        assert payload["method"] == "POST"
        assert payload["body"] == body
        headers = {item["name"]: item["value"] for item in payload["parameters"]}
        assert headers["notion-version"] == "2026-03-11"
        assert headers["content-type"] == "application/json"
        assert not {"authorization", "cookie", "x-api-key"}.intersection(headers)
        assert "provider-secret" not in request.content.decode()
        assert request.headers["x-api-key"] == "project-secret"
        return httpx.Response(200, json={"status": 400, "data": error})

    async with httpx.AsyncClient(transport=httpx.MockTransport(proxy)) as upstream:
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                api_key=auth.api_key.get_secret_value(),
                connected_account_id=auth.connected_account_id,
                allowed_hosts=auth.allowed_hosts,
            )
        ) as client:
            response = await client.post(
                "https://api.notion.com/v1/search",
                json=body,
                headers={
                    "Notion-Version": "2026-03-11",
                    "Authorization": "Bearer provider-secret",
                    "Cookie": "provider-secret",
                    "x-api-key": "provider-secret",
                },
            )
            assert response.status_code == 400
            assert response.json() == error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "https://prod-files-secure.s3.us-west-2.amazonaws.com/file",
        "https://api.notion.com.evil.example/v1/search",
        "https://www.notion.so/page",
        "http://api.notion.com/v1/search",
        "https://api.notion.com:444/v1/search",
        "https://user@api.notion.com/v1/search",
    ],
)
async def test_notion_rejects_unapproved_destinations_before_proxy(url):
    proxy = AsyncMock()
    async with httpx.AsyncClient(
        transport=ComposioTransport(
            client=proxy,
            api_key="secret",
            connected_account_id="ca_notion",
            allowed_hosts={"api.notion.com"},
        )
    ) as client:
        with pytest.raises(ComposioProxyError):
            await client.get(url)
    proxy.stream.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "account,toolkit,config_id,reason",
    [
        (None, "notion", "ac_notion", "explicit Composio account"),
        ("ca_notion", "github", "ac_notion", "toolkit"),
        ("ca_notion", "notion", "other", "auth config"),
    ],
)
async def test_notion_rejects_missing_account_or_mismatched_binding(
    monkeypatch,
    account,
    toolkit,
    config_id,
    reason,
):
    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "secret"},
        config={"account_id": account, "auth_config_id": "ac_notion"},
    )
    fetch = AsyncMock(
        return_value={
            "id": account,
            "toolkit": {"slug": toolkit},
            "status": "ACTIVE",
            "auth_config": {"id": config_id},
        }
    )
    monkeypatch.setattr(provider, "_get_with_auth", fetch)
    with pytest.raises(AuthProviderConfigError, match=reason):
        await provider.get_auth_result("notion", ["access_token"])
    if account is None:
        fetch.assert_not_called()
