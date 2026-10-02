"""Outlook native identity and transport boundaries, without provider qualification."""

from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import SecretStr

from airweave.domains.auth_provider.exceptions import AuthProviderAuthError
from airweave.domains.sources.exceptions import SourceAuthError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import OutlookCalendarConfig, OutlookMailConfig
from airweave.platform.http_client.composio_transport import ComposioTransport
from airweave.platform.sources.outlook_calendar import OutlookCalendarSource
from airweave.platform.sources.outlook_graph import OutlookBoundaryError
from airweave.platform.sources.outlook_mail import OutlookMailSource


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_cls,config_cls",
    [
        (OutlookMailSource, OutlookMailConfig),
        (OutlookCalendarSource, OutlookCalendarConfig),
    ],
)
async def test_managed_attestation_reset_blocks_list_and_attachment_reads(source_cls, config_cls):
    principal = "opaque-principal"
    requests = []

    def graph(request):
        requests.append(request)
        assert "authorization" not in request.headers
        assert "prefer" not in request.headers  # No silent legacy ID migration.
        return httpx.Response(
            200, json={"id": principal} if request.url.path == "/v1.0/me" else {"value": []}
        )

    auth = ManagedAuthProvider(
        api_key=SecretStr("private"),
        connected_account_id="account",
        allowed_hosts=frozenset({"graph.microsoft.com"}),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        with pytest.raises(OutlookBoundaryError, match="trusted expected"):
            await source_cls.create(
                auth=auth, logger=MagicMock(), http_client=client, config=config_cls()
            )
        assert not requests
        source = await source_cls.create(
            auth=auth,
            logger=MagicMock(),
            http_client=client,
            config=config_cls(expected_principal_id=principal),
        )
        await source._get("https://graph.microsoft.com/v1.0/me/messages/id/attachments")
        principal = "wrong-private-principal"
        with pytest.raises(OutlookBoundaryError, match="does not match") as error:
            await source.validate()
        assert principal not in str(error.value)
        count = len(requests)
        with pytest.raises(OutlookBoundaryError, match="attested"):
            await source._get("https://graph.microsoft.com/v1.0/me/events/id/attachments")
        assert len(requests) == count
        with pytest.raises(OutlookBoundaryError, match="does not match"):
            await anext(source.generate_entities())


@pytest.mark.asyncio
async def test_managed_proxy_and_native_401_keep_different_error_authority():
    native_status = 200
    proxy_status = 200

    def proxy(request):
        import json

        payload = json.loads(request.content)
        assert payload["connected_account_id"] == "bound-account"
        assert all(p["name"].lower() != "authorization" for p in payload["parameters"])
        return httpx.Response(
            proxy_status,
            json={
                "status": native_status,
                "data": {"id": "principal", "error": "private-token"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(proxy)) as proxy_client:
        transport = ComposioTransport(
            api_key="private",
            connected_account_id="bound-account",
            allowed_hosts={"graph.microsoft.com"},
            client=proxy_client,
        )
        auth = ManagedAuthProvider(
            api_key=SecretStr("private"),
            connected_account_id="bound-account",
            allowed_hosts=frozenset({"graph.microsoft.com"}),
        )
        async with httpx.AsyncClient(transport=transport) as client:
            source = await OutlookMailSource.create(
                auth=auth,
                logger=MagicMock(),
                http_client=client,
                config=OutlookMailConfig(expected_principal_id="principal"),
            )
            native_status = 401
            with pytest.raises(SourceAuthError) as error:
                await source._get("https://graph.microsoft.com/v1.0/me/messages")
            assert "private-token" not in str(error.value)
            assert source.graph.verified_principal_id is None
            proxy_status = 401
            with pytest.raises(AuthProviderAuthError):
                await source.validate()


@pytest.mark.asyncio
async def test_unsafe_continuations_are_fatal_before_request_even_with_direct_credentials():
    calls = []

    def graph(request):
        calls.append(request)
        return httpx.Response(200, json={"value": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await OutlookMailSource.create(
            auth=StaticTokenProvider("private"),
            logger=MagicMock(),
            http_client=client,
            config=OutlookMailConfig(),
        )
        for url in (
            "https://evil.example/v1.0/me/messages?token=private",
            "https://graph.microsoft.com/v1.0/users/other/messages",
            "https://graph.microsoft.com/beta/me/messages",
            "https://graph.microsoft.com/v1.0/me/%2e%2e/users/other",
            "https://graph.microsoft.com/v1.0/me/%252e%252e/users/other",
            "https://graph.microsoft.com/v1.0/me/messages#private",
        ):
            with pytest.raises(OutlookBoundaryError) as error:
                await source._get(url)
            assert "private" not in str(error.value)
        assert not calls
        await source.validate()  # No /me permission newly required for unbound direct connections.
        assert len(calls) == 1
        assert calls[0].url.path == "/v1.0/me/mailFolders"
        assert calls[0].headers["authorization"] == "Bearer private"


@pytest.mark.asyncio
async def test_direct_refresh_rechecks_principal_before_retrying_attachment():
    class RefreshingAuth(StaticTokenProvider):
        @property
        def supports_refresh(self):
            return True

        async def force_refresh(self):
            self._token = "replacement"
            return self._token

    paths = []

    def graph(request):
        paths.append(request.url.path)
        if request.url.path == "/v1.0/me":
            identity = (
                "original" if request.headers["authorization"] == "Bearer initial" else "other"
            )
            return httpx.Response(200, json={"id": identity})
        return httpx.Response(401, json={"error": {"message": "private-details"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await OutlookMailSource.create(
            auth=RefreshingAuth("initial"),
            logger=MagicMock(),
            http_client=client,
            config=OutlookMailConfig(expected_principal_id="original"),
        )
        with pytest.raises(OutlookBoundaryError, match="does not match"):
            await source._get("https://graph.microsoft.com/v1.0/me/messages/id/attachments")
        assert paths == ["/v1.0/me", "/v1.0/me/messages/id/attachments", "/v1.0/me"]
        assert source.graph.verified_principal_id is None


@pytest.mark.asyncio
async def test_broker_outlook_mapping_reuses_bound_graph_transport(monkeypatch):
    from unittest.mock import AsyncMock

    from airweave.domains.auth_provider.exceptions import AuthProviderConfigError
    from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider

    provider = await ComposioAuthProvider.create(
        credentials={"api_key": "secret"}, config={"account_id": "selected", "user_id": "owner"}
    )
    account = {
        "id": "selected",
        "user_id": "owner",
        "toolkit": {"slug": "outlook"},
        "status": "ACTIVE",
    }
    monkeypatch.setattr(provider, "_get_with_auth", AsyncMock(return_value=account))
    for name in ("outlook_mail", "outlook_calendar"):
        result = await provider.get_auth_result(name, [])
        assert result.credentials is None
        assert result.managed_auth.allowed_hosts == frozenset({"graph.microsoft.com"})
        assert result.managed_auth.connected_account_id == "selected"
    account["toolkit"]["slug"] = "other"
    with pytest.raises(AuthProviderConfigError, match="toolkit"):
        await provider.get_auth_result("outlook_mail", [])


@pytest.mark.asyncio
async def test_malformed_principal_and_error_envelope_do_not_become_empty_success():
    data = {"id": "principal"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=data))
    ) as client:
        source = await OutlookCalendarSource.create(
            auth=StaticTokenProvider("secret"),
            logger=MagicMock(),
            http_client=client,
            config=OutlookCalendarConfig(expected_principal_id="principal"),
        )
        for invalid_body in (None, [], "private-body"):
            data = invalid_body
            with pytest.raises(OutlookBoundaryError, match="invalid JSON"):
                await source._get("https://graph.microsoft.com/v1.0/me/calendars")
        data = {"error": {"message": "private-message"}}
        with pytest.raises(OutlookBoundaryError, match="error envelope") as error:
            await source._get("https://graph.microsoft.com/v1.0/me/calendars")
        assert "private-message" not in str(error.value)
        with pytest.raises(OutlookBoundaryError, match="invalid principal"):
            await source.validate()
        assert source.graph.verified_principal_id is None


@pytest.mark.asyncio
async def test_attachment_pagination_cannot_swallow_unsafe_continuation():
    calls = []

    def graph(request):
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "value": [],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/other/messages?token=private",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await OutlookMailSource.create(
            auth=StaticTokenProvider("secret"),
            logger=MagicMock(),
            http_client=client,
            config=OutlookMailConfig(),
        )
        with pytest.raises(OutlookBoundaryError, match="outside the bound mailbox"):
            await anext(source._process_attachments("message-id", [], None))
        assert len(calls) == 1
        assert calls[0].url.path == "/v1.0/me/messages/message-id/attachments"
        assert "private" not in str(source.logger.method_calls)


@pytest.mark.parametrize("capture_originals", [False, True])
async def test_source_creation_selects_capture_adapter_without_replacing_auth(capture_originals):
    from airweave.domains.entities.canonical.page_source import CanonicalPageSource

    def graph(request):
        assert request.url.path == "/v1.0/me"
        return httpx.Response(200, json={"id": "principal"})

    auth = StaticTokenProvider("synthetic")
    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await OutlookMailSource.create(
            auth=auth,
            logger=MagicMock(),
            http_client=client,
            config=OutlookMailConfig(
                capture_originals=capture_originals,
                expected_principal_id="principal",
                included_folders=[],
                excluded_folders=[],
            ),
        )
        assert source.auth is auth
        if capture_originals:
            assert isinstance(source.capture_page_source, CanonicalPageSource)
            assert source.capture_page_source.graph is source.graph
        else:
            assert source.capture_page_source is None


async def test_delta_expiry_classification_requires_structured_code_and_delta_endpoint():
    from airweave.domains.sources.exceptions import SourceEntityNotFoundError
    from airweave.platform.sources.outlook_graph import OutlookDeltaExpiredError, OutlookGraphClient

    code = "syncStateNotFound"

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        return httpx.Response(404, json={"error": {"code": code, "message": "private-value"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = OutlookGraphClient(
            StaticTokenProvider("secret"), client, "outlook_mail", "principal"
        )
        await source.verify_principal()
        delta = "https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages/delta"
        with pytest.raises(OutlookDeltaExpiredError) as error:
            await source.get(delta)
        assert "private-value" not in str(error.value)
        with pytest.raises(SourceEntityNotFoundError):
            await source.get("https://graph.microsoft.com/v1.0/me/messages/message")
        code = "ErrorItemNotFound"
        with pytest.raises(SourceEntityNotFoundError):
            await source.get(delta)


def test_owned_calendar_config_requires_principal_and_preserves_explicit_window():
    """Owned activation needs identity; recurrence bounds are explicit and reproducible."""
    from datetime import datetime, timezone

    from pydantic import ValidationError

    from airweave.platform.configs.config import CalendarOccurrenceWindow

    with pytest.raises(ValidationError, match="expected_principal_id"):
        OutlookCalendarConfig(capture_originals=True)
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    end = datetime(2026, 11, 1, tzinfo=timezone.utc)
    explicit = CalendarOccurrenceWindow(start=start, end=end)
    config = OutlookCalendarConfig(
        capture_originals=True,
        expected_principal_id="principal",
        occurrence_window=explicit,
    )
    assert config.resolved_window() == explicit
    assert not OutlookCalendarConfig().capture_originals
    with pytest.raises(ValidationError):
        OutlookCalendarConfig(occurrence_window={"start": end, "end": start})
