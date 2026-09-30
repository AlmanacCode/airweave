"""Native Drive principal attestation precedes every owned capture entrypoint."""

from unittest.mock import MagicMock

import httpx
import pytest

from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import GoogleDriveConfig
from airweave.platform.sources.google_drive import GoogleDriveSource


async def connector(client, expected="principal-a"):
    return await GoogleDriveSource.create(
        auth=StaticTokenProvider("synthetic"),
        logger=MagicMock(),
        http_client=client,
        config=GoogleDriveConfig(expected_permission_id=expected),
    )


@pytest.mark.parametrize(
    "user",
    [
        {"permissionId": "different", "me": True},
        {"permissionId": "principal-a", "me": False},
        {"permissionId": "principal-a", "me": "true"},
        {"permissionId": 123, "me": True},
        {"permissionId": "", "me": True},
        {"permissionId": "principal-a"},
        None,
    ],
)
async def test_invalid_principal_never_reaches_capture(user):
    calls = []

    def respond(request):
        calls.append(request.url.path)
        assert request.url.params["fields"] == "user(permissionId,me)"
        return httpx.Response(200, json={"user": user})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError) as error:
            await connector(client)
    assert calls == ["/drive/v3/about"]
    assert "principal-a" not in str(error.value) and "different" not in str(error.value)


async def test_revalidation_failure_revokes_direct_capture_and_resume():
    replies = ["principal-a", "different"]

    def respond(request):
        assert request.url.path == "/drive/v3/about"
        return httpx.Response(200, json={"user": {"permissionId": replies.pop(0), "me": True}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        source = await connector(client)
        original = source.capture_cycle_configuration
        with pytest.raises(ValueError, match="trusted binding"):
            await source.validate()
        with pytest.raises(ValueError, match="attested"):
            _ = source.capture_cycle_configuration
        with pytest.raises(ValueError, match="attested"):
            await source.prepare_cycle(None)
        with pytest.raises(ValueError, match="attested"):
            source.initial_continuation(MagicMock(configuration=original))
        with pytest.raises(ValueError, match="attested"):
            await source.capture_page(None, None, files=MagicMock())
        with pytest.raises(ValueError, match="attested"):
            await source.refresh_known(None, files=MagicMock())
    assert not replies


async def test_legacy_creation_is_allowed_but_canonical_capture_requires_binding():
    def respond(request):
        raise AssertionError("Legacy creation should not require a new permission binding")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        source = await connector(client, None)
        with pytest.raises(ValueError, match="attested"):
            await source.prepare_cycle(None)


async def test_fingerprint_and_resume_are_principal_bound():
    principals = iter(["principal-a", "principal-b"])

    def respond(request):
        return httpx.Response(200, json={"user": {"permissionId": next(principals), "me": True}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        first = await connector(client)
        second = await connector(client, "principal-b")
        assert first.capture_cycle_configuration != second.capture_cycle_configuration
        with pytest.raises(ValueError, match="trusted capture configuration"):
            second.initial_continuation(MagicMock(configuration=first.capture_cycle_configuration))
