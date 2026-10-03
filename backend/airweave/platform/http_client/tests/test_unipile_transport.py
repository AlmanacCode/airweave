"""Focused simulated native contract tests; these do not qualify a live account."""

from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.storage import FileSkippedException
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.unipile_transport import UnipileError, UnipileWhatsAppClient
from airweave.platform.sources.records.whatsapp_models import (
    WhatsAppAccount,
    WhatsAppChat,
    WhatsAppMessage,
    WhatsAppPage,
)

MESSAGE = {
    "object": "Message",
    "provider": "whatsapp",
    "id": "msg",
    "chat_id": "group@lid",
    "sender_id": "self@lid",
    "timestamp": "2026-10-02T00:00:00Z",
    "is_sender": False,
}


def client(raw, **kwargs):
    return UnipileWhatsAppClient(
        AirweaveHttpClient(raw, uuid4(), "whatsapp", feature_flag_enabled=False),
        account_id="acc_bound",
        api_key="private-test-token",
        **kwargs,
    )


@pytest.mark.asyncio
async def test_exact_account_encoded_ids_and_cursor_context():
    requests = []

    def respond(request):
        requests.append(request)
        assert request.method == "GET"
        assert request.headers["x-api-key"] == "private-test-token"
        return httpx.Response(200, json={"data": [], "next_cursor": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        api = client(raw)
        await api.messages("x/y?other=acc_other#z%40lid", cursor="opaque")
        assert api.account_id == "acc_bound"
    assert requests[0].url.raw_path == (
        b"/v2/acc_bound/chats/x%2Fy%3Fother%3Dacc_other%23z%2540lid/messages?cursor=opaque"
    )
    assert requests[0].url.host == "api.unipile.com"


@pytest.mark.asyncio
async def test_wrong_parent_and_account_fail_before_capture():
    def respond(request):
        if request.url.path.endswith("accounts/acc_bound"):
            return httpx.Response(
                200,
                json={
                    "object": "Account",
                    "id": "acc_other",
                    "provider": "whatsapp",
                    "user_id": "self@lid",
                    "status": "running",
                    "is_locked": False,
                },
            )
        return httpx.Response(200, json={"data": [MESSAGE | {"chat_id": "wrong"}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        api = client(raw)
        with pytest.raises(UnipileError, match="identity"):
            await api.account()
        with pytest.raises(UnipileError, match="identity"):
            await api.messages("group@lid")


@pytest.mark.asyncio
async def test_non_whatsapp_account_rejected():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "object": "Account",
                    "id": "acc_bound",
                    "provider": "telegram",
                    "user_id": "self",
                    "status": "running",
                    "is_locked": False,
                },
            )
        )
    ) as raw:
        with pytest.raises(UnipileError, match="protocol"):
            await client(raw).account()


@pytest.mark.asyncio
async def test_structured_errors_keep_timing_without_private_details(caplog):
    responses = iter(
        [
            (
                401,
                {
                    "object": "Error",
                    "type": "provider/invalid_authorization",
                    "status": 401,
                    "detail": "secret-message-private-test-token",
                },
                {},
            ),
            (
                200,
                {
                    "object": "Error",
                    "type": "api/too_many_requests",
                    "status": 429,
                    "detail": "secret-message",
                },
                {"retry-after": "12.5"},
            ),
        ]
    )

    def respond(request):
        status, data, headers = next(responses)
        return httpx.Response(status, json=data, headers=headers)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        api = client(raw)
        with pytest.raises(UnipileError) as auth:
            await api.account()
        assert auth.value.kind == "authentication"
        assert auth.value.native_type == "provider/invalid_authorization"
        with pytest.raises(UnipileError) as rate:
            await api.account()
        assert rate.value.kind == "rate_limit"
        assert rate.value.retry_after == 12.5
        assert "secret-message" not in str(auth.value)
        assert "private-test-token" not in caplog.text
        assert "secret-message" not in caplog.text


class Chunks(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = False
        self.remaining = 2

    def __aiter__(self):
        """Return the owned stream iterator."""
        return self

    async def __anext__(self):
        """Yield finite test bytes without hidden generator ownership."""
        if not self.remaining:
            raise StopAsyncIteration
        self.remaining -= 1
        return b"x" * 8

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_stream_bound_closes_body_and_redirect_never_forwards_credentials():
    chunks = Chunks()
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, stream=chunks)
        return httpx.Response(302, headers={"location": "https://evil.invalid/private?secret=yes"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as raw:
        api = client(raw, max_body_bytes=10)
        with pytest.raises(UnipileError, match="protocol"):
            await api.account()
        assert chunks.closed
        with pytest.raises(UnipileError):
            await api.account()
    assert len(calls) == 2
    assert all(request.url.host == "api.unipile.com" for request in calls)


def test_lossless_native_fields_and_declared_pagination_semantics():
    native = MESSAGE | {"new_native_field": {"important": [1, None]}, "text": None}
    assert WhatsAppMessage.model_validate(native).original() == native
    missing = WhatsAppPage[WhatsAppMessage].model_validate({"data": [native]})
    explicit = WhatsAppPage[WhatsAppMessage].model_validate({"data": [], "next_cursor": None})
    assert "next_cursor" not in missing.model_fields_set
    assert "next_cursor" in explicit.model_fields_set
    # These helpers are only valid after capture declares mode explicitly.
    assert missing.cursor_after(None) is None
    assert missing.offset_after(0, 20) == 20  # Short page does not establish exhaustion.
    assert explicit.offset_after(20, 20) is None
    repeated = WhatsAppPage[WhatsAppMessage].model_validate({"data": [], "next_cursor": "same"})
    with pytest.raises(ValueError, match="no progress"):
        repeated.cursor_after("same")
    with pytest.raises(ValueError, match="contradicts"):
        repeated.offset_after(0, 20)


@pytest.mark.asyncio
async def test_attachment_fixed_route_bound_and_content_type():
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                200, content=b"audio-bytes", headers={"content-type": "audio/ogg"}
            )
        return httpx.Response(
            200, content=b"<html>private</html>", headers={"content-type": "text/html"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        api = client(raw)
        assert await api.attachment("g@lid", "msg", "a/b", max_bytes=100) == (
            b"audio-bytes",
            "audio/ogg",
        )
        with pytest.raises(UnipileError, match="protocol"):
            await api.attachment(
                "g@lid", "msg", "a/b", max_bytes=100, expected_mimetype="audio/ogg"
            )
        with pytest.raises(FileSkippedException):
            await api.attachment("g@lid", "msg", "a/b", max_bytes=1)
    assert calls[0].url.raw_path.endswith(b"/messages/msg/attachments/a%2Fb")


def test_unsupported_origins_and_cross_account_constructor_rejected():
    for origin in [
        "http://api.unipile.com/v2",
        "https://evil.invalid/v2",
        "https://api.unipile.com:443/v2",
        "https://api.unipile.com/v1",
    ]:
        with pytest.raises(UnipileError):
            UnipileWhatsAppClient(None, account_id="acc_bound", api_key="secret", base_url=origin)
    with pytest.raises(UnipileError):
        UnipileWhatsAppClient(None, account_id="acc_a/../acc_b", api_key="secret")


def test_optional_has_more_vetoes_contradictory_end_without_selecting_mode():
    for continuation in ({}, {"next_cursor": None}):
        page = WhatsAppPage[WhatsAppMessage].model_validate(
            {"data": [], "has_more": True, **continuation}
        )
        with pytest.raises(ValueError, match="has_more contradicts"):
            page.cursor_after(None)
        with pytest.raises(ValueError, match="has_more contradicts"):
            page.offset_after(0, 20)
    continuing = WhatsAppPage[WhatsAppMessage].model_validate(
        {"data": [MESSAGE], "next_cursor": "next", "has_more": False}
    )
    with pytest.raises(ValueError, match="has_more contradicts"):
        continuing.cursor_after(None)
    terminal = WhatsAppPage[WhatsAppMessage].model_validate(
        {"data": [], "has_more": False, "native_extra": {"retained": 1}}
    )
    assert terminal.cursor_after(None) is None
    assert terminal.offset_after(20, 20) is None
    assert terminal.original() == {"data": [], "has_more": False, "native_extra": {"retained": 1}}
    # In offset mode only an empty page establishes the documented end.
    short = WhatsAppPage[WhatsAppMessage].model_validate({"data": [MESSAGE], "has_more": False})
    assert short.offset_after(0, 20) == 20
    progressing = continuing.model_copy(update={"has_more": True})
    assert progressing.cursor_after(None) == "next"
    assert "has_more" not in WhatsAppPage[WhatsAppMessage](data=[]).original()
    for invalid in (1, "false", []):
        with pytest.raises(ValidationError):
            WhatsAppPage[WhatsAppMessage].model_validate({"data": [], "has_more": invalid})


def test_chat_message_preview_is_lossless_and_not_a_full_message():
    chat = {
        "object": "Chat",
        "provider": "whatsapp",
        "id": "group@lid",
        "is_group": True,
        "is_1to1": False,
        "is_channel": False,
        "last_message": {"object": "MessagePreview", "text": "引用", "unknown": None},
    }
    parsed = WhatsAppChat.model_validate(chat)
    assert parsed.original() == chat
    assert parsed.last_message.object == "MessagePreview"
    full = chat | {"last_message": MESSAGE}
    assert WhatsAppChat.model_validate(full).original() == full
    with pytest.raises(ValidationError):
        WhatsAppChat.model_validate(chat | {"last_message": {"object": "Message"}})


@pytest.mark.asyncio
async def test_owner_alias_resolution_does_not_relax_native_user_identity():
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"object": "UserProfile", "id": "native@lid"})

    account = WhatsAppAccount.model_validate(
        {
            "object": "Account",
            "id": "acc_bound",
            "provider": "whatsapp",
            "user_id": "15550000000",
            "status": "running",
            "is_locked": False,
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        api = client(raw)
        assert (await api.owner_profile(account)).id == "native@lid"
        with pytest.raises(UnipileError, match="identity"):
            await api.owner_profile(account.model_copy(update={"id": "acc_other"}))
        assert len(calls) == 1
        with pytest.raises(UnipileError, match="identity"):
            await api.user(account.user_id)
        assert (await api.user("native@lid")).id == "native@lid"
