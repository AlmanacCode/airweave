"""Sanitized smoke output and hard read budgets using the actual bounded adapter."""

from uuid import uuid4

import httpx
import pytest

from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.unipile_transport import UnipileError, UnipileWhatsAppClient
from airweave.platform.sources.records.whatsapp_models import WhatsAppMessage, WhatsAppPage
from scripts.smoke_unipile_whatsapp import ScopeResult, SmokeResult, message_dates, parser, qualify


@pytest.mark.asyncio
async def test_smoke_bounds_calls_and_reports_terminals_without_private_contents():
    calls = []

    def respond(request):
        calls.append(request)
        path = request.url.path
        if path.endswith("/accounts/acc_bound"):
            payload = {
                "object": "Account",
                "id": "acc_bound",
                "provider": "whatsapp",
                "user_id": "private-owner",
                "status": "running",
                "is_locked": False,
            }
        elif "/users/" in path:
            payload = {"object": "UserProfile", "id": "private-native@lid"}
        elif path.endswith("/messages"):
            payload = {"data": []}
        else:
            payload = {
                "data": [
                    {
                        "object": "Chat",
                        "id": "private-chat@lid",
                        "provider": "whatsapp",
                        "name": "PRIVATE CONTENT",
                        "is_group": True,
                        "is_1to1": False,
                        "is_channel": False,
                    }
                ]
            }
        return httpx.Response(200, json=payload)

    args = parser().parse_args(
        ["--account-id", "acc_bound", "--max-pages", "1", "--max-calls", "4"]
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        api = UnipileWhatsAppClient(
            AirweaveHttpClient(raw, uuid4(), "whatsapp", feature_flag_enabled=False),
            account_id="acc_bound",
            api_key="PRIVATE KEY",
        )
        report = SmokeResult()
        await qualify(api, args, report)
        assert report.ok and report.requests == len(calls) == 4
        assert report.owner_binding == "discovered_only" and not report.enrollment_proven
        assert report.chats.end == "page_budget_limited"
        assert report.messages.end == "documented_empty_offset_terminal"
        assert all(
            value not in report.model_dump_json()
            for value in (
                "private-owner",
                "private-native",
                "private-chat",
                "PRIVATE CONTENT",
                "PRIVATE KEY",
            )
        )
        args.max_calls = 3
        report = SmokeResult()
        calls.clear()
        await qualify(api, args, report)
        assert report.requests == len(calls) == 3
        assert report.messages.end == "call_budget_limited"
        args.account_user_id, args.native_user_id = "wrong-owner", "private-native@lid"
        calls.clear()
        with pytest.raises(UnipileError, match="identity"):
            await qualify(api, args, SmokeResult())
        assert len(calls) == 2


def test_smoke_rejects_unbounded_arguments():
    for argument, value in (
        ("--max-calls", "3"),
        ("--max-calls", "11"),
        ("--max-pages", "4"),
        ("--page-size", "100"),
    ):
        with pytest.raises(SystemExit):
            parser().parse_args(["--account-id", "acc_bound", argument, value])


def test_smoke_dates_are_utc_and_naive_timestamps_are_counted():
    message = {
        "object": "Message",
        "provider": "whatsapp",
        "id": "synthetic",
        "chat_id": "chat",
        "sender_id": "self",
        "is_sender": False,
        "timestamp": "2026-10-03T00:30:00+02:00",
    }
    page = WhatsAppPage[WhatsAppMessage].model_validate(
        {
            "data": [message, message | {"timestamp": "2026-10-03"}],
        }
    )
    report = ScopeResult()
    assert message_dates(page, report) == ["2026-10-02"]
    assert report.malformed_timestamps == 1
