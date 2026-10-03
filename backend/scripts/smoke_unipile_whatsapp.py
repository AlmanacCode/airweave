"""Internal read-only WhatsApp v2 smoke; counts only, no capture or enrollment writes."""

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from airweave.platform.http_client.unipile_transport import UnipileWhatsAppClient
    from airweave.platform.sources.records.whatsapp_models import WhatsAppMessage, WhatsAppPage


class ScopeResult(BaseModel):
    """Sanitized finite traversal evidence, not a complete history guarantee."""

    pages: int = 0
    items: int = 0
    unique_items: int = 0
    end: str = "not_sampled"
    next_cursor_supplied: bool = False
    oldest_day: str | None = None
    newest_day: str | None = None
    malformed_timestamps: int = 0


class SmokeResult(BaseModel):
    """Exclude provider identifiers and native payloads from every report."""

    ok: bool = Field(
        default=False, description="Bounded requests and schemas passed; not readiness"
    )
    requests: int = 0
    owner_binding: str = "unverified"
    enrollment_proven: bool = False
    account_running: bool | None = None
    initial_sync_status: str | None = None
    pagination: str = "offset"
    chats: ScopeResult = Field(default_factory=ScopeResult)
    messages: ScopeResult = Field(default_factory=ScopeResult)
    error: str | None = None
    error_status: int | None = None
    seconds: float = 0


def parser() -> argparse.ArgumentParser:
    """Expose only bounded read controls; the API key is environment-only."""
    result = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Supply UNIPILE_API_KEY privately through the environment. Expected owner IDs "
            "must be supplied together; omitting both is discovery-only, not enrollment proof. "
            "Two calls attest account/owner. Minimal account/chat/message smoke: "
            "--max-pages 1 --max-calls 4 (when a chat exists). Larger chat-page budgets "
            "can leave messages unsampled. Page budgets apply separately per scope. "
            "Reported sample dates are UTC. A successful smoke is not full-history readiness."
        ),
    )
    result.add_argument("--account-id", required=True, help="Approved v2 account; never printed")
    result.add_argument(
        "--account-user-id", help="Expected account owner lookup ID; requires --native-user-id"
    )
    result.add_argument(
        "--native-user-id", help="Expected resolved stable profile ID; requires --account-user-id"
    )
    result.add_argument(
        "--page-size",
        type=int,
        choices=range(2, 6),
        default=2,
        help="Items per chat/message page (default: 2)",
    )
    result.add_argument(
        "--max-pages",
        type=int,
        choices=range(1, 4),
        default=2,
        help="Maximum pages per scope: chat list and one chat message list (default: 2)",
    )
    result.add_argument(
        "--max-calls",
        type=int,
        choices=range(4, 11),
        default=8,
        help="Total read budget including 2 identity calls (default ceiling: 8)",
    )
    return result


async def qualify(
    client: "UnipileWhatsAppClient", args: argparse.Namespace, report: SmokeResult
) -> None:
    """Attest owner then sample finite offset pages through the actual adapter."""
    from airweave.platform.http_client.unipile_transport import UnipileError  # noqa: PLC0415

    report.requests += 1
    account = await client.account()
    report.requests += 1
    owner = await client.owner_profile(account)
    if args.account_user_id is not None and (
        account.user_id != args.account_user_id or owner.id != args.native_user_id
    ):
        raise UnipileError("identity")
    report.owner_binding = "expected_values_matched" if args.account_user_id else "discovered_only"
    report.account_running = account.status == "running"
    report.initial_sync_status = account.initial_sync.status if account.initial_sync else None
    if account.is_locked:
        raise UnipileError("permission")
    if account.status != "running":
        raise UnipileError("authentication" if account.status == "disconnected" else "transient")

    selected_chat = await sample_scope(client, args, report, report.chats)
    if selected_chat is not None:
        await sample_scope(client, args, report, report.messages, chat_id=selected_chat)
    report.ok = True


def message_dates(page: "WhatsAppPage[WhatsAppMessage]", result: ScopeResult) -> list[str]:
    """Keep only calendar dates; malformed timestamps are counted without printing."""
    dates = []
    for item in page.data:
        try:
            timestamp = datetime.fromisoformat(item.timestamp.replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise ValueError("Timezone missing")
            dates.append(timestamp.astimezone(timezone.utc).date().isoformat())
        except ValueError:
            result.malformed_timestamps += 1
    return dates


async def sample_scope(
    client: "UnipileWhatsAppClient",
    args: argparse.Namespace,
    report: SmokeResult,
    result: ScopeResult,
    *,
    chat_id: str | None = None,
) -> str | None:
    """Bound one offset traversal; no inferred short-page or cursor-mode terminal."""
    from airweave.platform.http_client.unipile_transport import UnipileError  # noqa: PLC0415

    selected_chat = None
    offset = 0
    seen: set[str] = set()
    dates: list[str] = []
    for _ in range(args.max_pages):
        if report.requests >= args.max_calls:
            result.end = "call_budget_limited"
            break
        report.requests += 1
        if chat_id is None:
            page = await client.chats(offset=offset, limit=args.page_size)
        else:
            messages = await client.messages(chat_id, offset=offset, limit=args.page_size)
            dates.extend(message_dates(messages, result))
            page = messages
        result.pages += 1
        result.items += len(page.data)
        result.next_cursor_supplied |= "next_cursor" in page.model_fields_set
        ids = {item.id for item in page.data}
        if len(page.data) > args.page_size or len(ids) != len(page.data) or ids & seen:
            raise UnipileError("protocol")
        seen.update(ids)
        result.unique_items = len(seen)
        if selected_chat is None and page.data:
            selected_chat = page.data[0].id
        try:
            following = page.offset_after(offset, args.page_size)
        except ValueError:
            raise UnipileError("protocol") from None
        if following is None:
            result.end = "documented_empty_offset_terminal"
            break
        offset = following
        result.end = "page_budget_limited"
    if dates:
        result.oldest_day, result.newest_day = min(dates), max(dates)
    return selected_chat


async def run(args: argparse.Namespace) -> SmokeResult:
    """Import real engine dependencies after --help and suppress private library logs."""
    logging.disable(logging.CRITICAL)
    report = SmokeResult()
    loop = asyncio.get_running_loop()
    started = loop.time()
    key = os.environ.get("UNIPILE_API_KEY")
    if not key:
        report.error = "missing_environment_key"
        return report
    if (args.account_user_id is None) != (args.native_user_id is None):
        report.error = "expected_owner_ids_must_be_supplied_together"
        return report
    try:
        import httpx  # noqa: PLC0415

        from airweave.platform.http_client.airweave_client import (  # noqa: PLC0415
            AirweaveHttpClient,
        )
        from airweave.platform.http_client.unipile_transport import (  # noqa: PLC0415
            UnipileError,
            UnipileWhatsAppClient,
        )

        async with httpx.AsyncClient() as raw:
            client = UnipileWhatsAppClient(
                AirweaveHttpClient(raw, uuid4(), "whatsapp", feature_flag_enabled=False),
                account_id=args.account_id,
                api_key=key,
            )
            try:
                await qualify(client, args, report)
            except UnipileError as error:
                report.error, report.error_status = error.kind, error.status
    except Exception as error:
        # Neither exception text nor chained validation/native payloads are safe output.
        report.error = type(error).__name__
    report.seconds = round(loop.time() - started, 3)
    return report


def main() -> int:
    """Print one sanitized JSON report and a failure exit code when qualification fails."""
    report = asyncio.run(run(parser().parse_args()))
    print(json.dumps(report.model_dump(mode="json")))
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
