"""Read-only account-bound Unipile v2 using shared bounded async HTTP.

The official Python SDK (6171191821e77c2d8f10e84c75c7237c5a7814d7)
uses synchronous urllib3 and its normal methods read unbounded bodies. Reuse
Airweave's async transport, rate limits and byte bound instead. No retries,
sessions, event ordering or account-enrollment lifecycle are owned here.
"""

from __future__ import annotations

import asyncio
import math
import re
from typing import Literal, TypeVar
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from airweave.domains.storage import FileSkippedException
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.bounded_response import bounded_response_bytes
from airweave.platform.sources.records.whatsapp_models import (
    WhatsAppAccount,
    WhatsAppChat,
    WhatsAppMessage,
    WhatsAppNativeModel,
    WhatsAppPage,
    WhatsAppParticipant,
    WhatsAppReaction,
    WhatsAppUser,
)

ErrorKind = Literal[
    "authentication",
    "permission",
    "identity",
    "rate_limit",
    "not_found",
    "unsupported",
    "transient",
    "protocol",
    "request",
]


class UnipileError(Exception):
    """Safe structured failure: never retains response bodies, URLs or credentials."""

    def __init__(
        self,
        kind: ErrorKind,
        *,
        status: int | None = None,
        native_type: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        """Expose only known error classification and documented retry timing."""
        self.kind = kind
        self.status = status
        self.native_type = native_type
        self.retry_after = retry_after
        super().__init__(f"Unipile {kind} failure" + (f" (HTTP {status})" if status else ""))


class _Problem(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    object: Literal["Error"]
    type: str
    status: int


_NATIVE_ERRORS = frozenset(
    {
        "api/invalid_parameters",
        "provider/invalid_parameters",
        "api/invalid_auth_format",
        "provider/invalid_authorization",
        "provider/invalid_credentials",
        "provider/account_mismatch",
        "provider/invalid_checkpoint_code",
        "api/missing_authorization",
        "api/expired_authorization",
        "api/proxy_auth_error",
        "api/inactive_subscription",
        "api/insufficient_permissions",
        "provider/insufficient_permissions",
        "api/account_restricted",
        "api/already_exists",
        "provider/unknown_authentication_context",
        "provider/resource_not_found",
        "api/resource_not_found",
        "provider/method_not_allowed",
        "api/conflict",
        "provider/conflict",
        "provider/invalid_file",
        "provider/unprocessable_entity",
        "provider/too_many_requests",
        "api/too_many_requests",
        "api/internal_error",
        "api/not_implemented",
        "api/proxy_error",
        "provider/server_error",
        "api/proxy_timeout",
        "provider/timeout",
    }
)
Model = TypeVar("Model", bound=WhatsAppNativeModel)


def _segment(value: str) -> str:
    """Encode opaque IDs as exactly one path component, including slashes and percent."""
    if not value or value in {".", ".."} or any(ord(char) < 32 for char in value):
        raise UnipileError("request")
    return quote(value, safe="")


def _pagination(cursor: str | None, offset: int | None, limit: int) -> dict[str, str | int]:
    if cursor is not None and (not cursor or offset is not None):
        raise UnipileError("request")
    if not 1 <= limit <= 100 or (offset is not None and offset < 0):
        raise UnipileError("request")
    # Official v2 API usage: cursor contains original query context.
    if cursor is not None:
        return {"cursor": cursor}
    params: dict[str, str | int] = {"limit": limit}
    if offset is not None:
        params["offset"] = offset
    return params


def _retry_after(value: str | None) -> float | None:
    """Retain supplied numeric timing only; never invent a retry delay."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if math.isfinite(seconds) and seconds >= 0 else None


class UnipileWhatsAppClient:
    """Only documented read routes for one bound account; no arbitrary URL method."""

    def __init__(
        self,
        http: AirweaveHttpClient,
        *,
        account_id: str,
        api_key: str,
        base_url: str = "https://api.unipile.com/v2",
        max_body_bytes: int = 2 * 1024 * 1024,
        timeout_seconds: float = 60,
    ) -> None:
        """Caller owns shared HTTP; v2 removed configurable v1 DSNs."""
        if base_url.rstrip("/") != "https://api.unipile.com/v2":
            raise UnipileError("request")
        if not re.fullmatch(r"acc_[A-Za-z0-9_-]+", account_id) or not api_key:
            raise UnipileError("request")
        if not 1 <= max_body_bytes <= 20 * 1024 * 1024:
            raise UnipileError("request")
        if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 180:
            raise UnipileError("request")
        self._timeout_seconds = timeout_seconds
        self._timeout = httpx.Timeout(timeout_seconds)
        self._http = http
        self._account_id = account_id
        self._api_key = api_key
        self._base = "https://api.unipile.com/v2"
        self._maximum = max_body_bytes

    @property
    def account_id(self) -> str:
        """Public binding identity used by the canonical capture boundary."""
        return self._account_id

    @staticmethod
    def _check_failure(status: int, body: bytes, headers: httpx.Headers) -> None:
        """Classify RFC7807 evidence without keeping private problem detail."""
        problem = None
        try:
            problem = _Problem.model_validate_json(body)
        except ValidationError:
            pass
        if problem is not None or not 200 <= status < 300:
            effective = problem.status if problem is not None else status
            native = problem.type if problem and problem.type in _NATIVE_ERRORS else None
            kind: ErrorKind = "request"
            if native == "provider/account_mismatch":
                kind = "identity"
            elif effective == 401:
                kind = "authentication"
            elif effective == 403:
                kind = "permission"
            elif effective == 429:
                kind = "rate_limit"
            elif effective == 404:
                kind = "not_found"
            elif effective in (405, 501):
                kind = "unsupported"
            elif effective >= 500:
                kind = "transient"
            raise UnipileError(
                kind,
                status=effective,
                native_type=native,
                retry_after=_retry_after(headers.get("retry-after")),
            )

    async def _get(self, path: str, model: type[Model], params=None) -> Model:
        try:
            async with (
                asyncio.timeout(self._timeout_seconds),
                self._http.stream(
                    "GET",
                    self._base + path,
                    params=params,
                    headers={"X-API-KEY": self._api_key, "Accept-Encoding": "identity"},
                    follow_redirects=False,
                    timeout=self._timeout,
                ) as response,
            ):
                status = response.status_code
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise UnipileError("protocol", status=status)
                maximum = min(self._maximum, 64 * 1024) if status >= 400 else self._maximum
                body = await bounded_response_bytes(response, maximum, label="Unipile response")
                self._check_failure(status, body, response.headers)
                try:
                    return model.model_validate_json(body)
                except ValidationError:
                    raise UnipileError("protocol", status=status) from None
        except FileSkippedException:
            raise UnipileError("protocol") from None
        except (httpx.RequestError, TimeoutError):
            raise UnipileError("transient") from None
        except httpx.HTTPStatusError as error:
            # Shared rate limiter can reject before any provider request.
            raise UnipileError(
                "rate_limit" if error.response.status_code == 429 else "transient",
                status=error.response.status_code,
                retry_after=_retry_after(error.response.headers.get("retry-after")),
            ) from None

    def _account_path(self, suffix: str) -> str:
        return f"/{self._account_id}{suffix}"

    async def account(self) -> WhatsAppAccount:
        """Re-attest the account and WhatsApp provider before acquiring content."""
        account = await self._get(f"/accounts/{self._account_id}", WhatsAppAccount)
        if account.id != self._account_id:
            raise UnipileError("identity")
        return account

    async def user(self, user_id: str) -> WhatsAppUser:
        """Read an exact participant; no display-name or phone-ID substitution."""
        user = await self._get(self._account_path(f"/users/{_segment(user_id)}"), WhatsAppUser)
        if user.id != user_id:
            raise UnipileError("identity")
        return user

    async def owner_profile(self, account: WhatsAppAccount) -> WhatsAppUser:
        """Resolve an attested owner lookup ID; WhatsApp may return its native LID.

        Unlike exact native user reads, the account owner lookup can be a phone
        identifier. Capture separately checks the resolved enrolled principal.
        """
        if account.id != self._account_id:
            raise UnipileError("identity")
        return await self._get(
            self._account_path(f"/users/{_segment(account.user_id)}"), WhatsAppUser
        )

    async def self_user(self) -> WhatsAppUser:
        """Resolve the owner of a freshly attested account, retaining returned ID."""
        return await self.owner_profile(await self.account())

    async def chats(self, *, cursor=None, offset=None, limit=20) -> WhatsAppPage[WhatsAppChat]:
        """Read a bounded wire page; capture owns declared pagination semantics."""
        return await self._get(
            self._account_path("/chats"),
            WhatsAppPage[WhatsAppChat],
            _pagination(cursor, offset, limit),
        )

    async def chat(self, chat_id: str) -> WhatsAppChat:
        """Read exact chat metadata under the bound account."""
        chat = await self._get(self._account_path(f"/chats/{_segment(chat_id)}"), WhatsAppChat)
        if chat.id != chat_id:
            raise UnipileError("identity")
        return chat

    async def messages(
        self,
        chat_id: str,
        *,
        cursor=None,
        offset=None,
        limit=20,
        before=None,
        after=None,
    ) -> WhatsAppPage[WhatsAppMessage]:
        """Read native messages; date filters are exclusive sent timestamps."""
        params = _pagination(cursor, offset, limit)
        if cursor is not None and (before is not None or after is not None):
            raise UnipileError("request")
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after
        page = await self._get(
            self._account_path(f"/chats/{_segment(chat_id)}/messages"),
            WhatsAppPage[WhatsAppMessage],
            params,
        )
        if any(message.chat_id != chat_id for message in page.data):
            raise UnipileError("identity")
        return page

    async def message(self, chat_id: str, message_id: str) -> WhatsAppMessage:
        """Read exact message; returned parent identity must match the request."""
        message = await self._get(
            self._account_path(f"/chats/{_segment(chat_id)}/messages/{_segment(message_id)}"),
            WhatsAppMessage,
        )
        if message.id != message_id or message.chat_id != chat_id:
            raise UnipileError("identity")
        return message

    async def participants(self, chat_id: str, *, cursor=None, offset=None, limit=20):
        """Read current participants, supported by the WhatsApp availability table."""
        return await self._get(
            self._account_path(f"/chats/{_segment(chat_id)}/participants"),
            WhatsAppPage[WhatsAppParticipant],
            _pagination(cursor, offset, limit),
        )

    async def reactions(self, chat_id: str, message_id: str, *, cursor=None, offset=None, limit=20):
        """Read current reactions; this is not historical event replay."""
        return await self._get(
            self._account_path(
                f"/chats/{_segment(chat_id)}/messages/{_segment(message_id)}/reactions"
            ),
            WhatsAppPage[WhatsAppReaction],
            _pagination(cursor, offset, limit),
        )

    async def attachment(
        self,
        chat_id: str,
        message_id: str,
        attachment_id: str,
        *,
        max_bytes: int,
        expected_mimetype: str | None = None,
    ) -> tuple[bytes, str]:
        """Download only a documented account-scoped route, never a returned URL."""
        if not 1 <= max_bytes <= 200 * 1024 * 1024:
            raise UnipileError("request")
        path = self._account_path(
            f"/chats/{_segment(chat_id)}/messages/{_segment(message_id)}"
            f"/attachments/{_segment(attachment_id)}"
        )
        try:
            async with (
                asyncio.timeout(self._timeout_seconds),
                self._http.stream(
                    "GET",
                    self._base + path,
                    headers={"X-API-KEY": self._api_key, "Accept-Encoding": "identity"},
                    follow_redirects=False,
                    timeout=self._timeout,
                ) as response,
            ):
                status = response.status_code
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise UnipileError("protocol", status=status)
                limit = min(max_bytes, 64 * 1024) if status >= 400 else max_bytes
                body = await bounded_response_bytes(response, limit, label="Unipile attachment")
                self._check_failure(status, body, response.headers)
                content_type = (
                    response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                )
                if not re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", content_type):
                    raise UnipileError("protocol", status=status)
                if content_type == "application/problem+json":
                    raise UnipileError("protocol", status=status)
                if (
                    expected_mimetype
                    and content_type != expected_mimetype.split(";", 1)[0].strip().lower()
                ):
                    raise UnipileError("protocol", status=status)
                return body, content_type
        except FileSkippedException:
            if 200 <= status < 300:
                raise
            raise UnipileError("protocol") from None
        except (httpx.RequestError, TimeoutError):
            raise UnipileError("transient") from None
        except httpx.HTTPStatusError as error:
            raise UnipileError(
                "rate_limit" if error.response.status_code == 429 else "transient",
                status=error.response.status_code,
                retry_after=_retry_after(error.response.headers.get("retry-after")),
            ) from None
