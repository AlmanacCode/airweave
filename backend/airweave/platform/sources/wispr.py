"""Capture Wispr's available meeting representations through account-bound sessions."""

from __future__ import annotations

import re
from contextlib import aclosing
from datetime import datetime, timezone
from typing import AsyncGenerator
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, JsonValue

from airweave.core.logging import ContextualLogger
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.entities.canonical.requests import CaptureRecord, RecordIdentity
from airweave.domains.sources.token_providers.protocol import ManagedToolAuthProvider
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.decorators import source
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.composio_transport import ComposioTransport
from airweave.platform.sources._base import BaseSource
from airweave.schemas.source_connection import AuthenticationMethod


class SessionResponse(BaseModel):
    """Only the session identifier is needed; other routing metadata is ignored."""

    session_id: str


class ToolResponse(BaseModel):
    """Preserve the entire native tool result, without persisting Composio log IDs."""

    data: dict[str, JsonValue]
    error: JsonValue = None


class MeetingPage(BaseModel):
    """Wispr's real search response pagination fields."""

    model_config = ConfigDict(extra="ignore")
    meetings: list[dict[str, JsonValue]]
    has_more: bool
    next_cursor: str | None = None
    truncated: bool = False


@source(
    name="Wispr",
    short_name="wispr",
    auth_methods=[AuthenticationMethod.AUTH_PROVIDER],
    labels=["Meetings"],
    supports_continuous=False,
)
class WisprSource(BaseSource):
    """Capture available notes, summaries and transcript ranges; never infer deletions.

    The provider omits its raw editor JSON and has no deletion feed or snapshot cursor.
    Captures remain explicitly partial, even after every exposed text range is fetched.
    """

    canonical_record_types = ("meeting",)
    canonical_container_parents: dict[str, str] = {}

    @classmethod
    async def create(
        cls,
        *,
        auth: ManagedToolAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: BaseModel | None = None,
    ) -> WisprSource:
        """Require an explicit tool session capability, never a raw token."""
        if not isinstance(auth, ManagedToolAuthProvider):
            raise TypeError("Wispr requires managed tool authentication")
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance._tool_auth = auth
        instance._session_id: str | None = None
        return instance

    async def _post(self, path: str, body: dict) -> dict:
        response = await self.http_client.post(
            "https://backend.composio.dev/api/v3.1/tool_router/" + path,
            headers={"x-api-key": self._tool_auth.api_key.get_secret_value()},
            json=body,
        )
        ComposioTransport._check_proxy_response(response, response.request)
        return response.json()

    async def _execute(self, slug: str, arguments: dict) -> dict[str, JsonValue]:
        if self._session_id is None:
            session = SessionResponse.model_validate(
                await self._post(
                    "session",
                    {
                        "user_id": self._tool_auth.user_id,
                        "connected_accounts": {
                            "wispr_flow_mcp": [self._tool_auth.connected_account_id]
                        },
                        "toolkits": {"enable": ["wispr_flow_mcp"]},
                        "manage_connections": {"enable": False},
                    },
                )
            )
            self._session_id = session.session_id
        result = ToolResponse.model_validate(
            await self._post(
                "session/" + quote(self._session_id, safe="") + "/execute",
                {
                    "tool_slug": slug,
                    "arguments": arguments,
                },
            )
        )
        if result.error is not None:
            raise ValueError("Wispr tool execution failed; capture is incomplete")
        return result.data

    async def validate(self) -> None:
        """Verify the bound session can actually read meetings."""
        MeetingPage.model_validate(
            await self._execute("WISPR_FLOW_MCP_SEARCH_MEETINGS", {"limit": 1})
        )

    async def _list_window(
        self,
        since: datetime | None,
        until: datetime | None,
    ) -> tuple[list[dict[str, JsonValue]], bool]:
        arguments: dict[str, JsonValue] = {"limit": 200}
        if since is not None:
            arguments["since"] = since.isoformat()
        if until is not None:
            arguments["until"] = until.isoformat()
        records: list[dict[str, JsonValue]] = []
        seen_cursors: set[str] = set()
        while True:
            page = MeetingPage.model_validate(
                await self._execute("WISPR_FLOW_MCP_SEARCH_MEETINGS", arguments)
            )
            records.extend(page.meetings)
            if page.truncated:
                return records, True
            if not page.has_more:
                return records, False
            if not page.next_cursor or page.next_cursor in seen_cursors:
                raise ValueError("Wispr returned incomplete or repeated pagination")
            if len(records) >= 1000:
                return records, True
            seen_cursors.add(page.next_cursor)
            arguments["cursor"] = page.next_cursor

    async def _list_all(self) -> AsyncGenerator[dict[str, JsonValue], None]:
        windows: list[tuple[datetime | None, datetime | None, int]] = [(None, None, 0)]
        seen_ids: set[str] = set()
        while windows:
            since, until, depth = windows.pop()
            records, truncated = await self._list_window(since, until)
            if truncated:
                starts = [
                    datetime.fromisoformat(str(row["start"]).replace("Z", "+00:00"))
                    for row in records
                ]
                if not starts or min(starts) == max(starts) or depth >= 32:
                    raise ValueError("Wispr query cap cannot be partitioned safely")
                split = min(starts) + (max(starts) - min(starts)) / 2
                if (since is not None and split <= since) or (until is not None and split >= until):
                    raise ValueError("Wispr date partition made no progress")
                windows.extend([(since, split, depth + 1), (split, until, depth + 1)])
                continue
            for record in records:
                identity = record.get("id")
                if not isinstance(identity, str) or not identity:
                    raise ValueError("Wispr returned a meeting without identity")
                if identity not in seen_ids:
                    seen_ids.add(identity)
                    yield record

    @staticmethod
    def _next_offset(text: JsonValue, field: str) -> int | None:
        if not isinstance(text, str):
            raise ValueError("Wispr meeting text is not a string")
        match = re.search(
            rf"\(\.\.\.truncated, \d+ chars remaining; continue with "
            rf"view_{field}\.start_char=(\d+)\.\.\.\)\s*$",
            text,
            re.MULTILINE,
        )
        return int(match.group(1)) if match else None

    async def _meeting(self, identity: str) -> dict[str, JsonValue]:
        offsets = {"content": 0, "transcript": 0}
        responses: list[JsonValue] = []
        for _ in range(100):
            arguments = {
                "meeting_id": identity,
                **{
                    "view_" + field: {"char_limit": 40000, "start_char": offset}
                    for field, offset in offsets.items()
                },
            }
            response = await self._execute("WISPR_FLOW_MCP_GET_MEETING", arguments)
            if response.get("id") != identity:
                raise ValueError("Wispr returned a different meeting identity")
            if responses and response.get("modified_at") != responses[0]["response"].get(
                "modified_at"
            ):
                raise ValueError("Wispr meeting changed during paginated capture")
            responses.append({"requested_ranges": arguments, "response": response})
            following = {}
            for field, offset in offsets.items():
                next_offset = self._next_offset(response.get(field), field)
                if next_offset is not None:
                    if next_offset <= offset:
                        raise ValueError("Wispr continuation made no progress")
                    following[field] = next_offset
            if not following:
                return {"responses": responses}
            # Request only continuing ranges; complete ranges already remain in responses.
            offsets = following
        raise ValueError("Wispr meeting exceeds the bounded capture page limit")

    async def generate_observations(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[CaptureRecord, None]:
        """Capture full exposed ranges without claiming deletion or snapshot completeness."""
        if node_selections:
            raise ValueError("Wispr capture does not support selected meeting scopes")
        async with aclosing(self._list_all()) as meetings:
            async for listing in meetings:
                identity = str(listing["id"])
                payload = await self._meeting(identity)
                payload["listing"] = listing
                yield CaptureRecord(
                    identity=RecordIdentity(record_type="meeting", native_id=identity),
                    payload=payload,
                    completeness="partial",
                    observed_at=datetime.now(timezone.utc),
                )
