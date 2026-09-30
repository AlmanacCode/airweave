"""Capture Wispr's available meeting representations through account-bound sessions."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from urllib.parse import quote

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StrictBool

from airweave.core.logging import ContextualLogger
from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import CapturePage
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.token_providers.protocol import ManagedToolAuthProvider
from airweave.domains.storage.file_service import FileService
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
    meetings: list[dict[str, JsonValue]] = Field(max_length=200)
    has_more: StrictBool
    next_cursor: str | None = None
    truncated: StrictBool = False


class _Listing(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(min_length=1)


class _MeetingVersion(BaseModel):
    model_config = ConfigDict(extra="ignore")
    modified_at: AwareDatetime | None = None


class _Window(BaseModel):
    model_config = ConfigDict(extra="forbid")
    since: AwareDatetime | None = None
    until: AwareDatetime | None = None
    depth: int = Field(default=0, ge=0, le=32)


class _MeetingStart(BaseModel):
    model_config = ConfigDict(extra="ignore")
    start: AwareDatetime | None = None


class _Progress(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cursor: str | None = Field(default=None, min_length=1, max_length=8192)
    seen: list[str] = Field(default_factory=list, max_length=1000)
    count: int = Field(default=0, ge=0, le=1000)
    window: _Window = Field(default_factory=_Window)
    pending: list[_Window] = Field(default_factory=list, max_length=33)
    dates_complete: bool = True
    earliest: AwareDatetime | None = None
    latest: AwareDatetime | None = None


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

    canonical_record_types = ("meeting_listing", "meeting")
    canonical_container_parents = {"meeting": "meeting_listing"}

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Bind durable discovery policy to this account and representation version."""
        return self._cycle_configuration

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
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "version": 2,
                    "account": auth.connected_account_id,
                    "inventory": "meeting_listing",
                    "body": "meeting",
                    "policy": "discovery_only",
                    "listing_limit": 200,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        instance._cycle_configuration = CycleConfiguration.from_source(
            fingerprint=fingerprint,
            record_types=cls.canonical_record_types,
            container_parents=cls.canonical_container_parents,
            completion_policies={"meeting_listing": "discovery_only"},
        )
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

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Keep each body attached to its exact native listing observation."""
        if parent.identity.record_type != "meeting_listing" or record_type != "meeting":
            raise ValueError("Unsupported Wispr child scope")
        # Preserve existing provider-global body identity, including null container.
        return CompletedScope(record_type="meeting", parent=parent.identity)

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Wispr supplies no audited absence or revocation proof."""
        raise ValueError("Wispr listing omission is not deletion or revocation evidence")

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Commit listing pages and individual bodies through one durable frontier.

        Completed bodies remain observed state in this cycle even if a later attempt
        refreshes listing metadata. The next cycle refreshes them; this is no snapshot.
        """
        if scope.record_type == "meeting_listing" and scope.parent is None and parent is None:
            return await self._listing_page(continuation)
        if (
            scope.record_type != "meeting"
            or parent is None
            or parent.identity.record_type != "meeting_listing"
            or scope.parent != parent.identity
            or scope.container_id is not None
            or continuation.value
        ):
            raise ValueError("Unsupported Wispr body scope or continuation")
        identity = _Listing.model_validate(parent.payload).id
        if identity != parent.identity.native_id:
            raise ValueError("Wispr listing identity does not match its scope")
        payload = await self._meeting(identity)
        payload["listing"] = parent.payload
        modified = _MeetingVersion.model_validate(payload["responses"][0]["response"]).modified_at
        return CapturePage(
            records=(
                CaptureRecord(
                    identity=RecordIdentity(record_type="meeting", native_id=identity),
                    parent=parent.identity,
                    payload=payload,
                    completeness="partial",
                    source_updated_at=modified,
                    observed_at=datetime.now(timezone.utc),
                ),
            ),
            continuation=ScanContinuation(),
            final=True,
        )

    async def _listing_page(self, continuation: ScanContinuation) -> CapturePage:
        progress = _Progress.model_validate(continuation.value)
        arguments: dict[str, JsonValue] = {"limit": 200}
        if progress.cursor:
            arguments["cursor"] = progress.cursor
        if progress.window.since:
            arguments["since"] = progress.window.since.isoformat()
        if progress.window.until:
            arguments["until"] = progress.window.until.isoformat()
        page = MeetingPage.model_validate(
            await self._execute("WISPR_FLOW_MCP_SEARCH_MEETINGS", arguments)
        )
        count = progress.count + len(page.meetings)
        following = page.next_cursor if page.has_more else None
        starts = [_MeetingStart.model_validate(row).start for row in page.meetings]
        dates_complete = progress.dates_complete and all(value is not None for value in starts)
        known_starts = [value for value in starts if value is not None] + [
            d for d in (progress.earliest, progress.latest) if d is not None
        ]
        earliest = min(known_starts) if known_starts else None
        latest = max(known_starts) if known_starts else None
        capped = page.truncated or count > 1000 or (page.has_more and count >= 1000)
        if capped:
            if not dates_complete:
                raise ValueError("Wispr query cap cannot be partitioned safely")
            next_progress = self._partition(progress, earliest, latest)
            final = False
        elif page.has_more:
            following_digest = (
                hashlib.sha256(following.encode()).hexdigest()[:32] if following else None
            )
            if not following or following == progress.cursor or following_digest in progress.seen:
                raise ValueError("Wispr returned incomplete or repeated pagination")
            next_progress = _Progress.model_validate(
                {
                    **progress.model_dump(),
                    "dates_complete": dates_complete,
                    "cursor": following,
                    "count": count,
                    "earliest": earliest,
                    "latest": latest,
                    "seen": [*progress.seen, following_digest],
                }
            )
            final = False
        elif progress.pending:
            next_progress = _Progress(window=progress.pending[-1], pending=progress.pending[:-1])
            final = False
        else:
            next_progress = _Progress()
            final = True
        identities = [_Listing.model_validate(row).id for row in page.meetings]
        if len(identities) != len(set(identities)):
            raise ValueError("Wispr listing page repeats a meeting identity")
        records = tuple(
            CaptureRecord(
                identity=RecordIdentity(record_type="meeting_listing", native_id=identity),
                payload=row,
                completeness="metadata_only",
                observed_at=datetime.now(timezone.utc),
            )
            for identity, row in zip(identities, page.meetings, strict=True)
        )
        return CapturePage(
            records=records,
            continuation=ScanContinuation(value=next_progress.model_dump(mode="json")),
            final=final,
        )

    @staticmethod
    def _partition(
        progress: _Progress, earliest: datetime | None, latest: datetime | None
    ) -> _Progress:
        if earliest is None or latest is None or earliest == latest or progress.window.depth >= 32:
            raise ValueError("Wispr query cap cannot be partitioned safely")
        split = earliest + (latest - earliest) / 2
        window = progress.window
        if (window.since is not None and split <= window.since) or (
            window.until is not None and split >= window.until
        ):
            raise ValueError("Wispr date partition made no progress")
        return _Progress(
            window=_Window(since=window.since, until=split, depth=window.depth + 1),
            pending=[
                *progress.pending,
                _Window(since=split, until=window.until, depth=window.depth + 1),
            ],
        )
