"""Slack capture of accessible conversations, with optional live search."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, AsyncGenerator, Dict, List, Optional
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    TypeAdapter,
    ValidationError,
    model_validator,
)
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.auth_provider.exceptions import AuthProviderRateLimitError
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    InvalidScanContinuation,
    ScopeAccessLost,
)
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    parent_container_key,
)
from airweave.domains.entities.canonical.scan_models import ChildScopeObservation, ScanContinuation
from airweave.domains.sources.exceptions import SourceAuthError, SourceError, SourceRateLimitError
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.auth import SlackAuthConfig
from airweave.platform.configs.config import SlackConfig
from airweave.platform.decorators import source
from airweave.platform.entities._base import AirweaveSystemMetadata, BaseEntity, Breadcrumb
from airweave.platform.entities.slack import SlackMessageEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.http_helpers import _parse_retry_after, raise_for_status
from airweave.platform.sources.slack_content import SlackMessageFiles, capture_slack_file
from airweave.platform.sources.slack_errors import SlackApiError
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType


def _message_timestamp(value: object) -> datetime | None:
    """Decode Slack decimal seconds exactly; unavailable native dates stay unknown."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,12}(?:\.[0-9]{1,6})?", value):
        return None
    seconds, _, fraction = value.partition(".")
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            seconds=int(seconds), microseconds=int(fraction.ljust(6, "0"))
        )
    except OverflowError:
        return None


def _conversation_timestamp(value: object, *, milliseconds: bool = False) -> datetime | None:
    """Conversation creation uses seconds; settings updates use milliseconds."""
    if type(value) is not int or value < 0:
        return None
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            microseconds=value * (1000 if milliseconds else 1_000_000)
        )
    except OverflowError:
        return None


# Pattern for Slack mrkdwn special sequences:
# <@U1234|display_name> → @display_name  (user mention with label)
# <@U1234>              → @U1234         (user mention without label)
# <#C1234|channel-name> → #channel-name  (channel mention with label)
# <#C1234>              → #C1234         (channel mention without label)
# <!subteam^S1234|@team> → @team         (user group mention)
# <!here>, <!channel>, <!everyone> → @here, @channel, @everyone
# <http://url|label>    → label          (link with label)
# <http://url>          → url            (link without label)
_SLACK_MRKDWN_PATTERN = re.compile(
    r"<"
    r"(?:"
    r"[@#!](?:[A-Z0-9^]+\|)?([^>|]+)"  # mentions: use label after pipe, or the ID
    r"|"
    r"(?:https?://[^|>]+)\|([^>]+)"  # links with label: use label
    r"|"
    r"(https?://[^>]+)"  # links without label: use URL
    r")"
    r">"
)


def _clean_slack_mrkdwn(text: str) -> str:
    """Strip Slack mrkdwn markup, replacing mentions and links with display text."""

    def _replace(m: re.Match[str]) -> str:
        if m.group(1):
            # Mention (user/channel/special) — use the label
            return m.group(1)
        # Link — use label or URL
        return m.group(2) or m.group(3) or ""

    cleaned = _SLACK_MRKDWN_PATTERN.sub(_replace, text)
    # Strip Slack search highlight markers (U+E000 / U+E001 wrap matched terms)
    cleaned = cleaned.replace("\ue000", "").replace("\ue001", "")
    # Collapse multiple spaces left by removed markup
    return re.sub(r"  +", " ", cleaned).strip()


class SlackPrincipal(BaseModel):
    """Native authorized workspace/user identity, with no display-name inference."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    ok: StrictBool
    team_id: str = Field(min_length=1, pattern=r"^\S+$")
    user_id: str = Field(min_length=1, pattern=r"^\S+$")

    @model_validator(mode="after")
    def successful(self):
        """Only an explicit successful auth.test response attests identity."""
        if not self.ok:
            raise ValueError("Slack identity request did not succeed")
        return self


class SlackHistoryCoverage(BaseModel):
    """Slack omits this flag unless older history is unavailable under its limit."""

    model_config = ConfigDict(extra="ignore")
    is_limited: StrictBool = False


class SlackPageMetadata(BaseModel):
    """Only pagination metadata is parsed; record JSON stays untouched."""

    next_cursor: str = Field(default="", max_length=4096)


class SlackRootContinuation(BaseModel):
    """Bounded conversation-list pagination state."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cursor: str = Field(default="", max_length=4096)
    recent: tuple[str, ...] = Field(default=(), max_length=16)


class SlackMessageContinuation(BaseModel):
    """One history page's thread queue, never the whole conversation in memory."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    history_cursor: str = Field(default="", max_length=4096)
    history_recent: tuple[str, ...] = Field(default=(), max_length=16)
    history_done: bool = False
    pending_threads: tuple[str, ...] = Field(default=(), max_length=100)
    reply_cursor: str = Field(default="", max_length=4096)
    reply_recent: tuple[str, ...] = Field(default=(), max_length=16)


class SlackFileContinuation(BaseModel):
    """Position only in the retained parent inventory, never a mutable provider page."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    parent_id: UUID
    inventory: str = Field(pattern=r"^[a-f0-9]{64}$")
    next_index: int = Field(ge=0, le=100)


@source(
    name="Slack",
    short_name="slack",
    auth_methods=[
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
    ],
    oauth_type=OAuthType.ACCESS_ONLY,
    auth_config_class=SlackAuthConfig,
    config_class=SlackConfig,
    labels=["Communication", "Messaging"],
    supports_continuous=False,
    federated_search=False,
    rate_limit_level=RateLimitLevel.ORG,
)
class SlackSource(BaseSource):
    """Capture all accessible conversation history and replies without search truncation."""

    # Class-level capabilities remain available to the search source registry.
    canonical_record_types = ("channel", "message", "file")
    canonical_container_parents = {"message": "channel", "file": "message"}
    _slack_config: SlackConfig | None = None

    @property
    def slack_config(self) -> SlackConfig | None:
        """The source configuration fixes its capture topology before a cycle starts."""
        return self._slack_config

    @slack_config.setter
    def slack_config(self, config: SlackConfig) -> None:
        self._slack_config = config
        self.canonical_record_types = (
            ("channel", "message", "file") if config.capture_files else ("channel", "message")
        )
        self.canonical_container_parents = (
            {"message": "channel", "file": "message"}
            if config.capture_files
            else {"message": "channel"}
        )

    _verified_principal: SlackPrincipal | None = None

    def _require_principal(self) -> SlackPrincipal:
        """Unbound legacy search cannot bypass canonical capture identity checks."""
        principal = self._verified_principal
        if (
            principal is None
            or self.slack_config is None
            or principal.team_id != self.slack_config.expected_team_id
            or principal.user_id != self.slack_config.expected_user_id
        ):
            raise ValueError("Owned Slack capture requires an attested workspace and user")
        return principal

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Existing cycle ownership includes the trusted visibility principal."""
        principal = self._require_principal()
        material = {"version": 2, "team_id": principal.team_id, "user_id": principal.user_id}
        if self.slack_config.capture_files:
            material["capture_files"] = "message_owned_children_v1"
        fingerprint = hashlib.sha256(
            json.dumps(
                material,
                sort_keys=True,
            ).encode()
        ).hexdigest()
        return CycleConfiguration.from_source(
            fingerprint=fingerprint,
            record_types=self.canonical_record_types,
            container_parents=self.canonical_container_parents,
            exact_parent_validation=("message",) if self.slack_config.capture_files else (),
        )

    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: SlackConfig,
    ) -> SlackSource:
        """Create a new Slack source."""
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance.slack_config = config
        if config.expected_team_id is not None:
            await instance.validate()
        return instance

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout | retry_if_exception_type(AuthProviderRateLimitError),
        wait=partial(wait_rate_limit_with_backoff, max_rate_limit_wait=None),
        reraise=True,
    )
    async def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        """Make authenticated GET request to Slack API with token refresh support."""
        headers = await authorization_headers(self.auth)
        response = await self.http_client.get(url, headers=headers, params=params)

        if response.status_code == 401 and self.auth.supports_refresh:
            headers = await authorization_headers(self.auth, refresh=True)
            response = await self.http_client.get(url, headers=headers, params=params)

        raise_for_status(
            response,
            source_short_name=self.short_name,
            token_provider_kind=self.auth.provider_kind,
        )
        payload = response.json()
        if not payload.get("ok"):
            error = payload.get("error", "unknown_error")
            if error in {"ratelimited", "rate_limited"}:
                raise SourceRateLimitError(
                    source_short_name="slack", retry_after=_parse_retry_after(response, default=60)
                )
            if error in {"invalid_auth", "not_authed", "token_revoked", "account_inactive"}:
                raise SourceAuthError(
                    "Slack authentication is unavailable",
                    source_short_name="slack",
                    status_code=401,
                    token_provider_kind=self.auth.provider_kind,
                )
            # Do not turn denied channels, missing scopes or provider errors into empty pages.
            raise SlackApiError(error)
        return payload

    @staticmethod
    def _capture_message(message: dict, channel_id: str) -> CaptureRecord:
        """Keep native response untouched; opt-in acquisition subsequently adds blob evidence."""
        edited = message.get("edited")
        edited_ts = edited.get("ts") if isinstance(edited, dict) else None
        return CaptureRecord(
            identity=RecordIdentity(
                record_type="message", native_id=message["ts"], container_id=channel_id
            ),
            payload=message,
            observed_at=datetime.now(timezone.utc),
            completeness="partial" if message.get("files") else "complete",
            source_created_at=_message_timestamp(message["ts"]),
            source_updated_at=_message_timestamp(edited_ts),
        )

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Keep the established native container identity for this flat provider."""
        return self._child_scope(parent.identity, record_type)

    def _child_scope(self, parent: RecordIdentity, record_type: str) -> CompletedScope:
        """Enumeration and fresh-page receipts use exactly the same scope identity."""
        if self.canonical_container_parents.get(record_type) != parent.record_type:
            raise ValueError("Unsupported Slack child scope")
        return CompletedScope(
            record_type=record_type,
            container_id=parent_container_key(parent)
            if record_type == "file"
            else parent.native_id,
            parent=parent,
        )

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Fetch one page; record and nested reply progress are committed by the pipeline."""
        self._require_principal()
        try:
            if scope.record_type == "file":
                return await self._file_page(scope, continuation, files, parent)
            page = await self._validated_capture_page(scope, continuation)
            if self.slack_config.capture_files and scope.record_type == "message":
                records = tuple(self._attachment_owner(record) for record in page.records)
                page = page.model_copy(
                    update={
                        "records": records,
                        "child_scope_observations": tuple(
                            ChildScopeObservation(scope=self._child_scope(record.identity, "file"))
                            for record in records
                        ),
                    }
                )
            return page
        except ValidationError:
            # Validation diagnostics may contain private provider values.
            raise ValueError("Slack capture returned invalid page or continuation data") from None

    def _attachment_owner(self, record: CaptureRecord) -> CaptureRecord:
        """Validate the native inventory shared by enumeration and exact owner reads."""
        self._file_inventory(record.payload)
        return record.model_copy(
            update={
                "payload_schema_version": 2,
                "descendant_visibility_fields": ("files",),
                "completeness": "complete",
            }
        )

    async def refresh_known(self, record: SourceRecord, *, files: FileService) -> CaptureRecord:
        """Recheck an attachment owner without treating an ambiguous miss as deletion."""
        self._require_principal()
        if not self.slack_config.capture_files or record.identity.record_type != "message":
            raise ValueError("Slack exact owner refresh requires attachment capture and a message")
        try:
            message = await self._read_known_message(record)
            if message is None:
                raise ValueError("Slack message access remains unconfirmed; capture is incomplete")
            captured = self._capture_message(message, record.identity.container_id)
            return self._attachment_owner(captured).model_copy(update={"parent": record.parent})
        except ValidationError:
            raise ValueError("Slack exact message read returned invalid provider data") from None

    @staticmethod
    def _file_inventory(payload: dict[str, JsonValue]) -> list[dict[str, JsonValue]]:
        metadata = SlackMessageFiles.model_validate(payload).files
        if len({file.id for file in metadata}) != len(metadata):
            raise ValueError("Slack message contains duplicate file identities")
        return payload.get("files", [])

    async def _file_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        files: FileService,
        parent: SourceRecord | None,
    ) -> CapturePage:
        if (
            not self.slack_config.capture_files
            or parent is None
            or parent.identity.record_type != "message"
            or parent.payload_schema_version != 2
            or parent.content_access != "available"
            or parent.deleted_at is not None
            or scope != self.child_scope(parent, "file")
        ):
            raise ValueError("Slack file scope requires its current retained message")
        inventory = self._file_inventory(parent.payload)
        digest = hashlib.sha256(
            json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        state = (
            SlackFileContinuation.model_validate(continuation.value)
            if continuation.value
            else SlackFileContinuation(parent_id=parent.id, inventory=digest, next_index=0)
        )
        if state.parent_id != parent.id or state.inventory != digest:
            raise InvalidScanContinuation("Slack retained file inventory changed")
        if state.next_index > len(inventory):
            raise InvalidScanContinuation("Slack file continuation exceeds retained inventory")
        if state.next_index == len(inventory):
            return CapturePage(records=(), continuation=continuation, final=True)
        native = inventory[state.next_index]
        item = CaptureRecord(
            identity=RecordIdentity(
                record_type="file", native_id=native["id"], container_id=scope.container_id
            ),
            parent=parent.identity,
            payload=native,
            observed_at=datetime.now(timezone.utc),
        )
        captured = await capture_slack_file(self, item, files)
        following = state.model_copy(update={"next_index": state.next_index + 1})
        return CapturePage(
            records=(captured,),
            continuation=ScanContinuation(value=following.model_dump(mode="json")),
            final=following.next_index == len(inventory),
        )

    async def _validated_capture_page(
        self, scope: CompletedScope, continuation: ScanContinuation
    ) -> CapturePage:
        if scope.record_type == "channel" and scope.container_id is None:
            state = SlackRootContinuation.model_validate(continuation.value)
            items, following, recent = await self._capture_page_response(
                "conversations.list",
                "channels",
                {"types": "public_channel,private_channel,im,mpim", "exclude_archived": "false"},
                state.cursor,
                state.recent,
            )
            records = tuple(
                CaptureRecord(
                    identity=RecordIdentity(record_type="channel", native_id=item["id"]),
                    payload=item,
                    observed_at=datetime.now(timezone.utc),
                    source_created_at=_conversation_timestamp(item.get("created")),
                    source_updated_at=_conversation_timestamp(
                        item.get("updated"), milliseconds=True
                    ),
                )
                for item in items
            )
            return CapturePage(
                records=records,
                final=not following,
                continuation=ScanContinuation(
                    value=SlackRootContinuation(cursor=following, recent=recent).model_dump(
                        mode="json"
                    )
                ),
            )
        if scope.record_type != "message" or scope.container_id is None:
            raise ValueError("Slack page capture requires a declared whole conversation scope")
        state = SlackMessageContinuation.model_validate(continuation.value)
        channel_id = scope.container_id
        try:
            if state.pending_threads:
                items, following, recent = await self._capture_page_response(
                    "conversations.replies",
                    "messages",
                    {"channel": channel_id, "ts": state.pending_threads[0]},
                    state.reply_cursor,
                    state.reply_recent,
                )
                pending = state.pending_threads if following else state.pending_threads[1:]
                state = state.model_copy(
                    update={
                        "pending_threads": pending,
                        "reply_cursor": following,
                        "reply_recent": recent if following else (),
                    }
                )
            else:
                if state.history_done:
                    raise ValueError("Completed Slack continuation cannot fetch another page")
                items, following, recent = await self._capture_page_response(
                    "conversations.history",
                    "messages",
                    {"channel": channel_id},
                    state.history_cursor,
                    state.history_recent,
                )
                pending = tuple(
                    dict.fromkeys(item["ts"] for item in items if item.get("reply_count", 0))
                )
                state = SlackMessageContinuation(
                    history_cursor=following,
                    history_recent=recent,
                    history_done=not following,
                    pending_threads=pending,
                )
        except SlackApiError as exc:
            if exc.code in {"channel_not_found", "not_in_channel"}:
                raise ScopeAccessLost("Slack conversation access was lost") from exc
            raise
        return CapturePage(
            records=tuple(self._capture_message(item, channel_id) for item in items),
            continuation=ScanContinuation(value=state.model_dump(mode="json")),
            final=state.history_done and not state.pending_threads,
        )

    async def _capture_page_response(
        self,
        operation: str,
        item_key: str,
        params: dict[str, JsonValue],
        cursor: str,
        recent: tuple[str, ...],
    ) -> tuple[list[dict[str, JsonValue]], str, tuple[str, ...]]:
        """Validate provider pagination without rewriting native record payloads."""
        try:
            payload = await self._get(
                f"https://slack.com/api/{operation}",
                {**params, "limit": 100, **({"cursor": cursor} if cursor else {})},
            )
        except SlackApiError as exc:
            if exc.code == "invalid_cursor":
                raise InvalidScanContinuation("Slack page cursor expired") from exc
            if operation == "conversations.replies" and exc.code == "thread_not_found":
                # History queued a thread that no longer exists. Restart inventory;
                # missing replies alone cannot authorize deletion or scope completion.
                raise InvalidScanContinuation("Slack queued thread is no longer available") from exc
            raise
        if operation == "conversations.history":
            coverage = SlackHistoryCoverage.model_validate(payload)
            if coverage.is_limited:
                raise SourceError(
                    "Slack history is limited by workspace visibility. "
                    "Capture remains incomplete; older retained messages were not reconciled.",
                    source_short_name="slack",
                )
        items = TypeAdapter(list[dict[str, JsonValue]]).validate_python(payload.get(item_key))
        metadata = SlackPageMetadata.model_validate(payload.get("response_metadata") or {})
        following = metadata.next_cursor.strip()
        if not following and payload.get("has_more"):
            raise ValueError("Slack capture has more records without a cursor")
        if following:
            digest = hashlib.sha256(following.encode()).hexdigest()
            if following == cursor or digest in recent:
                raise ValueError("Slack capture repeated a pagination cursor")
            recent = (*recent[-15:], digest)
        return items, following, recent

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Require provider confirmation before hiding an omitted prior conversation."""
        self._require_principal()
        if record.identity.record_type == "message":
            await self._confirm_message_omission(record)
            return
        if record.identity.record_type != "channel" or record.identity.container_id is not None:
            raise ValueError("Slack omission confirmation requires a channel or message")
        native_id = record.identity.native_id
        try:
            await self._get("https://slack.com/api/conversations.info", {"channel": native_id})
        except SlackApiError as exc:
            if exc.code not in {"channel_not_found", "not_in_channel"}:
                raise
            return
        raise ValueError("Slack omitted an accessible prior channel; retry enumeration")

    async def _confirm_message_omission(self, record: SourceRecord) -> None:
        """An exact read can disprove omission; an empty read does not prove deletion."""
        message = await self._read_known_message(record)
        if message is not None:
            raise ValueError("Slack omitted an accessible prior message; retry enumeration")
        raise ValueError("Slack message omission remains unconfirmed; capture is incomplete")

    async def _read_known_message(self, record: SourceRecord) -> dict[str, JsonValue] | None:
        """Use native timestamp bounds and the retained thread route for one exact owner."""
        identity = record.identity
        if (
            identity.container_id is None
            or record.parent
            != RecordIdentity(record_type="channel", native_id=identity.container_id)
            or record.payload.get("ts") != identity.native_id
        ):
            raise ValueError("Slack message omission requires its retained channel identity")
        thread_ts = record.payload.get("thread_ts")
        if thread_ts is not None and (not isinstance(thread_ts, str) or not thread_ts):
            raise ValueError("Slack retained message has invalid thread identity")
        params = {
            "channel": identity.container_id,
            "oldest": identity.native_id,
            "latest": identity.native_id,
            "inclusive": "true",
            "limit": 1,
        }
        operation = "conversations.history"
        if thread_ts and thread_ts != identity.native_id:
            operation = "conversations.replies"
            params["ts"] = thread_ts
        payload = await self._get(f"https://slack.com/api/{operation}", params)
        messages = TypeAdapter(list[dict[str, JsonValue]]).validate_python(payload.get("messages"))
        matches = [message for message in messages if message.get("ts") == identity.native_id]
        if len(matches) > 1:
            raise ValueError("Slack exact message read returned duplicate identities")
        if not matches:
            return None
        message = matches[0]
        if message.get("thread_ts", identity.native_id) != (thread_ts or identity.native_id):
            raise ValueError("Slack exact message read changed its retained thread identity")
        return message

    # ------------------------------------------------------------------
    # Federated search
    # ------------------------------------------------------------------

    async def search(self, query: str, limit: int) -> List[BaseEntity]:
        """Search Slack for messages matching the query with pagination support.

        Uses Slack's search.messages API endpoint with pagination to retrieve
        up to the requested limit. Files are not included since processing file
        content requires the full sync pipeline (download, chunking, vectorization)
        which federated search sources skip.
        """
        self.logger.info(f"Searching Slack messages for query: '{query}' (limit: {limit})")
        results = await self._paginate_search_results(query, limit)
        self.logger.info(f"Slack search complete: returned {len(results)} results")
        return results

    async def _paginate_search_results(self, query: str, limit: int) -> List[BaseEntity]:
        """Paginate through Slack search results."""
        page = 1
        results_fetched = 0
        max_results_per_page = 100
        all_entities: List[BaseEntity] = []

        while results_fetched < limit:
            count = min(max_results_per_page, limit - results_fetched)
            response_data = await self._fetch_search_page(query, count, page)

            if not response_data:
                break

            messages = response_data.get("messages", {})
            message_matches = messages.get("matches", [])
            paging_info = messages.get("paging", {})

            self.logger.debug(
                f"Page {page}: found {len(message_matches)} results "
                f"(total available: {paging_info.get('total', 'unknown')})"
            )

            if not message_matches:
                break

            entities = self._process_message_matches(message_matches, limit, results_fetched)
            all_entities.extend(entities)
            results_fetched += len(entities)

            if page >= paging_info.get("pages", 1):
                break

            page += 1

        return all_entities

    async def _fetch_search_page(
        self, query: str, count: int, page: int
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single page of search results from Slack API."""
        params = {
            "query": query,
            "count": count,
            "page": page,
            "highlight": True,
            "sort": "score",
        }

        response_data = await self._get("https://slack.com/api/search.messages", params=params)

        if not response_data.get("ok"):
            error = response_data.get("error", "unknown_error")
            self.logger.warning(f"Slack search API error: {error}")

            if error == "missing_scope":
                raise ValueError(
                    "Slack search failed: missing 'search:read' scope. "
                    "Please ensure your Slack OAuth connection includes the 'search:read' scope "
                    "to enable message search."
                )
            elif error == "not_authed":
                raise ValueError("Slack search failed: authentication token is invalid or expired")
            elif error == "account_inactive":
                raise ValueError("Slack search failed: account is inactive")
            else:
                raise ValueError(f"Slack search failed: {error}")

        return response_data

    def _process_message_matches(
        self, message_matches: List[Dict], limit: int, results_fetched: int
    ) -> List[BaseEntity]:
        """Process message matches and return entities."""
        entities: List[BaseEntity] = []
        for message in message_matches:
            if results_fetched + len(entities) >= limit:
                break

            try:
                entity = self._create_message_entity(message)
                if entity:
                    entities.append(entity)
            except SourceAuthError:
                raise
            except Exception as e:
                self.logger.warning(f"Error creating message entity: {e}")
                continue

        return entities

    def _create_message_entity(self, message: Dict[str, Any]) -> Optional[SlackMessageEntity]:
        """Create a SlackMessageEntity from a search result."""
        channel_info = message.get("channel", {})
        channel_id = channel_info.get("id", "unknown")
        channel_name = channel_info.get("name")

        breadcrumbs = [
            Breadcrumb(
                entity_id=channel_id,
                name=f"#{channel_name}" if channel_name else channel_id,
                entity_type="SlackChannel",
            )
        ]

        entity = SlackMessageEntity.from_api(message, breadcrumbs=breadcrumbs)

        entity.airweave_system_metadata = AirweaveSystemMetadata(
            source_name="slack",
            entity_type="SlackMessageEntity",
            sync_id=None,
            sync_job_id=None,
        )

        entity.textual_representation = TextualRepresentationBuilder().build_metadata_section(
            entity=entity,
            source_name="slack",
        )

        return entity

    # ------------------------------------------------------------------
    # Sync entry point (not used — Slack is federated search only)
    # ------------------------------------------------------------------

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Not used — Slack is a federated search source.

        Raises NotImplementedError; use search() instead.
        """
        self.logger.warning("generate_entities() called on federated search source")
        raise NotImplementedError(
            "Slack uses federated search. Use the search() method instead of generate_entities()."
        )
        yield  # make this a generator  # noqa: RUF027

    async def validate(self) -> None:
        """Re-attest the exact bound workspace/user, clearing old evidence first."""
        self._verified_principal = None
        raw = await self._get("https://slack.com/api/auth.test")
        try:
            principal = SlackPrincipal.model_validate(raw)
        except ValidationError:
            raise ValueError("Slack returned an invalid native identity") from None
        if self.slack_config is not None and self.slack_config.expected_team_id is not None:
            if (
                principal.team_id != self.slack_config.expected_team_id
                or principal.user_id != self.slack_config.expected_user_id
            ):
                raise ValueError("Slack identity does not match the trusted binding")
            self._verified_principal = principal
