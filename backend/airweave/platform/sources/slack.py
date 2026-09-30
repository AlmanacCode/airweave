"""Slack capture of accessible conversations, with optional live search."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from functools import partial
from typing import Any, AsyncGenerator, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError
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
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceAuthError, SourceRateLimitError
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
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType

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


class SlackApiError(ValueError):
    """A Slack application error; unlike empty pages it cannot complete a scope."""

    def __init__(self, code: str):
        """Retain the safe provider code for explicit access-loss handling."""
        self.code = code
        super().__init__(f"Slack request failed: {code}")


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

    canonical_record_types = ("channel", "message")
    canonical_container_parents = {"message": "channel"}
    capture_cycle_configuration = CycleConfiguration.from_source(
        fingerprint=hashlib.sha256(b"slack:whole-conversations:history-and-replies:v1").hexdigest(),
        record_types=canonical_record_types,
        container_parents=canonical_container_parents,
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
        return cls(auth=auth, logger=logger, http_client=http_client)

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
        """Keep the complete native response; file bytes are explicitly not captured yet."""
        return CaptureRecord(
            identity=RecordIdentity(
                record_type="message", native_id=message["ts"], container_id=channel_id
            ),
            payload=message,
            observed_at=datetime.now(timezone.utc),
            completeness="partial" if message.get("files") else "complete",
        )

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Keep the established native container identity for this flat provider."""
        return CompletedScope(
            record_type=record_type, container_id=parent.identity.native_id, parent=parent.identity
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
        try:
            return await self._validated_capture_page(scope, continuation)
        except ValidationError:
            # Validation diagnostics may contain private provider values.
            raise ValueError("Slack capture returned invalid page or continuation data") from None

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
        native_id = record.identity.native_id
        try:
            await self._get("https://slack.com/api/conversations.info", {"channel": native_id})
        except SlackApiError as exc:
            if exc.code not in {"channel_not_found", "not_in_channel"}:
                raise
            return
        raise ValueError("Slack omitted an accessible prior channel; retry enumeration")

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
        """Validate credentials by calling Slack auth.test."""
        await self._get("https://slack.com/api/auth.test")
