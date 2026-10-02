"""Native Notion originals; incomplete discovery never establishes absence.

Page/database/data-source topology entries describe enumeration scopes, not grants.
These originals are independently retrieved roots. Blocks alone inherit access from
native owners. Properties, comments and file bytes are not complete in this slice.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncGenerator, Sequence
from datetime import datetime, timezone
from functools import partial
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StrictBool
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
from airweave.domains.sources.exceptions import SourceAuthError, SourceError
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.file_service import FileService
from airweave.domains.storage.limits import MAX_FILE_SIZE_BYTES
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.config import NotionConfig
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType

API_VERSION = "2026-03-11"
FIELD_SET_VERSION = 2
_ROOTS = ("page", "database", "data_source")
_ENDPOINTS = {
    "page": "pages",
    "database": "databases",
    "data_source": "data_sources",
    "block": "blocks",
}
_PAGE_SIZE = 25


class _Native(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _Object(_Native):
    id: UUID
    object: Literal["page", "database", "data_source", "block"]
    created_time: AwareDatetime
    last_edited_time: AwareDatetime
    in_trash: StrictBool = False


class _Parent(_Native):
    type: str
    page_id: UUID | None = None
    block_id: UUID | None = None
    database_id: UUID | None = None
    data_source_id: UUID | None = None


class _Located(_Object):
    parent: _Parent


class _Block(_Located):
    type: str
    has_children: StrictBool
    synced_block: dict[str, JsonValue] | None = None


class _Database(_Object):
    data_sources: list[dict[str, JsonValue]] = Field(max_length=1000)


class _Id(_Native):
    id: UUID


class _BotWorkspace(_Native):
    workspace_id: UUID


class _Principal(_Id):
    object: Literal["user"]
    type: Literal["bot"]
    bot: _BotWorkspace


class _Status(_Native):
    type: Literal["complete", "incomplete"]
    incomplete_reason: str | None = None


class _List(_Native):
    object: Literal["list"]
    results: list[dict[str, JsonValue]] = Field(max_length=_PAGE_SIZE)
    has_more: StrictBool
    next_cursor: str | None
    request_status: _Status | None = None


class _Property(_Native):
    # Native IDs already contain percent encoding. Accept one URL path segment,
    # including valid escapes, and never quote it a second time.
    id: str = Field(pattern=r"^(?:[A-Za-z0-9._~-]|%[0-9A-Fa-f]{2})+$")
    type: str


class _PageProperties(_Object):
    properties: dict[str, dict[str, JsonValue]]


class _Calculation(_Native):
    type: str


class _PropertyMetadata(_Property):
    rollup: _Calculation | None = None
    formula: _Calculation | None = None


class _PropertyItem(_PropertyMetadata):
    object: Literal["property_item"]


class _PropertyList(_List):
    type: Literal["property_item"]
    property_item: _PropertyMetadata


class _PropertyArchiveHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")
    format_version: Literal[1] = 1
    page_id: UUID
    property_id: str
    notion_version: Literal["2026-03-11"] = API_VERSION
    page_last_edited_time: AwareDatetime


class _PropertyManifest(_PropertyArchiveHeader):
    name: str
    property: dict[str, JsonValue]
    response_count: int = Field(ge=1)
    value_status: Literal["available", "unsupported"]


class _PropertyProgress(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    index: int = Field(default=0, ge=0)
    inventory: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class _Error(_Native):
    code: str


class _Progress(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    cursor: str | None = Field(default=None, min_length=1, max_length=4096)
    offset: int = Field(default=0, ge=0, le=1000)
    window_start: AwareDatetime | None = None
    last_created: AwareDatetime | None = None
    capped: bool = False
    cursor_hashes: tuple[Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")], ...] = Field(
        default=(), max_length=512
    )


@source(
    name="Notion",
    short_name="notion",
    auth_methods=[
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
    ],
    oauth_type=OAuthType.ACCESS_ONLY,
    auth_config_class=None,
    config_class=NotionConfig,
    labels=["Knowledge Base", "Productivity"],
    supports_continuous=False,
    rate_limit_level=RateLimitLevel.CONNECTION,
)
class NotionSource(BaseSource):
    """One source registration, native acquisition, and existing SQL-owned page progress."""

    canonical_record_types = (*_ROOTS, "block", "page_property")
    # Root alternatives remain independent access authorities. Child alternatives
    # represent native enumeration operations; they emit discovered roots, not edges.
    canonical_container_parents = {
        "page": (None, "data_source"),
        "database": (None, "data_source"),
        "data_source": (None, "database"),
        "block": ("page", "block"),
        "page_property": "page",
    }

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Versioned acquisition contract; changing it requires explicit cycle restart."""
        return self._cycle_configuration

    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: NotionConfig,
    ) -> NotionSource:
        """Build a native managed/direct-auth source without acquiring additional grants."""
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance.config = config
        fingerprint = (
            f"notion:{API_VERSION}:{FIELD_SET_VERSION}:partial-native-roots-blocks-properties:"
            f"{config.expected_workspace_id}:{config.expected_bot_id}"
        )
        instance._cycle_configuration = CycleConfiguration.from_source(
            fingerprint=hashlib.sha256(fingerprint.encode()).hexdigest(),
            record_types=cls.canonical_record_types,
            container_parents=cls.canonical_container_parents,
            completion_policies={kind: "discovery_with_validation" for kind in _ROOTS},
        )
        return instance

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout | retry_if_exception_type(AuthProviderRateLimitError),
        wait=partial(wait_rate_limit_with_backoff, max_rate_limit_wait=None),
        reraise=True,
    )
    async def _request(
        self,
        path: str,
        body: dict[str, JsonValue] | None = None,
        *,
        params: dict[str, str | int] | None = None,
    ) -> dict[str, JsonValue] | None:
        headers = {**await authorization_headers(self.auth), "Notion-Version": API_VERSION}
        method = self.http_client.get if body is None else self.http_client.post
        kwargs = {"params": params} if body is None else {"json": body}
        response = await method(f"https://api.notion.com/v1/{path}", headers=headers, **kwargs)
        if response.status_code == 401 and self.auth.supports_refresh:
            headers = {
                **await authorization_headers(self.auth, refresh=True),
                "Notion-Version": API_VERSION,
            }
            response = await method(f"https://api.notion.com/v1/{path}", headers=headers, **kwargs)
        if (
            response.status_code == 404
            and _Error.model_validate(response.json()).code == "object_not_found"
        ):
            return None
        raise_for_status(
            response, source_short_name="notion", token_provider_kind=self.auth.provider_kind
        )
        result = response.json()
        if not isinstance(result, dict):
            raise ValueError("Notion returned a non-object response")
        return result

    async def validate(self) -> None:
        """Verify identity independently of content discovery capabilities."""
        payload = await self._request("users/me")
        principal = _Principal.model_validate(payload)
        if (principal.bot.workspace_id, principal.id) != (
            self.config.expected_workspace_id,
            self.config.expected_bot_id,
        ):
            raise SourceAuthError(
                "Notion workspace or connection bot does not match the configured account",
                source_short_name="notion",
                status_code=200,
                token_provider_kind=self.auth.provider_kind,
            )

    async def _retrieve(self, kind: str, native_id: str) -> dict[str, JsonValue] | None:
        native_id = str(UUID(native_id))
        payload = await self._request(f"{_ENDPOINTS[kind]}/{native_id}")
        if payload is not None:
            parsed = _Object.model_validate(payload)
            if parsed.object != kind or str(parsed.id) != native_id:
                raise ValueError("Notion returned the wrong exact object identity")
        return payload

    @staticmethod
    def _record(
        kind: str, payload: dict[str, JsonValue], *, parent: RecordIdentity | None = None
    ) -> CaptureRecord:
        parsed = _Object.model_validate(payload)
        if parsed.object != kind:
            raise ValueError("Notion returned an unexpected object kind")
        return CaptureRecord(
            identity=RecordIdentity(record_type=kind, native_id=str(parsed.id)),
            parent=parent,
            allow_reparent=parent is not None,
            payload=payload,
            payload_schema_version=FIELD_SET_VERSION,
            completeness="partial",
            source_created_at=parsed.created_time,
            source_updated_at=parsed.last_edited_time,
            observed_at=datetime.now(timezone.utc),
            kind="delete" if parsed.in_trash else "upsert",
            removal_reason="provider_deleted" if parsed.in_trash else None,
        )

    @staticmethod
    def _unavailable(record: SourceRecord) -> CaptureRecord:
        return CaptureRecord(
            identity=record.identity,
            parent=record.parent,
            payload={},
            kind="delete",
            removal_reason="scope_removed",
            completeness="partial",
            observed_at=datetime.now(timezone.utc),
        )

    async def refresh_known(self, record: SourceRecord, *, files: FileService) -> CaptureRecord:
        """Search omission is repaired by an exact native read, never inferred deletion."""
        if record.identity.record_type not in _ROOTS or record.parent is not None:
            raise ValueError("Notion known-object refresh requires an independent root")
        payload = await self._retrieve(record.identity.record_type, record.identity.native_id)
        return (
            self._unavailable(record)
            if payload is None
            else self._record(record.identity.record_type, payload)
        )

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Only native block-child listings establish their exact-owner absence."""
        if record.identity.record_type != "block" or record.parent is None:
            raise ValueError("Notion discovery requires exact known-object refresh")
        payload = await self._retrieve("block", record.identity.native_id)
        if payload is None:
            return
        block = _Block.model_validate(payload)
        if block.in_trash or self._block_parent(block) != record.parent:
            return
        raise ValueError("Notion omitted a block still readable under its captured owner")

    @staticmethod
    def _block_parent(block: _Block) -> RecordIdentity:
        if block.parent.type == "page_id" and block.parent.page_id is not None:
            return RecordIdentity(record_type="page", native_id=str(block.parent.page_id))
        if block.parent.type == "block_id" and block.parent.block_id is not None:
            return RecordIdentity(record_type="block", native_id=str(block.parent.block_id))
        raise ValueError("Notion block has an unsupported or missing native owner")

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Notion UUIDs are global; do not rewrite native object identity when it moves."""
        if record_type not in self.capture_cycle_configuration.children_of(
            parent.identity.record_type
        ):
            raise ValueError("Unsupported Notion enumeration scope")
        return CompletedScope(
            record_type=record_type,
            parent=parent.identity,
            container_id=parent.identity.native_id if record_type == "page_property" else None,
        )

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Return bounded observations; SQL commits them together with this continuation."""
        if scope.record_type == "page_property":
            if (
                parent is None
                or parent.identity != scope.parent
                or scope != self.child_scope(parent, "page_property")
            ):
                raise ValueError("Notion property scope has a different native page owner")
            return await self._property_page(
                parent, _PropertyProgress.model_validate(continuation.value), files
            )
        progress = _Progress.model_validate(continuation.value)
        if scope.container_id is not None:
            raise ValueError("Notion native identities do not have a synthetic container")
        if scope.parent is None:
            if parent is not None or scope.record_type not in _ROOTS:
                raise ValueError("Invalid Notion discovery scope")
            return await self._search(scope.record_type, progress)
        if parent is None or parent.identity != scope.parent:
            raise ValueError("Notion enumeration owner is missing or mismatched")
        self.child_scope(parent, scope.record_type)
        if scope.record_type == "block":
            return await self._blocks(parent, progress)
        if parent.identity.record_type == "database":
            return await self._database_sources(parent, progress)
        if scope.record_type == "database":
            located = _Located.model_validate(parent.payload)
            native = located.parent.database_id
            roots = await self._roots("database", [str(native)] if native else [])
            return self._page((), progress, True, roots)
        return await self._query_rows(parent, progress)

    @staticmethod
    def _page(
        records: Sequence[CaptureRecord],
        progress: _Progress | _PropertyProgress,
        final: bool,
        discovered: Sequence[CaptureRecord] = (),
    ) -> CapturePage:
        return CapturePage(
            records=tuple(records),
            discovered_records=tuple(discovered),
            continuation=ScanContinuation(value=progress.model_dump(mode="json")),
            final=final,
        )

    @staticmethod
    def _following(page: _List, progress: _Progress) -> tuple[str | None, tuple[str, ...]]:
        if page.has_more:
            if not page.next_cursor or page.next_cursor == progress.cursor:
                raise ValueError("Notion returned a missing or repeated cursor")
            digest = hashlib.sha256(page.next_cursor.encode()).hexdigest()
            if digest in progress.cursor_hashes:
                raise ValueError("Notion pagination entered a cursor cycle")
            # Detect recent cycles without imposing a total page-count ceiling.
            # Longer-period loops remain subject to the existing runtime limits.
            return page.next_cursor, (*progress.cursor_hashes[-511:], digest)
        if page.next_cursor is not None:
            raise ValueError("Notion returned an inconsistent terminal cursor")
        return None, progress.cursor_hashes

    async def _roots(self, kind: str, identities: list[str]) -> list[CaptureRecord]:
        result = []
        for native in dict.fromkeys(identities):
            payload = await self._retrieve(kind, native)
            if payload is not None:
                record = self._record(kind, payload)
                # Side admissions cannot claim deletion of a different root inventory.
                if record.kind == "upsert":
                    result.append(record)
        return result

    async def _search(self, kind: str, progress: _Progress) -> CapturePage:
        if kind == "database":
            return self._page((), progress, True)
        body: dict[str, JsonValue] = {
            "page_size": _PAGE_SIZE,
            "filter": {"property": "object", "value": kind},
        }
        if progress.cursor:
            body["start_cursor"] = progress.cursor
        page = _List.model_validate(await self._request("search", body))
        if page.request_status and page.request_status.type == "incomplete":
            raise SourceError(
                "Notion search was capped; discovery has not completed", source_short_name="notion"
            )
        records = []
        for item in page.results:
            parsed = _Object.model_validate(item)
            if parsed.object != kind:
                raise ValueError("Notion search returned a different filtered object type")
            payload = await self._retrieve(kind, str(parsed.id))
            if payload is not None:
                records.append(self._record(kind, payload))
        cursor, cursor_hashes = self._following(page, progress)
        return self._page(
            records,
            progress.model_copy(update={"cursor": cursor, "cursor_hashes": cursor_hashes}),
            cursor is None,
        )

    async def _database_sources(self, parent: SourceRecord, progress: _Progress) -> CapturePage:
        # Revalidate exact current database, not a stale embedded data-source inventory.
        payload = await self._retrieve("database", parent.identity.native_id)
        if payload is None:
            raise ScopeAccessLost("Notion database is unavailable", removal_reason="scope_removed")
        database = _Database.model_validate(payload)
        if database.in_trash:
            raise ScopeAccessLost("Notion database is in trash", removal_reason="scope_removed")
        identities = database.data_sources
        ids = [str(_Id.model_validate(item).id) for item in identities]
        # One bounded response is re-read on resume. A changed list must restart rather
        # than skip objects by an offset into a different inventory.
        digest = hashlib.sha256("\n".join(ids).encode()).hexdigest()
        if progress.cursor and progress.cursor != digest:
            raise InvalidScanContinuation("Notion database inventory changed; restart this scope")
        roots = await self._roots(
            "data_source", ids[progress.offset : progress.offset + _PAGE_SIZE]
        )
        offset = progress.offset + _PAGE_SIZE
        return self._page(
            (),
            progress.model_copy(update={"offset": min(offset, len(ids)), "cursor": digest}),
            offset >= len(ids),
            roots,
        )

    async def _query_rows(self, parent: SourceRecord, progress: _Progress) -> CapturePage:
        body: dict[str, JsonValue] = {
            "page_size": _PAGE_SIZE,
            "sorts": [{"timestamp": "created_time", "direction": "ascending"}],
        }
        if progress.cursor:
            body["start_cursor"] = progress.cursor
        if progress.window_start:
            body["filter"] = {
                "timestamp": "created_time",
                "created_time": {"on_or_after": progress.window_start.isoformat()},
            }
        payload = await self._request(f"data_sources/{parent.identity.native_id}/query", body)
        if payload is None:
            raise ScopeAccessLost(
                "Notion data source is unavailable", removal_reason="scope_removed"
            )
        page = _List.model_validate(payload)
        roots = []
        last = progress.last_created
        for item in page.results:
            parsed = _Object.model_validate(item)
            if parsed.object not in {"page", "data_source"}:
                raise ValueError("Notion data source returned an unexpected row type")
            if last is not None and parsed.created_time < last:
                raise ValueError("Notion data source violated created-time ordering")
            last = parsed.created_time
            roots.extend(await self._roots(parsed.object, [str(parsed.id)]))
        cursor, cursor_hashes = self._following(page, progress)
        if (
            page.request_status
            and page.request_status.type == "incomplete"
            and page.request_status.incomplete_reason != "query_result_limit_reached"
        ):
            raise SourceError(
                "Notion query was incomplete for an unsupported reason", source_short_name="notion"
            )
        capped = progress.capped or bool(
            page.request_status and page.request_status.type == "incomplete"
        )
        next_progress = progress.model_copy(
            update={
                "cursor": cursor,
                "cursor_hashes": cursor_hashes,
                "last_created": last,
                "capped": capped,
            }
        )
        if cursor is None and capped:
            if last is None or (
                progress.window_start is not None and last <= progress.window_start
            ):
                raise SourceError(
                    "Notion query limit cannot be split by creation time; capture is incomplete",
                    source_short_name="notion",
                )
            next_progress = _Progress(window_start=last)
            return self._page((), next_progress, False, roots)
        return self._page((), next_progress, cursor is None, roots)

    async def _blocks(self, parent: SourceRecord, progress: _Progress) -> CapturePage:
        if parent.identity.record_type == "block":
            block = _Block.model_validate(parent.payload)
            if (
                not block.has_children
                or block.type in {"child_page", "child_database"}
                or (block.synced_block and block.synced_block.get("synced_from") is not None)
            ):
                return self._page((), progress, True)
        params: dict[str, str | int] = {"page_size": _PAGE_SIZE}
        if progress.cursor:
            params["start_cursor"] = progress.cursor
        payload = await self._request(f"blocks/{parent.identity.native_id}/children", params=params)
        if payload is None:
            raise ScopeAccessLost(
                "Notion block owner is unavailable", removal_reason="scope_removed"
            )
        page = _List.model_validate(payload)
        if page.request_status and page.request_status.type == "incomplete":
            raise ValueError("Notion block enumeration was incomplete")
        records, roots = [], []
        for item in page.results:
            block = _Block.model_validate(item)
            if self._block_parent(block) != parent.identity:
                raise ValueError("Notion block has a different native owner")
            records.append(self._record("block", item, parent=parent.identity))
            target = {"child_page": "page", "child_database": "database"}.get(block.type)
            if target and not block.in_trash:
                roots.extend(await self._roots(target, [str(block.id)]))
        cursor, cursor_hashes = self._following(page, progress)
        return self._page(
            records,
            progress.model_copy(update={"cursor": cursor, "cursor_hashes": cursor_hashes}),
            cursor is None,
            roots,
        )

    async def _property_inventory(
        self,
        page_id: str,
    ) -> tuple[str, list[tuple[str, dict[str, JsonValue]]], datetime]:
        payload = await self._retrieve("page", page_id)
        if payload is None or _Object.model_validate(payload).in_trash:
            raise ScopeAccessLost(
                "Notion property page is unavailable", removal_reason="scope_removed"
            )
        page = _PageProperties.model_validate(payload)
        rows = sorted(
            page.properties.items(), key=lambda item: _Property.model_validate(item[1]).id
        )
        identities = [
            (_Property.model_validate(value).id, name, _Property.model_validate(value).type)
            for name, value in rows
        ]
        if len({item[0] for item in identities}) != len(identities):
            raise ValueError("Notion page contains duplicate property IDs")
        material = json.dumps(
            [page.last_edited_time.isoformat(), identities], ensure_ascii=False
        ).encode()
        return hashlib.sha256(material).hexdigest(), rows, page.last_edited_time

    @staticmethod
    def _property_response(
        payload: dict[str, JsonValue], prop: _Property, progress: _Progress
    ) -> tuple[_Progress, bool, Literal["available", "unsupported"]]:
        if payload.get("object") == "property_item":
            if progress.cursor is not None:
                raise InvalidScanContinuation(
                    "Notion property changed response shape while reading"
                )
            item = _PropertyItem.model_validate(payload)
            metadata = item
            final = True
            following = progress
        else:
            page = _PropertyList.model_validate(payload)
            if page.request_status and page.request_status.type == "incomplete":
                raise SourceError(
                    "Notion property response is incomplete", source_short_name="notion"
                )
            metadata = page.property_item
            for value in page.results:
                item = _PropertyItem.model_validate(value)
                # Rollup show_original may return the target property's items.
                if prop.type != "rollup" and (item.id != prop.id or item.type != prop.type):
                    raise ValueError("Notion property list contains another property's values")
            cursor, hashes = NotionSource._following(page, progress)
            following = progress.model_copy(update={"cursor": cursor, "cursor_hashes": hashes})
            final = cursor is None
        if metadata.id != prop.id or metadata.type != prop.type:
            raise ValueError("Notion returned a different property identity or type")
        calculation = metadata.rollup if prop.type == "rollup" else metadata.formula
        if prop.type in {"rollup", "formula"} and calculation is None:
            raise ValueError("Notion calculation has no native result metadata")
        if final and calculation and calculation.type == "incomplete":
            raise SourceError(
                "Notion property calculation did not complete", source_short_name="notion"
            )
        status = "unsupported" if calculation and calculation.type == "unsupported" else "available"
        return following, final, status

    async def _collect_property(
        self, page_id: str, prop: _Property, fingerprint: str, page_last_edited_time: datetime
    ) -> tuple[bytes, int, Literal["available", "unsupported"]]:
        """Retain native pages once each; retry starts this property without partial publication."""
        header = (
            _PropertyArchiveHeader(
                page_id=UUID(page_id),
                property_id=prop.id,
                page_last_edited_time=page_last_edited_time,
            )
            .model_dump_json()
            .encode()
        )
        prefix = header[:-1] + b',"responses":['
        responses: list[bytes] = []
        total = len(prefix) + len(b"]}")
        following = _Progress()
        while True:
            params: dict[str, str | int] = {"page_size": _PAGE_SIZE}
            if following.cursor:
                params["start_cursor"] = following.cursor
            payload = await self._request(f"pages/{page_id}/properties/{prop.id}", params=params)
            if payload is None:
                # The page may have been removed, or its schema changed. Do not
                # withdraw the whole page based solely on a property endpoint404.
                current, _, _ = await self._property_inventory(page_id)
                if current != fingerprint:
                    raise InvalidScanContinuation(
                        "Notion property inventory changed during retrieval"
                    )
                raise SourceError(
                    "Notion property is unavailable while its page remains readable",
                    source_short_name="notion",
                )
            following, final, status = self._property_response(payload, prop, following)
            encoded = json.dumps(
                payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
            ).encode()
            total += len(encoded) + bool(responses)
            if total > MAX_FILE_SIZE_BYTES:
                raise SourceError(
                    "Notion property exceeds the retained-file size limit; capture is incomplete",
                    source_short_name="notion",
                )
            responses.append(encoded)
            if final:
                break
        return prefix + b",".join(responses) + b"]}", len(responses), status

    async def _property_page(
        self, parent: SourceRecord, progress: _PropertyProgress, files: FileService
    ) -> CapturePage:
        page_id = parent.identity.native_id
        fingerprint, properties, page_last_edited_time = await self._property_inventory(page_id)
        if progress.inventory is not None and progress.inventory != fingerprint:
            raise InvalidScanContinuation(
                "Notion page properties changed; restart their enumeration"
            )
        if progress.index > len(properties):
            raise ValueError("Notion property continuation is outside its inventory")
        if progress.index == len(properties):
            return self._page((), progress.model_copy(update={"inventory": fingerprint}), True)
        name, native_property = properties[progress.index]
        prop = _Property.model_validate(native_property)
        archive, count, status = await self._collect_property(
            page_id, prop, fingerprint, page_last_edited_time
        )
        current, _, _ = await self._property_inventory(page_id)
        if current != fingerprint:
            raise InvalidScanContinuation("Notion page changed before property capture completed")
        blob = await files.store_canonical_blob(archive, media_type="application/json")
        record = CaptureRecord(
            identity=RecordIdentity(
                record_type="page_property", native_id=prop.id, container_id=page_id
            ),
            parent=parent.identity,
            payload=_PropertyManifest(
                page_id=UUID(page_id),
                property_id=prop.id,
                name=name,
                property=native_property,
                response_count=count,
                value_status=status,
                page_last_edited_time=page_last_edited_time,
            ).model_dump(mode="json"),
            payload_schema_version=FIELD_SET_VERSION,
            completeness="partial",
            blobs=(blob,),
            observed_at=datetime.now(timezone.utc),
        )
        next_progress = _PropertyProgress(index=progress.index + 1, inventory=fingerprint)
        return self._page((record,), next_progress, next_progress.index == len(properties))

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """The legacy formatter/crawler is replaced, not maintained as a second authority."""
        raise NotImplementedError("Notion requires canonical page capture")
        yield  # pragma: no cover
