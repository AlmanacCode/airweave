"""Opt-in mailbox-owned full Outlook capture; no legacy cursor or activation changes."""

import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit

from pydantic import AwareDatetime, TypeAdapter, ValidationError

from airweave.domains.entities.canonical.cycle_models import CaptureCycle, CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import CapturePage, CapturePlan
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.configs.config import OutlookMailConfig
from airweave.platform.sources.outlook_graph import OutlookBoundaryError, OutlookGraphClient
from airweave.platform.sources.outlook_mail_models import (
    OutlookMailContinuation,
    OutlookMessage,
    OutlookMessageID,
    OutlookMessagePage,
)

BASE = "https://graph.microsoft.com/v1.0/me"


class OutlookMailCapture:
    """One root scan uses shared page commits, blobs and exact omission validation.

    MIME-backed schema1 originals are partial until native attachment inventory is
    qualified. Incremental folder deltas remain a required, unimplemented follow-up.
    """

    canonical_record_types = ("message",)
    canonical_container_parents: dict[str, str | tuple[str | None, ...]] = {}

    def __init__(
        self,
        graph: OutlookGraphClient,
        config: OutlookMailConfig,
        included_ids: frozenset[str],
        excluded_ids: frozenset[str],
    ) -> None:
        """Construct through create so the native principal and folder aliases are resolved."""
        self.graph = graph
        self.config = config.model_copy(deep=True)
        self.included_ids = included_ids
        self.excluded_ids = excluded_ids
        self.after = (
            datetime.strptime(config.after_date, "%Y/%m/%d").replace(tzinfo=timezone.utc)
            if config.after_date
            else None
        )
        self.fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "version": 1,
                    "id_type": "ImmutableId",
                    "config": self.config.model_dump(mode="json"),
                    "included_ids": sorted(included_ids),
                    "excluded_ids": sorted(excluded_ids),
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()

    @classmethod
    async def create(
        cls, *, graph: OutlookGraphClient, config: OutlookMailConfig
    ) -> "OutlookMailCapture":
        """Require a trusted identity and resolve native folder aliases, never display names."""
        if not config.expected_principal_id or (
            graph.expected_principal_id != config.expected_principal_id
        ):
            raise OutlookBoundaryError(
                "Canonical Outlook requires a bound principal", source_short_name="outlook_mail"
            )
        await graph.verify_principal()
        folders = {}
        for alias in dict.fromkeys((*config.included_folders, *config.excluded_folders)):
            raw = await graph.get(
                f"{BASE}/mailFolders/{quote(alias, safe='')}", params={"$select": "id"}
            )
            try:
                folders[alias] = OutlookMessageID.model_validate(raw).id
            except ValidationError:
                raise OutlookBoundaryError(
                    "Invalid Outlook folder identity", source_short_name="outlook_mail"
                ) from None
        return cls(
            graph,
            config,
            frozenset(folders[x] for x in config.included_folders),
            frozenset(folders[x] for x in config.excluded_folders),
        )

    def _require_principal(self) -> None:
        if not self.config.expected_principal_id or (
            self.graph.verified_principal_id != self.config.expected_principal_id
            or self.graph.expected_principal_id != self.config.expected_principal_id
        ):
            raise OutlookBoundaryError(
                "Canonical Outlook principal is not attested", source_short_name="outlook_mail"
            )

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Observed enumeration is not a snapshot; omissions require exact validation."""
        self._require_principal()
        return CycleConfiguration.from_source(
            fingerprint=self.fingerprint,
            record_types=self.canonical_record_types,
            container_parents={},
            completion_policies={"message": "discovery_with_validation"},
        )

    async def prepare_cycle(self, previous: CaptureCycle | None) -> CapturePlan:
        """Every cycle is explicitly full; no delta completion is implied."""
        await self.graph.verify_principal()
        self._require_principal()
        return CapturePlan()

    def initial_continuation(self, cycle: CaptureCycle) -> ScanContinuation:
        """Bind a fresh scan to its persisted principal, selection and ID format."""
        self._require_principal()
        if cycle.configuration != self.capture_cycle_configuration:
            raise OutlookBoundaryError(
                "Outlook capture configuration changed", source_short_name="outlook_mail"
            )
        return self._continuation(OutlookMailContinuation(fingerprint=self.fingerprint))

    @staticmethod
    def _continuation(state: OutlookMailContinuation) -> ScanContinuation:
        return ScanContinuation(value=state.model_dump(mode="json"))

    def _state(self, continuation: ScanContinuation) -> OutlookMailContinuation:
        try:
            state = OutlookMailContinuation.model_validate(continuation.value)
        except ValidationError:
            raise OutlookBoundaryError(
                "Invalid Outlook capture continuation", source_short_name="outlook_mail"
            ) from None
        if state.fingerprint != self.fingerprint:
            raise OutlookBoundaryError(
                "Outlook continuation belongs to another capture scope",
                source_short_name="outlook_mail",
            )
        return state

    def _list_url(self, url: str) -> None:
        self.graph._validate_url(url)
        if urlsplit(url).path != "/v1.0/me/messages":
            raise OutlookBoundaryError(
                "Outlook continuation changed the message collection",
                source_short_name="outlook_mail",
            )

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Capture one complete message; durable pending IDs avoid refetching inventory."""
        self._require_principal()
        if scope != CompletedScope(record_type="message") or parent is not None:
            raise OutlookBoundaryError(
                "Outlook requires the mailbox message root", source_short_name="outlook_mail"
            )
        state = self._state(continuation)
        if not state.pending_ids:
            if state.started and state.next_link is None:
                return CapturePage(records=(), continuation=continuation, final=True)
            url = state.next_link or f"{BASE}/messages"
            self._list_url(url)
            raw = await self.graph.get(
                url,
                params=None if state.started else {"$select": "id", "$top": 25},
                immutable_ids=True,
            )
            try:
                page = OutlookMessagePage.model_validate(raw)
            except ValidationError:
                raise OutlookBoundaryError(
                    "Invalid Outlook message inventory", source_short_name="outlook_mail"
                ) from None
            if page.next_link:
                self._list_url(page.next_link)
                if page.next_link == url:
                    raise OutlookBoundaryError(
                        "Outlook continuation did not advance", source_short_name="outlook_mail"
                    )
            state = OutlookMailContinuation(
                fingerprint=self.fingerprint,
                started=True,
                pending_ids=tuple(dict.fromkeys(item.id for item in page.value)),
                next_link=page.next_link,
            )
            self._continuation(state)  # Validate the byte bound before hydrating any originals.
        records = ()
        if state.pending_ids:
            observation = await self.message(state.pending_ids[0], files=files)
            # New out-of-selection objects are not stored as synthetic tombstones.
            # Existing objects are checked by the shared exact omission phase.
            records = (observation,) if observation.kind == "upsert" else ()
            state = state.model_copy(update={"pending_ids": state.pending_ids[1:]})
        return CapturePage(
            records=records,
            continuation=self._continuation(state),
            final=not state.pending_ids and state.next_link is None,
        )

    @staticmethod
    def _date(raw: str | None) -> datetime | None:
        if raw is None:
            return None
        try:
            return TypeAdapter(AwareDatetime).validate_python(raw)
        except ValidationError:
            raise OutlookBoundaryError(
                "Invalid Outlook message timestamp", source_short_name="outlook_mail"
            ) from None

    async def message(self, native_id: str, *, files: FileService) -> CaptureRecord:
        """Exact current mailbox state decides membership; ambiguous404 never means deletion."""
        self._require_principal()
        url = f"{BASE}/messages/{quote(native_id, safe='')}"
        raw = await self.graph.get(url, immutable_ids=True)
        try:
            message = OutlookMessage.model_validate(raw)
        except ValidationError:
            raise OutlookBoundaryError(
                "Invalid Outlook message metadata", source_short_name="outlook_mail"
            ) from None
        if message.id != native_id:
            raise OutlookBoundaryError(
                "Outlook message identity mismatch", source_short_name="outlook_mail"
            )
        identity = RecordIdentity(record_type="message", native_id=native_id)
        outside = message.parentFolderId in self.excluded_ids or (
            bool(self.included_ids) and message.parentFolderId not in self.included_ids
        )
        received = self._date(message.receivedDateTime)
        if self.after is not None and not outside:
            if received is None:
                raise OutlookBoundaryError(
                    "Outlook date selection requires a received timestamp",
                    source_short_name="outlook_mail",
                )
            if received < self.after:
                outside = True
        if outside:
            return CaptureRecord(
                identity=identity,
                payload={"id": native_id},
                kind="delete",
                removal_reason="scope_removed",
                observed_at=datetime.now(timezone.utc),
            )
        blobs = ()
        try:
            content = await self.graph.mime_bytes(
                f"{url}/$value", max_bytes=files.MAX_FILE_SIZE_BYTES
            )
            blob = await files.store_canonical_blob(content, media_type="message/rfc822")
            blobs = (blob,)
        except FileSkippedException:
            pass  # Explicit metadata-only original; transport/storage failures are not skips.
        current = await self.graph.get(
            url, params={"$select": "id,changeKey,parentFolderId"}, immutable_ids=True
        )
        if any(current.get(key) != raw.get(key) for key in ("id", "changeKey", "parentFolderId")):
            raise OutlookBoundaryError(
                "Outlook message changed during original capture", source_short_name="outlook_mail"
            )
        return CaptureRecord(
            identity=identity,
            payload=raw,
            payload_schema_version=1,
            observed_at=datetime.now(timezone.utc),
            blobs=blobs,
            completeness="partial" if blobs else "metadata_only",
            source_created_at=self._date(message.createdDateTime),
            source_updated_at=self._date(message.lastModifiedDateTime),
        )

    async def refresh_known(self, record: SourceRecord, *, files: FileService) -> CaptureRecord:
        """Exact omitted-root reads must succeed before completion."""
        if (
            record.identity.record_type != "message"
            or record.identity.container_id is not None
            or record.parent is not None
        ):
            raise OutlookBoundaryError(
                "Outlook omission identity is outside mailbox scope",
                source_short_name="outlook_mail",
            )
        return await self.message(record.identity.native_id, files=files)

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Messages own their MIME originals; there are no independently scanned children."""
        raise OutlookBoundaryError(
            "Outlook message capture has no child scopes", source_short_name="outlook_mail"
        )

    async def confirm_absent(self, record: SourceRecord) -> None:
        """The engine must use exact omission observations, never an absence assertion."""
        raise OutlookBoundaryError(
            "Outlook requires exact omission validation", source_short_name="outlook_mail"
        )
