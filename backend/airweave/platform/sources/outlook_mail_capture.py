"""Mailbox-owned Outlook originals with bounded per-folder delta progress."""

import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlsplit

from pydantic import AwareDatetime, TypeAdapter, ValidationError

from airweave.domains.entities.canonical.cycle_models import (
    CaptureCycle,
    CycleConfiguration,
    ProviderCheckpoint,
)
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    CapturePlan,
    InvalidCaptureCheckpoint,
)
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceGoneError
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.configs.config import OutlookMailConfig
from airweave.platform.sources.outlook_graph import (
    OutlookBoundaryError,
    OutlookDeltaExpiredError,
    OutlookGraphClient,
)
from airweave.platform.sources.outlook_mail_models import (
    OutlookDeltaPage,
    OutlookFolder,
    OutlookFolderPage,
    OutlookMailCheckpoint,
    OutlookMailContinuation,
    OutlookMessage,
    OutlookMessageID,
)

BASE = "https://graph.microsoft.com/v1.0/me"


class OutlookMailCapture:
    """One root scan uses shared page commits, blobs and exact omission validation.

    MIME-backed schema1 originals are partial until native attachment inventory is
    qualified. Folder removals are signals, never proof of mailbox deletion.
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
                    "version": 2,
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
        """Only a compatible promoted mailbox checkpoint authorizes changes mode."""
        await self.graph.verify_principal()
        self._require_principal()
        if (
            previous is None
            or previous.configuration != self.capture_cycle_configuration
            or previous.promoted_checkpoint is None
        ):
            # An explicit empty starting map opts into terminal checkpoint publication.
            # It is not a completed native baseline and full mode never reuses its tokens.
            return CapturePlan(
                starting_checkpoint=ProviderCheckpoint(
                    value=OutlookMailCheckpoint(fingerprint=self.fingerprint).model_dump(
                        mode="json"
                    )
                )
            )
        checkpoint = previous.promoted_checkpoint.checkpoint
        self._checkpoint(checkpoint)
        return CapturePlan(mode="changes", starting_checkpoint=checkpoint)

    def initial_continuation(self, cycle: CaptureCycle) -> ScanContinuation:
        """Bind progress to the persisted principal, selection and immutable ID format."""
        self._require_principal()
        if cycle.configuration != self.capture_cycle_configuration:
            raise self._error("Outlook capture configuration changed")
        links = {}
        if cycle.mode == "changes":
            if cycle.starting_checkpoint is None:
                raise self._error("Outlook changes require a complete mailbox checkpoint")
            links = self._checkpoint(cycle.starting_checkpoint).folder_links
        elif cycle.mode != "full":
            raise self._error("Unsupported Outlook capture mode")
        return self._continuation(
            OutlookMailContinuation(
                fingerprint=self.fingerprint, mode=cycle.mode, folder_links=links
            )
        )

    @staticmethod
    def _error(message: str) -> OutlookBoundaryError:
        return OutlookBoundaryError(message, source_short_name="outlook_mail")

    def _checkpoint(self, checkpoint: ProviderCheckpoint) -> OutlookMailCheckpoint:
        try:
            state = OutlookMailCheckpoint.model_validate(checkpoint.value)
        except ValidationError:
            raise self._error("Invalid Outlook mailbox checkpoint") from None
        if state.fingerprint != self.fingerprint:
            raise self._error("Outlook checkpoint belongs to another capture scope")
        for folder, link in state.folder_links.items():
            self._collection_url(link, self._delta_url(folder))
        return state

    def _continuation(self, state: OutlookMailContinuation) -> ScanContinuation:
        try:
            return ScanContinuation(value=state.model_dump(mode="json"))
        except ValidationError:
            raise self._error("Outlook continuation exceeds the 64 KiB capacity") from None

    def _state(self, continuation: ScanContinuation) -> OutlookMailContinuation:
        try:
            state = OutlookMailContinuation.model_validate(continuation.value)
        except ValidationError:
            raise self._error("Invalid Outlook capture continuation") from None
        if state.fingerprint != self.fingerprint:
            raise self._error("Outlook continuation belongs to another capture scope")
        return state

    @staticmethod
    def _delta_url(folder: str) -> str:
        return f"{BASE}/mailFolders/{quote(folder, safe='')}/messages/delta"

    def _collection_url(self, url: str, expected: str) -> None:
        """Opaque queries remain unchanged, but cannot redirect to another collection."""
        self.graph._validate_url(url)
        path = unquote(urlsplit(url).path).replace("/mailfolders", "/mailFolders")
        expected_path = unquote(urlsplit(expected).path)
        allowed = {expected_path}
        prefix = "/v1.0/me/mailFolders/"
        if expected_path.startswith(prefix):
            folder, suffix = expected_path[len(prefix) :].split("/", 1)
            allowed.add(f"/v1.0/me/mailFolders('{folder}')/{suffix}")
        if path.rstrip("/") not in allowed:
            raise self._error("Outlook continuation changed the native collection")

    def _physical_folder(self, folder: OutlookFolder) -> bool:
        """Virtual views duplicate physical messages but may contain real child folders."""
        if folder.odata_type == "#microsoft.graph.mailSearchFolder":
            if folder.id in self.included_ids or folder.id in self.excluded_ids:
                raise self._error("Outlook search-folder selections are not supported")
            return False
        if folder.odata_type != "#microsoft.graph.mailFolder":
            raise self._error("Outlook unknown folder type is not qualified for delta capture")
        return True

    async def _topology_page(self, state: OutlookMailContinuation) -> OutlookMailContinuation:
        """Enumerate each folder's hidden children; topology never owns message visibility."""
        folder = state.folders_to_visit[0]
        collection = (
            f"{BASE}/mailFolders/{quote(folder, safe='')}/childFolders"
            if folder
            else f"{BASE}/mailFolders"
        )
        url = state.next_link or collection
        self._collection_url(url, collection)
        raw = await self.graph.get(
            url,
            params=None
            if state.next_link
            else {"includeHiddenFolders": "true", "$select": "id", "$top": 25},
            immutable_ids=True,
        )
        try:
            page = OutlookFolderPage.model_validate(raw)
        except ValidationError:
            raise self._error("Invalid Outlook folder inventory") from None
        discovered = list(state.discovered_folders)
        physical = list(state.remaining_folders)
        queue = list(state.folders_to_visit)
        for item in page.value:
            if self._physical_folder(item):
                physical.append(item.id)
            if item.id in discovered:
                raise InvalidCaptureCheckpoint("Outlook folder topology repeated an identity")
            discovered.append(item.id)
            queue.append(item.id)
        if page.next_link:
            self._collection_url(page.next_link, collection)
            if page.next_link == url:
                raise self._error("Outlook topology continuation did not advance")
        else:
            queue.pop(0)
        if queue:
            return state.model_copy(
                update={
                    "folders_to_visit": tuple(queue),
                    "discovered_folders": tuple(discovered),
                    "remaining_folders": tuple(physical),
                    "next_link": page.next_link,
                }
            )
        if set(state.folder_links) - set(physical):
            raise InvalidCaptureCheckpoint("Outlook folder disappeared; mailbox baseline required")
        return state.model_copy(
            update={
                "phase": "messages",
                "folders_to_visit": (),
                "discovered_folders": (),
                "remaining_folders": tuple(sorted(physical)),
                "next_link": None,
            }
        )

    def _page(
        self, state: OutlookMailContinuation, records: tuple[CaptureRecord, ...] = ()
    ) -> CapturePage:
        continuation = self._continuation(state)
        final = state.phase == "messages" and not state.remaining_folders
        # Check the final encoding as well: checkpoints escape Unicode while cursors do not.
        try:
            checkpoint = ProviderCheckpoint(
                value=OutlookMailCheckpoint(
                    fingerprint=self.fingerprint, folder_links=state.folder_links
                ).model_dump(mode="json")
            )
        except ValidationError:
            raise self._error("Outlook checkpoint exceeds the 64 KiB capacity") from None
        return CapturePage(
            records=records,
            continuation=continuation,
            final=final,
            provider_checkpoint=checkpoint if final else None,
        )

    async def _delta_page(self, state: OutlookMailContinuation) -> OutlookMailContinuation:
        """Interpret one opaque folder delta page before exact hydration."""
        folder = state.remaining_folders[0]
        collection = self._delta_url(folder)
        url = state.next_link or state.folder_links.get(folder) or collection
        self._collection_url(url, collection)
        try:
            raw = await self.graph.get(
                url,
                params={"$select": "id"} if url == collection else None,
                immutable_ids=True,
            )
        except (OutlookDeltaExpiredError, SourceGoneError):
            raise InvalidCaptureCheckpoint(
                "Outlook folder delta requires a fresh baseline"
            ) from None
        try:
            page = OutlookDeltaPage.model_validate(raw)
        except ValidationError:
            raise self._error("Invalid Outlook message delta") from None
        link = page.next_link or page.delta_link
        self._collection_url(link, collection)
        if page.next_link == url:
            raise self._error("Outlook delta continuation did not advance")
        links = dict(state.folder_links)
        if page.delta_link:
            links[folder] = page.delta_link
        state = state.model_copy(
            update={
                "folder_links": links,
                "next_link": page.next_link,
                "pending_ids": tuple(dict.fromkeys(item.id for item in page.value)),
            }
        )
        self._page(state)  # Enforce working-state capacity before MIME/blob work.
        return state

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """One topology page or exact message observation shares its atomic cursor commit."""
        self._require_principal()
        if scope != CompletedScope(record_type="message") or parent is not None:
            raise self._error("Outlook requires the mailbox message root")
        state = self._state(continuation)
        if state.phase == "topology":
            return self._page(await self._topology_page(state))
        if not state.remaining_folders:
            return self._page(state)
        if not state.pending_ids:
            state = await self._delta_page(state)
        records = ()
        if state.pending_ids:
            observation = await self.message(state.pending_ids[0], files=files)
            # Full omissions exact-refresh known roots. Changes must carry scope withdrawals.
            if observation.kind == "upsert" or state.mode == "changes":
                records = (observation,)
            state = state.model_copy(update={"pending_ids": state.pending_ids[1:]})
        if not state.pending_ids and state.next_link is None:
            state = state.model_copy(update={"remaining_folders": state.remaining_folders[1:]})
        return self._page(state, records)

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
