"""Google Calendar originals with durable per-calendar capture and observed range reads.

Canonical capture uses the shared page engine: complete calendar membership,
per-calendar full or incremental raw events, and a fixed expanded-instance window.
The engine owns every page acknowledgement and completed scope publication.

The upstream BaseSource entity interface remains implemented below; it is not the
canonical cursor authority. Configured native calendar IDs and occurrence windows
are validated by GoogleCalendarConfig.
"""

from __future__ import annotations

import json
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.auth_provider.exceptions import (
    AuthProviderRateLimitError,
    AuthProviderServerError,
)
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.entities.canonical.cycle_models import CaptureCycle, CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    CapturePlan,
    InvalidScanContinuation,
)
from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation, ScanState
from airweave.domains.entities.canonical.scope_execution import ScopePlan
from airweave.domains.sources.exceptions import SourceError, SourceRateLimitError
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.config import GoogleCalendarConfig
from airweave.platform.cursors.google_calendar import GoogleCalendarCursor
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity, Breadcrumb
from airweave.platform.entities.google_calendar import (
    GoogleCalendarCalendarEntity,
    GoogleCalendarEventEntity,
    GoogleCalendarFreeBusyEntity,
    GoogleCalendarListEntity,
)
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.bounded_response import bounded_response_bytes
from airweave.platform.http_client.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    should_retry_on_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.platform.sources.records.calendar_pages import CalendarPages, invalid_page_token
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType


def _retry_capture(exception: BaseException) -> bool:
    """Do not retry earlier than a provider's minimum beyond this request wait budget."""
    if isinstance(exception, (SourceRateLimitError, AuthProviderRateLimitError)):
        return exception.retry_after <= 120
    if isinstance(exception, AuthProviderServerError):
        return True
    if isinstance(exception, httpx.HTTPStatusError) and exception.response.status_code == 429:
        try:
            return float(exception.response.headers.get("Retry-After", "0")) <= 120
        except ValueError:
            return True
    return should_retry_on_rate_limit_or_timeout(exception)


@source(
    name="Google Calendar",
    short_name="google_calendar",
    auth_methods=[
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
    ],
    oauth_type=OAuthType.WITH_REFRESH,
    requires_byoc=True,
    auth_config_class=None,
    config_class=GoogleCalendarConfig,
    labels=["Productivity", "Calendar"],
    supports_continuous=True,
    cursor_class=GoogleCalendarCursor,
    rate_limit_level=RateLimitLevel.ORG,
)
class GoogleCalendarSource(BaseSource):
    """Google Calendar source connector integrates with the Google Calendar API to extract data.

    Synchronizes calendars, events, and free/busy information.

    It provides comprehensive access to your
    Google Calendar scheduling information for productivity and time management insights.
    """

    canonical_record_types = ("calendar", "event", "event_occurrence")
    canonical_container_parents = {"event": "calendar", "event_occurrence": "calendar"}

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Calendar membership owns exact native event and occurrence scopes."""
        return CalendarPages(self._get_capture_json, self.calendar_config).configuration

    async def prepare_cycle(self, previous: CaptureCycle | None) -> CapturePlan:
        """Resolve the expansion window once per persisted cycle."""
        return CalendarPages(self._get_capture_json, self.calendar_config).prepare()

    async def prepare_scope(
        self,
        scope: CompletedScope,
        cycle: CaptureCycle,
        previous: ScanState | None,
        *,
        parent: SourceRecord | None,
        force_full: bool,
    ) -> ScopePlan:
        """Choose native delta only from compatible completed exact-scope evidence."""
        return CalendarPages(self._get_capture_json, self.calendar_config).scope_plan(
            scope, cycle, previous, parent=parent, force_full=force_full
        )

    def initial_scope_continuation(
        self, scope: CompletedScope, cycle: CaptureCycle, plan: ScopePlan
    ) -> ScanContinuation:
        """Provider progress starts from the immutable engine-attested scope plan."""
        return CalendarPages.initial(plan)

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Native calendar IDs remain the stable container namespace."""
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
        """Originals and pagination commit together through the production page driver."""
        return await CalendarPages(self._get_capture_json, self.calendar_config).page(
            scope, continuation
        )

    async def confirm_absent(self, record: SourceRecord) -> None:
        """An accessible omitted calendar prevents premature membership reconciliation."""
        if record.identity.record_type != "calendar" or record.parent is not None:
            raise ValueError("Calendar omission must identify root membership")
        await CalendarPages(self._get_capture_json, self.calendar_config).confirm_member_absent(
            record.identity.native_id
        )

    # -----------------------
    # Construction / Config
    # -----------------------
    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: GoogleCalendarConfig,
    ) -> GoogleCalendarSource:
        """Create a new Google Calendar source instance."""
        instance = cls(auth=auth, logger=logger, http_client=http_client)

        instance.calendar_config = config
        config_dict = config.model_dump() if config else {}
        instance.batch_generation = bool(config_dict.get("batch_generation", False))
        instance.batch_size = int(config_dict.get("batch_size", 30))
        instance.max_queue_size = int(config_dict.get("max_queue_size", 200))
        instance.preserve_order = bool(config_dict.get("preserve_order", False))
        instance.stop_on_error = bool(config_dict.get("stop_on_error", False))

        return instance

    # -----------------------
    # HTTP helpers
    # -----------------------

    async def _authed_headers(self) -> Dict[str, str]:
        """Build Authorization headers with a fresh token."""
        return await authorization_headers(self.auth)

    async def _refresh_and_get_headers(self) -> Dict[str, str]:
        """Force-refresh the token and return updated headers."""
        return await authorization_headers(self.auth, refresh=True)

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception(_retry_capture),
        wait=wait_rate_limit_with_backoff,
        reraise=True,
    )
    async def _get_capture_json(self, url: str, params: Optional[dict] = None) -> dict:
        """Bound native capture responses, preserving auth refresh and typed errors."""
        headers = {**await self._authed_headers(), "Accept-Encoding": "identity"}
        for attempt in range(2):
            async with self.http_client.stream(
                "GET", url, headers=headers, params=params
            ) as response:
                if response.status_code == 401 and attempt == 0 and self.auth.supports_refresh:
                    headers = {
                        **await self._refresh_and_get_headers(),
                        "Accept-Encoding": "identity",
                    }
                    continue
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise SourceError(
                        "Calendar did not honor identity encoding",
                        source_short_name="google_calendar",
                    )
                maximum = 32 * 1024 * 1024 if response.is_success else 65536
                body = await bounded_response_bytes(response, maximum, label="Calendar response")
                buffered = httpx.Response(
                    response.status_code,
                    headers=response.headers,
                    request=response.request,
                    content=body,
                )
                if response.status_code == 400 and params and params.get("pageToken"):
                    try:
                        native_error = json.loads(body)
                    except (ValueError, UnicodeError):
                        native_error = None
                    if isinstance(native_error, dict) and invalid_page_token(native_error):
                        raise InvalidScanContinuation("Calendar rejected its saved page token")
                raise_for_status(
                    buffered,
                    source_short_name=self.short_name,
                    token_provider_kind=self.auth.provider_kind,
                )
                value = json.loads(body)
                if not isinstance(value, dict):
                    raise ValueError("Calendar response must be an object")
                return value
        raise AssertionError("Unreachable Calendar refresh state")

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout,
        wait=wait_rate_limit_with_backoff,
        reraise=True,
    )
    async def _get(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Make an authenticated GET request to the Google Calendar API."""
        headers = await self._authed_headers()
        response = await self.http_client.get(url, headers=headers, params=params)

        if response.status_code == 401 and self.auth.supports_refresh:
            self.logger.warning(
                f"Got 401 Unauthorized from Google Calendar API at {url}, refreshing token..."
            )
            headers = await self._refresh_and_get_headers()
            response = await self.http_client.get(url, headers=headers, params=params)

        raise_for_status(
            response,
            source_short_name=self.short_name,
            token_provider_kind=self.auth.provider_kind,
        )
        return response.json()

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout,
        wait=wait_rate_limit_with_backoff,
        reraise=True,
    )
    async def _post(self, url: str, json_data: Dict) -> Dict:
        """Make an authenticated POST request to the Google Calendar API."""
        headers = await self._authed_headers()
        headers["Content-Type"] = "application/json"
        response = await self.http_client.post(url, headers=headers, json=json_data)

        if response.status_code == 401 and self.auth.supports_refresh:
            self.logger.warning(
                f"Got 401 Unauthorized from Google Calendar API at {url}, refreshing token..."
            )
            headers = await self._refresh_and_get_headers()
            headers["Content-Type"] = "application/json"
            response = await self.http_client.post(url, headers=headers, json=json_data)

        raise_for_status(
            response,
            source_short_name=self.short_name,
            token_provider_kind=self.auth.provider_kind,
        )
        return response.json()

    # -----------------------
    # Listing / entity helpers
    # -----------------------
    async def _generate_calendar_list_entities(
        self,
    ) -> AsyncGenerator[GoogleCalendarListEntity, None]:
        """Yield GoogleCalendarListEntity objects for each calendar in the user's CalendarList."""
        url = "https://www.googleapis.com/calendar/v3/users/me/calendarList"
        params: Dict[str, Any] = {"maxResults": 100}
        page = 0
        while True:
            page += 1
            self.logger.info(f"Fetching CalendarList page #{page} with params: {params}")
            data = await self._get(url, params=params)
            items = data.get("items", []) or []
            self.logger.info(f"CalendarList page #{page} returned {len(items)} items")
            for cal in items:
                yield GoogleCalendarListEntity.from_api(cal)
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                self.logger.info("No more CalendarList pages")
                break
            params["pageToken"] = next_page_token

    async def _generate_calendar_entity(
        self, calendar_id: str
    ) -> AsyncGenerator[GoogleCalendarCalendarEntity, None]:
        """Yield a GoogleCalendarCalendarEntity for the specified calendar_id."""
        encoded_calendar_id = urllib.parse.quote(calendar_id)
        url = f"https://www.googleapis.com/calendar/v3/calendars/{encoded_calendar_id}"
        self.logger.info(f"Fetching Calendar resource for calendar_id={calendar_id}")
        data = await self._get(url)
        yield GoogleCalendarCalendarEntity.from_api(data)

    async def _generate_event_entities(
        self, calendar_list_entry: GoogleCalendarListEntity
    ) -> AsyncGenerator[GoogleCalendarEventEntity, None]:
        """Yield GoogleCalendarEventEntities for all events in the given calendar."""
        encoded_calendar_id = urllib.parse.quote(calendar_list_entry.calendar_key)
        base_url = f"https://www.googleapis.com/calendar/v3/calendars/{encoded_calendar_id}/events"
        params: Dict[str, Any] = {"maxResults": 100}
        cal_breadcrumb = Breadcrumb(
            entity_id=calendar_list_entry.calendar_key,
            name=calendar_list_entry.display_name,
            entity_type=GoogleCalendarListEntity.__name__,
        )
        page = 0
        while True:
            page += 1
            self.logger.info(
                f"Fetching events page #{page} for calendar_id={calendar_list_entry.calendar_key} "
                f"params={params}"
            )
            data = await self._get(base_url, params=params)
            events = data.get("items", []) or []
            self.logger.info(
                f"Events page #{page} for calendar_id={calendar_list_entry.calendar_key}: "
                f"{len(events)} events"
            )
            for event in events:
                yield GoogleCalendarEventEntity.from_api(
                    event,
                    calendar_key=calendar_list_entry.calendar_key,
                    breadcrumbs=[cal_breadcrumb],
                )

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                self.logger.info(
                    f"No more event pages for calendar_id={calendar_list_entry.calendar_key}"
                )
                break
            params["pageToken"] = next_page_token

    async def _generate_freebusy_entities(
        self, calendar_list_entry: GoogleCalendarListEntity
    ) -> AsyncGenerator[GoogleCalendarFreeBusyEntity, None]:
        """Yield a GoogleCalendarFreeBusyEntity for the next 7 days for each calendar."""
        url = "https://www.googleapis.com/calendar/v3/freeBusy"
        now = datetime.utcnow()
        in_7_days = now + timedelta(days=7)

        request_body = {
            "timeMin": now.isoformat() + "Z",
            "timeMax": in_7_days.isoformat() + "Z",
            "items": [{"id": calendar_list_entry.calendar_key}],
        }
        self.logger.info(f"Fetching FreeBusy for calendar_id={calendar_list_entry.calendar_key}")
        data = await self._post(url, request_body)
        cal_busy_info = data.get("calendars", {}).get(calendar_list_entry.calendar_key, {}) or {}
        busy_ranges = cal_busy_info.get("busy", []) or []
        web_url = (
            f"https://calendar.google.com/calendar/u/0/r?cid="
            f"{urllib.parse.quote(calendar_list_entry.calendar_key)}"
        )

        yield GoogleCalendarFreeBusyEntity(
            breadcrumbs=[],
            freebusy_key=f"{calendar_list_entry.calendar_key}_freebusy",
            label=f"Free/Busy for {calendar_list_entry.display_name}",
            calendar_id=calendar_list_entry.calendar_key,
            busy=busy_ranges,
            web_url_value=web_url,
        )

    async def _process_calendars_sequential(
        self, calendar_list_entries: List[GoogleCalendarListEntity]
    ) -> AsyncGenerator[BaseEntity, None]:
        """Process calendars sequentially (original behavior)."""
        for cal_list_entity in calendar_list_entries:
            async for calendar_entity in self._generate_calendar_entity(
                cal_list_entity.calendar_key
            ):
                yield calendar_entity

        for cal_list_entity in calendar_list_entries:
            async for event_entity in self._generate_event_entities(cal_list_entity):
                yield event_entity

        for cal_list_entity in calendar_list_entries:
            async for freebusy_entity in self._generate_freebusy_entities(cal_list_entity):
                yield freebusy_entity

    async def _process_calendars_concurrent(
        self, calendar_list_entries: List[GoogleCalendarListEntity]
    ) -> AsyncGenerator[BaseEntity, None]:
        """Process calendars concurrently using bounded concurrency."""

        async def _calendar_worker(cal_list_entity: GoogleCalendarListEntity):
            """Emit Calendar resource, its events, then free/busy for a single calendar."""
            async for calendar_entity in self._generate_calendar_entity(
                cal_list_entity.calendar_key
            ):
                yield calendar_entity

            async for event_entity in self._generate_event_entities(cal_list_entity):
                yield event_entity

            async for freebusy_entity in self._generate_freebusy_entities(cal_list_entity):
                yield freebusy_entity

        async for ent in self.process_entities_concurrent(
            items=calendar_list_entries,
            worker=_calendar_worker,
            batch_size=getattr(self, "batch_size", 30),
            preserve_order=getattr(self, "preserve_order", False),
            stop_on_error=getattr(self, "stop_on_error", False),
            max_queue_size=getattr(self, "max_queue_size", 200),
        ):
            if ent is not None:
                yield ent

    # -----------------------
    # Top-level orchestration
    # -----------------------
    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Generate all Google Calendar entities.

        Yields entities in the following order:
          - CalendarList entries
          - Underlying Calendar resources
          - Events for each calendar
          - FreeBusy data for each calendar (7-day window)

        In concurrent mode, Step 2-4 are processed per-calendar using bounded concurrency.
        """
        calendar_list_entries: List[GoogleCalendarListEntity] = []
        async for cal_list_entity in self._generate_calendar_list_entities():
            yield cal_list_entity
            calendar_list_entries.append(cal_list_entity)

        if not calendar_list_entries:
            self.logger.info("No calendars found in CalendarList; generation complete.")
            return

        if getattr(self, "batch_generation", False):
            async for entity in self._process_calendars_concurrent(calendar_list_entries):
                yield entity
        else:
            async for entity in self._process_calendars_sequential(calendar_list_entries):
                yield entity

    async def validate(self) -> None:
        """Validate credentials by pinging the Calendar API calendarList endpoint."""
        await self._get(
            "https://www.googleapis.com/calendar/v3/users/me/calendarList",
            params={"maxResults": "1"},
        )
