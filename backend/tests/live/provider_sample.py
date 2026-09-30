"""Account-checked source construction for the bounded live capture harness."""

import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import quote

import httpx

from airweave.domains.sources.token_providers.protocol import (
    ManagedAuthProvider,
    ManagedToolAuthProvider,
)
from airweave.platform.configs.config import (
    GmailConfig,
    GoogleCalendarConfig,
    GoogleDriveConfig,
    SlackConfig,
)
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.composio_transport import ComposioTransport
from airweave.platform.sources import GmailSource, GoogleDriveSource
from airweave.platform.sources.google_calendar import GoogleCalendarSource
from airweave.platform.sources.slack import SlackSource
from airweave.platform.sources.wispr import WisprSource


async def verify_rest_identity(name, source, expected_email):
    """Use actual provider identity; names/connection labels are not identity."""
    if name == "gmail":
        profile = await source._get("https://gmail.googleapis.com/gmail/v1/users/me/profile")
        email = profile.get("emailAddress", "")
    elif name == "google_calendar":
        profile = await source._get(
            "https://www.googleapis.com/calendar/v3/users/me/calendarList/primary"
        )
        email = profile.get("id", "")
        if profile.get("primary") is not True:
            raise ValueError("Calendar identity response did not identify the primary calendar")
    elif name == "slack":
        identity = await source._get("https://slack.com/api/auth.test")
        if not identity.get("team_id") or not identity.get("user_id"):
            raise ValueError("Slack did not return workspace and user identity")
        profile = await source._get(
            "https://slack.com/api/users.info", {"user": identity["user_id"]}
        )
        if profile.get("user", {}).get("id") != identity["user_id"]:
            raise ValueError("Slack returned a different user")
        email = profile.get("user", {}).get("profile", {}).get("email", "")
    else:
        profile = await source._get(
            "https://www.googleapis.com/drive/v3/about", params={"fields": "user(emailAddress)"}
        )
        email = profile.get("user", {}).get("emailAddress", "")
    if email.casefold() != expected_email.casefold():
        raise ValueError("Provider identity does not match the explicitly selected email")


@asynccontextmanager
async def rest_source(
    name,
    account,
    expected_email,
    key,
    fence,
    *,
    gmail_query="newer_than:7d smaller:5M",
    request_hook=None,
    calendar_config=None,
    max_file_bytes=10 * 1024 * 1024,
):
    host = {"gmail": "gmail.googleapis.com", "slack": "slack.com"}.get(name, "www.googleapis.com")
    auth = ManagedAuthProvider(api_key=key, connected_account_id=account, allowed_hosts={host})
    transport = ComposioTransport(api_key=key, connected_account_id=account, allowed_hosts={host})
    transport.MAX_BINARY_BYTES = max_file_bytes
    hooks = {"request": [request_hook]} if request_hook is not None else {}
    async with httpx.AsyncClient(transport=transport, timeout=180, event_hooks=hooks) as client:
        wrapped = AirweaveHttpClient(
            client, fence.organization_id, name, feature_flag_enabled=False
        )
        source_type, config = {
            "gmail": (GmailSource, GmailConfig(gmail_query=gmail_query)),
            "google_drive": (GoogleDriveSource, GoogleDriveConfig()),
            "google_calendar": (GoogleCalendarSource, calendar_config or GoogleCalendarConfig()),
            "slack": (SlackSource, SlackConfig()),
        }[name]
        source = await source_type.create(
            auth=auth, logger=logging.getLogger("probe"), http_client=wrapped, config=config
        )
        await verify_rest_identity(name, source, expected_email)
        yield source, "provider_email"


@asynccontextmanager
async def wispr_source(account, key, fence):
    """Verify exact Composio account binding; Wispr exposes no identity profile here."""
    expected_user = os.environ["LIVE_WISPR_USER_ID"]
    async with httpx.AsyncClient(timeout=180) as client:
        metadata_response = await client.get(
            "https://backend.composio.dev/api/v3/connected_accounts/" + quote(account, safe=""),
            headers={"x-api-key": key},
        )
        metadata_response.raise_for_status()
        metadata = metadata_response.json()
        if (
            metadata.get("id") != account
            or metadata.get("status") != "ACTIVE"
            or metadata.get("toolkit", {}).get("slug") != "wispr_flow_mcp"
            or metadata.get("user_id") != expected_user
        ):
            raise ValueError("Wispr connected-account identity/state mismatch")
        source = await WisprSource.create(
            auth=ManagedToolAuthProvider(
                api_key=key, connected_account_id=account, user_id=expected_user
            ),
            logger=logging.getLogger("probe"),
            http_client=AirweaveHttpClient(
                client, fence.organization_id, "wispr", feature_flag_enabled=False
            ),
        )
        await source.validate()
        yield source, "composio_account_principal_only"
