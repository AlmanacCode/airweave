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
    response_hook=None,
    gmail_unfiltered=False,
    calendar_config=None,
    max_file_bytes=10 * 1024 * 1024,
):
    host = {"gmail": "gmail.googleapis.com", "slack": "slack.com"}.get(name, "www.googleapis.com")
    hosts = {host, "docs.googleapis.com"} if name == "google_drive" else {host}
    auth = ManagedAuthProvider(api_key=key, connected_account_id=account, allowed_hosts=hosts)
    transport = ComposioTransport(api_key=key, connected_account_id=account, allowed_hosts=hosts)
    transport.MAX_BINARY_BYTES = max_file_bytes
    hooks = {"request": [request_hook]} if request_hook is not None else {}
    if response_hook is not None:
        hooks["response"] = [response_hook]
    async with httpx.AsyncClient(transport=transport, timeout=180, event_hooks=hooks) as client:
        wrapped = AirweaveHttpClient(
            client, fence.organization_id, name, feature_flag_enabled=False
        )
        if name == "google_calendar":
            calendar_config = calendar_config or GoogleCalendarConfig()
            if calendar_config.expected_primary_calendar_id is None:
                calendar_config = GoogleCalendarConfig.model_validate(
                    {
                        **calendar_config.model_dump(),
                        "expected_primary_calendar_id": os.environ["LIVE_CALENDAR_PRIMARY_ID"],
                    }
                )
        source_type, config = {
            "gmail": (
                GmailSource,
                GmailConfig(
                    expected_mailbox=expected_email,
                    gmail_query=None,
                    included_labels=[],
                    excluded_labels=[],
                    excluded_categories=[],
                    after_date=None,
                )
                if gmail_unfiltered
                else GmailConfig(gmail_query=gmail_query, expected_mailbox=expected_email),
            ),
            "google_drive": (
                GoogleDriveSource,
                GoogleDriveConfig(expected_permission_id=os.environ["LIVE_DRIVE_PERMISSION_ID"])
                if name == "google_drive"
                else GoogleDriveConfig(),
            ),
            "google_calendar": (
                GoogleCalendarSource,
                calendar_config or GoogleCalendarConfig(),
            ),
            "slack": (
                SlackSource,
                SlackConfig(
                    expected_team_id=os.environ["LIVE_SLACK_TEAM_ID"],
                    expected_user_id=os.environ["LIVE_SLACK_USER_ID"],
                )
                if name == "slack"
                else SlackConfig(),
            ),
        }[name]
        source = await source_type.create(
            auth=auth, logger=logging.getLogger("probe"), http_client=wrapped, config=config
        )
        # Sources attest their explicitly pinned native identity during create.
        if name not in {"gmail", "google_calendar", "google_drive", "slack"}:
            await verify_rest_identity(name, source, expected_email)
        identity_kind = {
            "slack": "provider_team_user_id",
            "google_drive": "provider_permission_id",
            "google_calendar": "provider_primary_calendar_id",
        }.get(name, "provider_email")
        yield source, identity_kind


@asynccontextmanager
async def wispr_source(
    account, key, fence, *, request_hook=None, response_hook=None, envelope_hook=None
):
    """Verify exact Composio account binding; Wispr exposes no identity profile here."""
    expected_user = os.environ["LIVE_WISPR_USER_ID"]
    hooks = {"request": [request_hook]} if request_hook is not None else {}
    if response_hook is not None:
        hooks["response"] = [response_hook]
    async with httpx.AsyncClient(timeout=180, event_hooks=hooks) as client:
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
        if envelope_hook is not None:
            original_post = source._post

            async def observed_post(path, body):
                result = await original_post(path, body)
                envelope_hook(result)
                return result

            source._post = observed_post
        await source.validate()
        yield source, "composio_account_principal_only"
