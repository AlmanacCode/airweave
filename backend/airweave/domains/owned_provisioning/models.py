"""Backend-only desired account capture contract; credentials remain in Composio."""

from typing import Literal
from uuid import UUID

from croniter import croniter
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationInfo,
    field_validator,
    model_validator,
)


class ManagedSource(BaseModel):
    """Native identity is supplied only by Almanac's verified account authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    provider: Literal[
        "gmail",
        "google_calendar",
        "google_drive",
        "slack",
        "outlook_mail",
        "stripe",
        "linear",
        "attio",
    ]
    expected_identity: str = Field(min_length=1, max_length=512)
    expected_user_identity: str | None = Field(default=None, min_length=1, max_length=512)
    collection: str = Field(min_length=1, max_length=255)
    auth_provider: str = Field(min_length=1, max_length=255)
    connected_account_id: str = Field(min_length=1, max_length=255)
    auth_config_id: str = Field(min_length=1, max_length=255)
    user_id: str = Field(min_length=1, max_length=255)
    config: dict[str, JsonValue] = Field(default_factory=dict)
    cron: str = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def native_principal(self):
        """Slack workspace membership requires its native user, not broker user_id."""
        if (self.provider == "slack") != (self.expected_user_identity is not None):
            raise ValueError("Only Slack requires an expected native user identity")
        if self.provider in {"stripe", "linear", "attio"}:
            self.source_config()  # Validate native scope before admission, not during delivery.
        return self

    @field_validator("expected_identity")
    @classmethod
    def workspace_identity(cls, value: str, info: ValidationInfo) -> str:
        """Workspace UUIDs have one canonical spelling across admission and reconnect."""
        if info.data.get("provider") in {"linear", "attio"}:
            return str(UUID(value))
        return value

    @field_validator("cron")
    @classmethod
    def valid_schedule(cls, value: str) -> str:
        """Use the existing scheduler dependency rather than inventing cron parsing."""
        if len(value.split()) != 5 or not croniter.is_valid(value):
            raise ValueError("Use a valid five-field cron schedule")
        return value

    def source_config(self) -> dict[str, JsonValue]:
        """Expected identity cannot be overridden inside unstructured provider config."""
        if self.provider in {"linear", "attio"}:
            from airweave.platform.configs.config import AttioConfig, LinearConfig

            config_type = LinearConfig if self.provider == "linear" else AttioConfig
            parsed = config_type.model_validate(
                {**self.config, "workspace_id": self.expected_identity}
            )
            value = parsed.model_dump(mode="json")
            if self.provider == "linear":
                value["team_ids"] = sorted(value["team_ids"])
            return value
        if self.provider == "stripe":
            from airweave.platform.configs.config import StripeCaptureConfig, StripeConfig

            binding = StripeCaptureConfig.model_validate(
                {**self.config, "expected_account_id": self.expected_identity}
            )
            return StripeConfig(original_capture=binding).model_dump(mode="json")
        if self.provider == "outlook_mail":
            return {
                **self.config,
                "expected_principal_id": self.expected_identity,
                "capture_originals": True,
            }
        if self.provider == "slack":
            return {
                **self.config,
                "expected_team_id": self.expected_identity,
                "expected_user_id": self.expected_user_identity,
            }
        field = {
            "gmail": "expected_mailbox",
            "google_calendar": "expected_primary_calendar_id",
            "google_drive": "expected_permission_id",
        }[self.provider]
        return {**self.config, field: self.expected_identity}

    def auth_config(self) -> dict[str, str]:
        """Selectors contain no provider tokens or API keys."""
        return {
            "account_id": self.connected_account_id,
            "auth_config_id": self.auth_config_id,
            "user_id": self.user_id,
        }


class EnsureSource(BaseModel):
    """Monotonic account authority request, safe for exact retries."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    generation: int = Field(ge=1, strict=True)
    state: Literal["active", "paused", "disconnected"]
    source: ManagedSource | None = None

    @model_validator(mode="after")
    def active_source(self) -> "EnsureSource":
        """Inactive requests never replace identity or credentials."""
        if self.state == "active" and self.source is None:
            raise ValueError("An active account requires its verified source specification")
        if self.state != "active" and self.source is not None:
            raise ValueError("Inactive state does not accept new credentials or identity")
        return self


class ProvisionedSource(BaseModel):
    """Committed identifiers and delivery state, not proof of capture completion."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    account_id: UUID
    organization_id: UUID
    generation: int
    observed_generation: int
    state: Literal["pending", "ready", "paused", "disconnected"]
    source_connection_id: UUID | None
    sync_id: UUID | None
    expected_identity: str | None
    expected_user_identity: str | None = None


def _workspace_principal(provider: str, config: dict) -> str:
    """Both workspace sources retain canonical UUIDs in their validated native config."""
    from airweave.platform.configs.config import AttioConfig, LinearConfig

    schema = LinearConfig if provider == "linear" else AttioConfig
    return str(schema.model_validate(config).workspace_id)


def native_principal(provider: str, config: dict) -> tuple[str, str | None]:
    """Read the original attested principal from the protected source config."""
    if provider in {"linear", "attio"}:
        return _workspace_principal(provider, config), None
    return _account_principal(provider, config)


def _account_principal(provider: str, config: dict) -> tuple[str, str | None]:
    """Validate the established provider-specific account identity contracts."""
    from airweave.platform.configs.config import (
        GmailConfig,
        GoogleCalendarConfig,
        GoogleDriveConfig,
        OutlookMailConfig,
        SlackConfig,
        StripeConfig,
    )

    user = None
    match provider:
        case "gmail":
            identity = GmailConfig.model_validate(config).expected_mailbox
        case "google_calendar":
            identity = GoogleCalendarConfig.model_validate(config).expected_primary_calendar_id
        case "google_drive":
            identity = GoogleDriveConfig.model_validate(config).expected_permission_id
        case "outlook_mail":
            outlook = OutlookMailConfig.model_validate(config)
            if not outlook.capture_originals:
                raise ValueError("Owned Outlook source must capture originals")
            identity = outlook.expected_principal_id
        case "stripe":
            stripe = StripeConfig.model_validate(config)
            if stripe.original_capture is None:
                raise ValueError("Owned Stripe source must capture originals")
            identity = stripe.original_capture.expected_account_id
        case "slack":
            slack = SlackConfig.model_validate(config)
            identity, user = slack.expected_team_id, slack.expected_user_id
        case _:
            raise ValueError("Unsupported owned source provider")
    if identity is None:
        raise ValueError("Owned source has no trusted native identity")
    return identity, user
