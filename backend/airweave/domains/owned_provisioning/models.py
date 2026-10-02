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

from airweave.domains.auth_provider.assurance import (
    AccountAssurance,
    BrokerConnection,
    ProviderIdentity,
)


class ManagedSource(BaseModel):
    """Native identity is supplied only by Almanac's verified account authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    provider: Literal[
        "wispr",
        "gmail",
        "google_calendar",
        "google_drive",
        "slack",
        "outlook_mail",
        "outlook_calendar",
        "stripe",
        "linear",
        "attio",
        "github",
        "notion",
    ]
    expected_identity: str | None = Field(default=None, min_length=1, max_length=512)
    assurance: AccountAssurance | None = None
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
        """Workspace-scoped grants retain the native member or bot, not broker user_id."""
        if self.provider == "wispr":
            if not isinstance(self.assurance, BrokerConnection) or (
                self.expected_identity is not None
                or self.expected_user_identity is not None
                or self.assurance.user_id != self.user_id
                or self.assurance.connected_account_id != self.connected_account_id
                or self.assurance.auth_config_id != self.auth_config_id
            ):
                raise ValueError(
                    "Wispr requires its exact broker assurance without native identity"
                )
            return self
        if isinstance(self.assurance, BrokerConnection) or self.expected_identity is None:
            raise ValueError("This provider requires its native principal")
        if self.assurance is not None and self.assurance != ProviderIdentity(
            account_id=self.expected_identity, user_id=self.expected_user_identity
        ):
            raise ValueError("Provider assurance disagrees with its native principal")
        if (self.provider in {"slack", "notion"}) != (self.expected_user_identity is not None):
            raise ValueError("Slack and Notion require an expected native user or bot identity")
        if self.provider in {"stripe", "linear", "attio", "github", "notion"}:
            self.source_config()  # Validate native scope before admission, not during delivery.
        return self

    @field_validator("expected_identity")
    @classmethod
    def canonical_identity(cls, value: str, info: ValidationInfo) -> str:
        """Native IDs have one canonical spelling across admission and reconnect."""
        if info.data.get("provider") == "github":
            if not value.isascii() or not value.isdecimal() or int(value) <= 0:
                raise ValueError("GitHub identity must be a positive numeric user ID")
            return str(int(value))
        if info.data.get("provider") in {"linear", "attio", "notion"}:
            return str(UUID(value))
        return value

    @field_validator("expected_user_identity")
    @classmethod
    def canonical_member(cls, value: str | None, info: ValidationInfo) -> str | None:
        """Notion bot UUID is part of the verified native account pair."""
        if value is not None and info.data.get("provider") == "notion":
            return str(UUID(value))
        return value

    @field_validator("cron")
    @classmethod
    def valid_schedule(cls, value: str) -> str:
        """Use the existing scheduler dependency rather than inventing cron parsing."""
        if len(value.split()) != 5 or not croniter.is_valid(value):
            raise ValueError("Use a valid five-field cron schedule")
        return value

    @property
    def account_assurance(self) -> AccountAssurance:
        """Return the typed assertion from the supplied facts."""
        return self.assurance or ProviderIdentity(
            account_id=self.expected_identity, user_id=self.expected_user_identity
        )

    def source_config(self) -> dict[str, JsonValue]:
        """Expected identity cannot be overridden inside unstructured provider config."""
        if self.provider == "wispr":
            from airweave.platform.configs.config import WisprConfig

            return WisprConfig.model_validate(
                {**self.config, "assurance": self.assurance.model_dump(mode="json")}
            ).model_dump(mode="json")
        if self.provider == "notion":
            from airweave.platform.configs.config import NotionConfig

            return NotionConfig.model_validate(
                {
                    **self.config,
                    "expected_workspace_id": self.expected_identity,
                    "expected_bot_id": self.expected_user_identity,
                }
            ).model_dump(mode="json")
        if self.provider == "github":
            from airweave.platform.configs.config import GitHubConfig

            parsed = GitHubConfig.model_validate(
                {**self.config, "expected_user_id": int(self.expected_identity)}
            )
            value = parsed.model_dump(mode="json")
            value["repositories"] = sorted(
                value["repositories"], key=lambda item: item["repository_id"]
            )
            return value
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
        if self.provider in {"outlook_mail", "outlook_calendar"}:
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
        result = {
            "account_id": self.connected_account_id,
            "auth_config_id": self.auth_config_id,
            "user_id": self.user_id,
        }
        if self.provider == "wispr":
            result["project_key"] = self.assurance.project_key
        return result


class EnsureSource(BaseModel):
    """Monotonic account authority request, safe for exact retries."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    generation: int = Field(ge=1, strict=True)
    state: Literal["active", "paused", "unavailable", "disconnected"]
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
    state: Literal["pending", "ready", "paused", "unavailable", "disconnected"]
    source_connection_id: UUID | None
    sync_id: UUID | None
    expected_identity: str | None
    assurance: AccountAssurance | None = None
    expected_user_identity: str | None = None


def _workspace_principal(provider: str, config: dict) -> str:
    """Both workspace sources retain canonical UUIDs in their validated native config."""
    from airweave.platform.configs.config import AttioConfig, LinearConfig

    schema = LinearConfig if provider == "linear" else AttioConfig
    return str(schema.model_validate(config).workspace_id)


def native_principal(provider: str, config: dict) -> tuple[str, str | None]:
    """Read the original attested principal from the protected source config."""
    if provider == "notion":
        from airweave.platform.configs.config import NotionConfig

        notion = NotionConfig.model_validate(config)
        return str(notion.expected_workspace_id), str(notion.expected_bot_id)
    if provider == "github":
        from airweave.platform.configs.config import GitHubConfig

        return str(GitHubConfig.model_validate(config).expected_user_id), None
    if provider in {"linear", "attio"}:
        return _workspace_principal(provider, config), None
    return _account_principal(provider, config)


def _account_principal(provider: str, config: dict) -> tuple[str, str | None]:
    """Validate the established provider-specific account identity contracts."""
    from airweave.platform.configs.config import (
        GmailConfig,
        GoogleCalendarConfig,
        GoogleDriveConfig,
        OutlookCalendarConfig,
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
        case "outlook_mail" | "outlook_calendar":
            config_type = OutlookMailConfig if provider == "outlook_mail" else OutlookCalendarConfig
            outlook = config_type.model_validate(config)
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


def source_assurance(provider: str, config: dict, auth_config: dict) -> AccountAssurance:
    """Observe only committed, coherent source and credential selectors."""
    if provider != "wispr":
        account, user = native_principal(provider, config)
        return ProviderIdentity(account_id=account, user_id=user)
    from airweave.platform.configs.config import WisprConfig

    proof = WisprConfig.model_validate(config).assurance
    if proof is None or (
        proof.connected_account_id != auth_config.get("account_id")
        or proof.user_id != auth_config.get("user_id")
        or proof.auth_config_id != auth_config.get("auth_config_id")
        or proof.project_key != auth_config.get("project_key")
    ):
        raise ValueError("Committed Wispr broker selectors disagree")
    return proof
