"""Shared Composio infrastructure for explicitly owned account capture."""

from typing import Annotated

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from airweave.domains.owned_provisioning.models import ManagedSource

Nonblank = Annotated[str, Field(min_length=1, max_length=255, pattern=r"\S")]


class OwnedComposioSettings(BaseModel):
    """One deployment project; tenant records contain only account selectors."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    project_key: Nonblank
    api_key: SecretStr = Field(repr=False)
    auth_config_ids: dict[Nonblank, Nonblank] = Field(min_length=1)

    @model_validator(mode="after")
    def nonblank_key(self):
        """Reject a configured project without its developer credential."""
        if not self.api_key.get_secret_value().strip():
            raise ValueError("Owned Composio API key must not be blank")
        return self

    def verify(self, source: ManagedSource) -> None:
        """Never infer a project or auth config from a caller-selected account."""
        if (
            source.project_key != self.project_key
            or self.auth_config_ids.get(source.provider) != source.auth_config_id
        ):
            raise HTTPException(409, "Owned Composio project or auth config mismatch")
