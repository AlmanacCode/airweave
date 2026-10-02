"""Private owned-account assurance; broker binding is not native identity."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class ProviderIdentity(BaseModel):
    """Provider-reported principal, without a universal verification claim."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["provider_identity"] = "provider_identity"
    account_id: str = Field(min_length=1, max_length=512)
    user_id: str | None = Field(default=None, min_length=1, max_length=512)


class BrokerConnection(BaseModel):
    """An exact managed grant, with no native subject assertion."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["broker_connection"] = "broker_connection"
    project_key: str = Field(min_length=1, max_length=255)
    user_id: str = Field(min_length=1, max_length=255)
    toolkit: Literal["wispr_flow_mcp"] = "wispr_flow_mcp"
    auth_config_id: str = Field(min_length=1, max_length=255)
    connected_account_id: str = Field(min_length=1, max_length=255)


AccountAssurance = Annotated[ProviderIdentity | BrokerConnection, Field(discriminator="kind")]
