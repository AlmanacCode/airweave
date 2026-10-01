"""Shared backend-only authority gate; organization keys retain their existing scope."""

from fastapi import Depends, HTTPException

from airweave.api import deps
from airweave.api.context import ApiContext


def backend_actor(ctx: ApiContext = Depends(deps.get_context)) -> ApiContext:
    """A user session cannot act as Almanac's credential/identity authority."""
    if not ctx.is_api_key_auth:
        raise HTTPException(status_code=403, detail="Owned provisioning requires a backend API key")
    return ctx
