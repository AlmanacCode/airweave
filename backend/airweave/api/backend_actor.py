"""Shared backend-only authority gate; organization keys retain their existing scope."""

from fastapi import Depends, HTTPException

from airweave.api import deps
from airweave.api.context import ApiContext


def backend_actor(ctx: ApiContext = Depends(deps.get_context)) -> ApiContext:
    """A user session cannot act as Almanac's credential/identity authority."""
    if not ctx.is_api_key_auth:
        raise HTTPException(status_code=403, detail="Owned operations require a backend API key")
    return ctx


def backend_search_actor(ctx: ApiContext = Depends(deps.get_owned_search_context)) -> ApiContext:
    """Apply the same backend gate after search authentication closes its SQL session."""
    return backend_actor(ctx)
