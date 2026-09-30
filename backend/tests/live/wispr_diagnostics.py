"""Allowlisted diagnostic metadata; never retain provider error strings or arguments."""

import json
from typing import Literal

from pydantic import BaseModel


class WisprDiagnostics(BaseModel):
    operation: Literal["metadata", "session", "search", "get", "unknown"] = "unknown"
    last_http_status: int | None = None
    tool_error_present: bool | None = None
    tool_error_kind: Literal["string", "object", "array", "boolean", "number", "none"] | None = None
    rate_signal_detected: bool = False

    def request(self, request):
        self.last_http_status = None
        self.tool_error_present = self.tool_error_kind = None
        self.rate_signal_detected = False
        path = request.url.path
        if "/connected_accounts/" in path:
            self.operation = "metadata"
        elif path.endswith("/tool_router/session"):
            self.operation = "session"
        elif path.endswith("/execute"):
            try:
                value = json.loads(request.content)
            except (ValueError, UnicodeDecodeError):
                value = None
            slug = value.get("tool_slug") if isinstance(value, dict) else None
            self.operation = {
                "WISPR_FLOW_MCP_SEARCH_MEETINGS": "search",
                "WISPR_FLOW_MCP_GET_MEETING": "get",
            }.get(slug, "unknown")
        else:
            self.operation = "unknown"

    async def response(self, response):
        self.last_http_status = response.status_code
        self.rate_signal_detected = response.status_code == 429

    def envelope(self, value):
        if self.operation not in {"search", "get"}:
            return
        error = value.get("error")
        self.tool_error_present = error is not None
        if error is None:
            self.tool_error_kind = "none"
        elif isinstance(error, str):
            self.tool_error_kind = "string"
            lowered = error.lower()
            self.rate_signal_detected |= "rate limit" in lowered or "too many requests" in lowered
        elif isinstance(error, dict):
            self.tool_error_kind = "object"
            self.rate_signal_detected |= any(
                type(error.get(key)) is int and error[key] == 429
                for key in ("code", "status", "status_code")
            )
        elif isinstance(error, list):
            self.tool_error_kind = "array"
        elif isinstance(error, bool):
            self.tool_error_kind = "boolean"
        else:
            self.tool_error_kind = "number"


async def observe_request(diagnostics, meter, request):
    await meter(request)
    diagnostics.request(request)
