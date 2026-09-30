"""Safe metadata around actual Wispr validation; no network."""

from airweave.domains.entities.canonical.tests.test_resume_probe import subprocess_script


async def test_safe_validation_failure_metadata_without_payload_leak():
    script = r"""
import asyncio, json, os, sys
from types import SimpleNamespace
from uuid import uuid4
sys.path.insert(0, "tests/live")
import conftest
import httpx
import provider_sample
from wispr_diagnostics import WisprDiagnostics

async def main():
    original_client = httpx.AsyncClient
    for error, kind, rate in [("private-detail rate limit", "string", True),
                              ({"status_code": 429, "private": "secret"}, "object", True),
                              ("private-detail failure", "string", False)]:
        diagnostics = WisprDiagnostics()
        calls = []
        async def handle(request):
            calls.append(diagnostics.operation)
            if diagnostics.operation == "metadata":
                value = {"id": "fixture", "status": "ACTIVE", "user_id": "fixture",
                         "toolkit": {"slug": "wispr_flow_mcp"}}
            elif diagnostics.operation == "session":
                value = {"session_id": "fixture"}
            else:
                value = {"data": {}, "error": error}
            return httpx.Response(200, json=value)
        class MockClient(original_client):
            def __init__(self, **kwargs):
                super().__init__(transport=httpx.MockTransport(handle), **kwargs)
        provider_sample.httpx.AsyncClient = MockClient
        async def request(req): diagnostics.request(req)
        os.environ["LIVE_WISPR_USER_ID"] = "fixture"
        try:
            async with provider_sample.wispr_source("fixture", "fixture",
                SimpleNamespace(organization_id=uuid4()), request_hook=request,
                response_hook=diagnostics.response, envelope_hook=diagnostics.envelope):
                raise AssertionError("Validation error swallowed")
        except ValueError as exc:
            assert str(exc) == "Wispr tool execution failed; capture is incomplete"
        assert calls == ["metadata", "session", "search"]
        assert diagnostics.operation == "search" and diagnostics.last_http_status == 200
        assert diagnostics.tool_error_present and diagnostics.tool_error_kind == kind
        assert diagnostics.rate_signal_detected == rate
        assert "private" not in diagnostics.model_dump_json()
        assert "secret" not in diagnostics.model_dump_json()
    req = httpx.Request("POST", "https://backend.composio.dev/api/v3.1/tool_router/session/x/execute",
                        json={"tool_slug": "WISPR_FLOW_MCP_GET_MEETING",
                              "arguments": {"id": "private"}})
    diagnostics.request(req)
    await diagnostics.response(httpx.Response(429))
    assert diagnostics.operation == "get" and diagnostics.rate_signal_detected
    assert diagnostics.tool_error_present is None
    print("verified")
asyncio.run(main())
"""
    code, output, error = await subprocess_script(script, {})
    assert code == 0, error
    assert output.splitlines()[-1] == "verified"


async def test_reduced_wispr_budget_validation_before_network():
    script = r"""
import sys
sys.path.insert(0, "tests/live")
import conftest
from provider_lifecycle import wispr_request_limit
for value in ("0", "21", "-1", "no", "1.5"):
    try: wispr_request_limit(value)
    except ValueError: pass
    else: raise AssertionError("Invalid budget accepted")
assert wispr_request_limit("17") == 17
assert wispr_request_limit("20") == 20
assert wispr_request_limit("1") == 1
print("verified")
"""
    code, output, error = await subprocess_script(script, {})
    assert code == 0, error
    assert output.splitlines()[-1] == "verified"
