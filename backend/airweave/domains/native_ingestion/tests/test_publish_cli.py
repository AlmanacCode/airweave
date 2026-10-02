"""CLI validation and outcome claims, without duplicating publisher lifecycle tests."""

import json
from uuid import UUID

import httpx
import pytest
from evaluation import native_import_cli as cli

from airweave.domains.native_ingestion.tests.test_ingestion import snapshot


@pytest.fixture
def invocation(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRWEAVE_API_KEY", "private-test-key")
    path = tmp_path / "staged.json"
    path.write_text(json.dumps([snapshot().model_dump(mode="json")]))
    return path, [
        "--url",
        "http://127.0.0.1:18086/api/v1/",
        "--input",
        str(path),
        "--owner",
        "owner-one",
        "--dataset",
        "knowledge",
        "--collection",
        "native-test",
        "--request-key",
        "repeat-this",
    ]


def transport(monkeypatch, handler):
    client = httpx.AsyncClient

    def configured(**kw):
        assert kw["follow_redirects"] is False and kw["trust_env"] is False
        return client(transport=httpx.MockTransport(handler), **kw)

    monkeypatch.setattr(cli.httpx, "AsyncClient", configured)


@pytest.mark.parametrize("invalid", ["json", "oversize", "owner", "credentials"])
def test_invalid_input_makes_no_http(invocation, monkeypatch, capsys, invalid):
    path, args = invocation
    calls = []

    def no_http(request):
        calls.append(request)
        raise AssertionError("Invalid input reached HTTP")

    transport(monkeypatch, no_http)
    if invalid == "json":
        path.write_text('{"secret-content":')
    elif invalid == "oversize":
        monkeypatch.setattr(cli, "MAX_INPUT_BYTES", 1)
    elif invalid == "owner":
        args[5] = "wrong-owner"
    else:
        monkeypatch.delenv("AIRWEAVE_API_KEY")
    assert cli.main(args) in (2, 4)
    assert not calls
    output = capsys.readouterr()
    assert not output.out
    assert "secret-content" not in output.err and "private-test-key" not in output.err


def test_lost_response_is_unknown_not_success(invocation, monkeypatch, capsys):
    _, args = invocation
    calls = []

    def lose(request):
        calls.append(request)
        raise httpx.ReadTimeout("private-test-key and private body", request=request)

    transport(monkeypatch, lose)
    assert cli.main(args) == 3
    assert len(calls) == 1
    output = capsys.readouterr()
    assert not output.out
    assert "outcome unknown" in output.err and "private" not in output.err


def test_completed_retry_outputs_capture_summary(invocation, monkeypatch, capsys):
    _, args = invocation
    requests = []
    identifier = str(UUID(int=1))

    def destination(request):
        requests.append(request)
        assert request.headers["X-API-Key"] == "private-test-key"
        if request.url.path.endswith("/native/sources"):
            return httpx.Response(
                200,
                json={
                    "source_connection_id": identifier,
                    "sync_id": identifier,
                    "organization_id": identifier,
                    "binding": {"owner_id": "owner-one", "dataset": "knowledge"},
                    "collection": "native-test",
                    "available": True,
                },
            )
        assert request.url.path.endswith("/imports/repeat-this")
        return httpx.Response(
            200,
            json={
                "source_id": identifier,
                "import_id": identifier,
                "cycle_id": identifier,
                "request_key": "repeat-this",
                "request": json.loads(request.content),
                "status": "completed",
                "summary": {
                    "outcome": "completed",
                    "coverage": "bounded",
                    "finished_at": "2026-10-01T00:00:00Z",
                    "sequence": 1,
                    "completed_scopes": 1,
                    "capture_complete": True,
                    "indexing": "not_verified",
                },
            },
        )

    transport(monkeypatch, destination)
    assert cli.main(args) == 0
    assert len(requests) == 2
    output = capsys.readouterr()
    assert not output.err
    summary = json.loads(output.out)
    assert summary["capture_complete"] and summary["indexing"] == "not_verified"
    assert "private-test-key" not in output.out


@pytest.mark.parametrize(
    "url",
    [
        "https://user:secret@example.com/api/",
        "https://example.com/api/?",
        "https://example.com/api/#",
        "http://localhost.example.com/api/",
        "file:///tmp/api/",
    ],
)
def test_unsafe_url_rejected(invocation, url, capsys):
    _, args = invocation
    args[1] = url
    assert cli.main(args) == 2
    assert url not in capsys.readouterr().err


def test_help_needs_no_server_settings(tmp_path):
    import os
    import subprocess
    import sys
    from pathlib import Path

    backend = Path(__file__).resolve().parents[4]
    result = subprocess.run(
        [sys.executable, "-m", "evaluation.native_import_cli", "--help"],
        cwd=tmp_path,
        env={"PATH": os.defpath, "PYTHONPATH": f"{backend}:{backend.parent}"},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert "--request-key" in result.stdout
    assert not result.stderr
