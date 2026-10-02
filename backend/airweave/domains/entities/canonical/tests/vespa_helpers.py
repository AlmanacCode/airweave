"""Disposable real Vespa schema deployment for synthetic integration tests."""

import asyncio
import io
import time
import zipfile
from pathlib import Path

import httpx


async def wait_healthy(client, url, seconds=180):
    """Bound readiness independently of the enclosing CI job timeout."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            response = await client.get(url)
            if (
                response.status_code == 200
                and response.json().get("status", {}).get("code") == "up"
            ):
                return
        except (httpx.TransportError, ValueError):
            pass
        await asyncio.sleep(2)
    raise AssertionError(f"Disposable Vespa did not become ready: {url}")


async def deploy_schema(http):
    """Deploy only to the explicitly disposable engine used by these tests."""
    package = Path(__file__).resolve().parents[6] / "vespa/app"
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as output:
        for path in sorted(package.rglob("*")):
            if path.is_file():
                content = path.read_text().replace("{{EMBEDDING_DIM}}", "384")
                output.writestr(
                    str(path.relative_to(package)), content.replace("{{VERSION}}", "test")
                )
    await wait_healthy(http, "http://localhost:19071/state/v1/health")
    deployed = await http.post(
        "http://localhost:19071/application/v2/tenant/default/prepareandactivate",
        content=archive.getvalue(),
        headers={"Content-Type": "application/zip"},
    )
    assert deployed.status_code == 200, deployed.text
    await wait_healthy(http, "http://localhost:8081/state/v1/health")
