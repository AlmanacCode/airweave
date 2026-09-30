"""Private subprocess read of the real stored-document HTTP route; no provider keys."""

import asyncio
import hashlib
import json
import logging
import sys
from pathlib import Path
from uuid import uuid4

import httpx

logging.disable(logging.CRITICAL)


async def main():
    path = Path(sys.argv[1]).resolve()
    if path.stat().st_mode & 0o077 or path.parent.stat().st_mode & 0o077:
        raise ValueError("Private handoff permissions required")
    config = json.loads(path.read_text())
    endpoint = f"/sync/{config['sync_id']}/records/{config['record_id']}/document"
    async with httpx.AsyncClient(
        base_url=config["origin"],
        headers={"x-api-key": config["api_key"]},
        timeout=30,
        trust_env=False,
        follow_redirects=False,
    ) as client:
        response = await client.get(endpoint, params={"revision": config["revision"]})
        response.raise_for_status()
        result = response.json()
        assert result["id"] == config["record_id"] and result["revision"] == config["revision"]
        encoded = json.dumps(
            result["document"],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
        assert hashlib.sha256(encoded).hexdigest() == config["expected"]
        assert result["manifest"]["file_id"] == result["document"]["documentId"]
        assert result["manifest"]["native"]["status"] == "complete"
        assert response.headers["cache-control"] == "private, no-store"
        stale = await client.get(endpoint, params={"revision": config["revision"] + 1})
        assert stale.status_code == 409
        foreign = await client.get(
            f"/sync/{uuid4()}/records/{config['record_id']}/document",
            params={"revision": config["revision"]},
        )
        assert foreign.status_code == 404
    print(
        json.dumps(
            {
                "verified": True,
                "native_document_digest_verified": True,
                "identity_verified": True,
                "stale_revision_denied": True,
                "wrong_source_denied": True,
                "top_level_tabs": len(result["document"]["tabs"]),
                "scope": "stored_document_http_route_with_fixture_auth",
            }
        )
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        print(json.dumps({"verified": False, "stage": "exact_record_read"}))
        raise SystemExit(1) from None
