"""Publish one bounded staged snapshot using the existing durable native importer."""

import argparse
import asyncio
import ipaddress
import sys
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import httpx
from pydantic import (
    Field,
    HttpUrl,
    SecretStr,
    TypeAdapter,
    ValidationError,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from airweave.domains.native_ingestion.models import NativeModel, NativeSnapshot
from evaluation.native_import import NativePublishError, publish_native

MAX_INPUT_BYTES = 32 * 1024 * 1024
SNAPSHOTS = TypeAdapter(tuple[NativeSnapshot, ...])


class Settings(BaseSettings):
    """Read authentication only from the process environment, never dotenv files."""

    model_config = SettingsConfigDict(env_prefix="AIRWEAVE_", extra="ignore")
    api_key: SecretStr

    @field_validator("api_key")
    @classmethod
    def valid_key(cls, value: SecretStr) -> SecretStr:
        """Require a nonempty printable header value without exposing it."""
        key = value.get_secret_value()
        if not key or any(ord(char) < 33 or ord(char) > 126 for char in key):
            raise ValueError("Invalid API key")
        return value


class PublishArguments(NativeModel):
    """CLI input boundary; all state and retry decisions belong to the publisher."""

    url: str
    input: Path
    owner: str = Field(min_length=1)
    dataset: Literal["knowledge", "sessions"]
    collection: str = Field(min_length=1, max_length=255)
    request_key: str = Field(min_length=1, max_length=128)

    @field_validator("url")
    @classmethod
    def destination_url(cls, value: str) -> str:
        """Allow explicit TLS destinations and local HTTP evaluation only."""
        if any(char.isspace() or ord(char) < 32 for char in value):
            raise ValueError("Invalid destination")
        parsed = urlsplit(value)
        # Reject even empty query/fragment delimiters and URL-carried credentials.
        if any(mark in value for mark in ("?", "#")) or parsed.username is not None:
            raise ValueError(
                "Destination must not include credentials, query or fragment"
            )
        url = TypeAdapter(HttpUrl).validate_python(value)
        if url.username is not None or url.password is not None:
            raise ValueError("Destination must not include credentials")
        if url.scheme == "http":
            hostname = parsed.hostname or ""
            try:
                loopback = ipaddress.ip_address(hostname).is_loopback
            except ValueError:
                loopback = hostname == "localhost"
            if not loopback:
                raise ValueError("HTTP requires a loopback destination")
        return str(url).rstrip("/") + "/"


class Parser(argparse.ArgumentParser):
    """Argument errors do not echo potentially sensitive caller input."""

    def error(self, message: str) -> None:
        """Print static usage guidance instead of the original parser diagnostic."""
        self.exit(2, "Invalid arguments. Use --help for the accepted options.\n")


def parse_args(argv: list[str] | None) -> PublishArguments:
    """Parse explicit publisher options into their validated boundary."""
    parser = Parser(description=__doc__)
    parser.add_argument(
        "--url", required=True, help="API root, e.g. https://host/api/v1/"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="JSON array of NativeSnapshot values (max 32 MiB)",
    )
    parser.add_argument("--owner", required=True)
    parser.add_argument("--dataset", required=True, choices=("knowledge", "sessions"))
    parser.add_argument("--collection", required=True)
    parser.add_argument(
        "--request-key",
        required=True,
        help="Retain this key and exact input for retries",
    )
    return PublishArguments.model_validate(vars(parser.parse_args(argv)))


async def publish(arguments: PublishArguments, settings: Settings) -> str:
    """Read bounded originals then delegate the entire workflow to the publisher."""
    with arguments.input.open("rb") as staged:
        data = staged.read(MAX_INPUT_BYTES + 1)
    if len(data) > MAX_INPUT_BYTES:
        raise ValueError("Staged input exceeds the size limit")
    snapshots = SNAPSHOTS.validate_json(data)
    async with httpx.AsyncClient(
        base_url=arguments.url,
        headers={"X-API-Key": settings.api_key.get_secret_value()},
        follow_redirects=False,
        trust_env=False,
        timeout=httpx.Timeout(60, connect=10),
    ) as client:
        state = await publish_native(
            client,
            owner_id=arguments.owner,
            dataset=arguments.dataset,
            collection=arguments.collection,
            request_key=arguments.request_key,
            snapshots=snapshots,
        )
    # The publisher attests this summary; never turn absent progress into success.
    if state.summary is None:
        raise NativePublishError("Destination returned no capture summary")
    return state.summary.model_dump_json()


def main(argv: list[str] | None = None) -> int:
    """Emit only the capture summary or a sanitized diagnostic with a stable exit code."""
    try:
        arguments = parse_args(argv)
        settings = Settings()
        result = asyncio.run(publish(arguments, settings))
    except NativePublishError:
        print(
            "Publication rejected or progress invalid. Retain input and request key; "
            "inspect destination state.",
            file=sys.stderr,
        )
        return 4
    except httpx.HTTPError:
        print(
            "Network outcome unknown. Retry only with the identical staged input and request key.",
            file=sys.stderr,
        )
        return 3
    except (ValidationError, ValueError, OSError):
        print(
            "Invalid configuration or staged input. "
            "Check credentials, destination and JSON (maximum 32 MiB).",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print(
            "Interrupted; outcome may be unknown. Retain the exact staged input and request key.",
            file=sys.stderr,
        )
        return 130
    except Exception:
        print(
            "Unexpected failure; outcome may be unknown. "
            "Retain the exact staged input and request key.",
            file=sys.stderr,
        )
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
