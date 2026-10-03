"""Self-authored Foundation archives; no private Messages data or native decoding."""

import asyncio
import base64
import hashlib
import sys
from pathlib import Path

import pytest

from airweave.domains.entities.canonical.apple_preparation import (
    ApplePreparationError,
    PreparationLimits,
    prepare_message_body,
)
from airweave.domains.entities.canonical.apple_preparation.messages import decode_message
from airweave.domains.entities.canonical.apple_preparation.models import BodyDecodeRequest

FIXTURES = Path(__file__).with_name("fixtures")
TEXT = "Hello — مرحبا — हिन्दी — 👩🏽‍💻"


@pytest.fixture
def limits():
    return PreparationLimits(
        maximum_input_bytes=128 * 1024,
        maximum_decompressed_bytes=256 * 1024,
        maximum_text_bytes=128 * 1024,
        cpu_seconds=3,
        wall_seconds=10,
        memory_bytes=256 * 1024 * 1024,
    )


def prepare(body, limits):
    return asyncio.run(prepare_message_body(body, limits=limits))


@pytest.mark.parametrize("name", ["attributed-body.bin", "mutable-attributed-body.bin"])
def test_foundation_archive_exact_text_and_explicit_omissions(name, limits):
    original = (FIXTURES / name).read_bytes()
    preserved = original[:]
    result = prepare(original, limits)
    assert result.text == TEXT
    assert result.format == "messages_typedstream"
    assert result.decoder_version == "pytypedstream-0.1.0-backing-string-v1"
    assert result.fidelity == "plain_text_only"
    assert result.omissions == ("rich_formatting", "embedded_content")
    assert result.original_sha256 == hashlib.sha256(original).hexdigest()
    assert original == preserved
    assert result.cpu_limit_enforced is True
    assert result.memory_limit_enforced is sys.platform.startswith("linux")
    assert result.os_sandboxed is False


def test_unsupported_native_root_is_not_searched_for_text(limits):
    with pytest.raises(ApplePreparationError, match="unsupported_typedstream_root"):
        prepare((FIXTURES / "unsupported-string-root.bin").read_bytes(), limits)


def test_every_truncated_prefix_is_rejected_by_library_adapter(limits):
    original = (FIXTURES / "attributed-body.bin").read_bytes()
    for length in range(len(original)):
        request = BodyDecodeRequest(
            format="messages_typedstream",
            original_base64=base64.b64encode(original[:length]).decode("ascii"),
            limits=limits,
        )
        with pytest.raises(ApplePreparationError):
            decode_message(request, cpu_enforced=False, memory_enforced=False)


def test_worker_rejects_bad_truncated_and_trailing_archives(limits):
    original = (FIXTURES / "attributed-body.bin").read_bytes()
    for invalid in (b"not an archive", original[:-1], original + b"trailing"):
        with pytest.raises(ApplePreparationError, match="invalid_typedstream"):
            prepare(invalid, limits)


def test_message_text_budget(limits):
    original = (FIXTURES / "attributed-body.bin").read_bytes()
    with pytest.raises(ApplePreparationError, match="text_limit"):
        prepare(original, limits.model_copy(update={"maximum_text_bytes": 4}))


def test_input_budget_rejected_before_spawn(monkeypatch, limits):
    async def no_spawn(*args, **kwargs):
        raise AssertionError("Input over budget entered parser process")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", no_spawn)
    with pytest.raises(ApplePreparationError, match="input_limit"):
        prepare(b"x" * (limits.maximum_input_bytes + 1), limits)


def test_unqualified_parser_version_fails_closed(monkeypatch, limits):
    from airweave.domains.entities.canonical.apple_preparation import messages

    monkeypatch.setattr(messages, "version", lambda name: "9.0.0")
    request = BodyDecodeRequest(
        format="messages_typedstream",
        original_base64=base64.b64encode((FIXTURES / "attributed-body.bin").read_bytes()).decode("ascii"),
        limits=limits,
    )
    with pytest.raises(ApplePreparationError, match="unsupported_parser_version"):
        decode_message(request, cpu_enforced=False, memory_enforced=False)
