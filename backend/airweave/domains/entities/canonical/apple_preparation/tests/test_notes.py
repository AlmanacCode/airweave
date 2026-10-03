"""Actual subprocess and native-format decoding; no provider content/access involved."""

import asyncio
import gzip
import hashlib
import os
import sys
from pathlib import Path

import pytest

from airweave.domains.entities.canonical.apple_preparation import (
    ApplePreparationError, PreparationLimits, prepare_notes_body,
)
from airweave.domains.entities.canonical.apple_preparation.notestore_pb2 import NoteStoreProto


@pytest.fixture
def limits():
    return PreparationLimits(
        maximum_input_bytes=128 * 1024, maximum_decompressed_bytes=256 * 1024,
        maximum_text_bytes=128 * 1024, cpu_seconds=3, wall_seconds=10,
        memory_bytes=256 * 1024 * 1024,
    )


def native_body(text):
    note = NoteStoreProto()
    note.document.version = 1
    note.document.note.note_text = text
    return gzip.compress(note.SerializeToString(), mtime=0)


def prepare(body, limits, *, locked=False):
    return asyncio.run(prepare_notes_body(body, locked=locked, limits=limits))


def test_multilingual_exact_original_and_fidelity(limits):
    text = "مرحبا — हिन्दी — 👩🏽‍💻\nsecond line\ufffc"
    original = native_body(text)
    preserved = original[:]
    result = prepare(original, limits)
    assert result.text == text
    assert original == preserved
    assert result.original_sha256 == hashlib.sha256(original).hexdigest()
    assert result.fidelity == "plain_text_only"
    assert result.omissions == ("rich_formatting", "embedded_content")
    assert result.cpu_limit_enforced is True
    assert result.memory_limit_enforced is sys.platform.startswith("linux")
    assert result.os_sandboxed is False


def test_native_format_fixture(limits):
    directory = Path(__file__).with_name("fixtures")
    simple = prepare((directory / "simple_note_protobuf_gzipped.bin").read_bytes(), limits)
    assert simple.text == "Title"
    wide = prepare((directory / "wide_characters_gzipped.bin").read_bytes(), limits)
    assert len(wide.text) == 55
    assert any(ord(char) > 127 for char in wide.text)


def test_locked_rejected_before_process(monkeypatch, limits):
    async def no_process(*args, **kwargs):
        raise AssertionError("Locked content entered parser process")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", no_process)
    with pytest.raises(ApplePreparationError, match="locked_content"):
        prepare(b"stale plaintext", limits, locked=True)


def test_gzip_rejects_truncation_and_trailing_members(limits):
    original = native_body("hello")
    for invalid in (original[:-1], original + original, b"not gzip"):
        with pytest.raises(ApplePreparationError, match="invalid_gzip"):
            prepare(invalid, limits)


def test_expansion_and_text_limits(limits):
    bomb = gzip.compress(b"X" * (limits.maximum_decompressed_bytes + 1))
    with pytest.raises(ApplePreparationError, match="decompressed_limit"):
        prepare(bomb, limits)
    smaller = limits.model_copy(update={"maximum_text_bytes": 4})
    with pytest.raises(ApplePreparationError, match="text_limit"):
        prepare(native_body("Hello"), smaller)


def test_protobuf_required_fields_and_bad_wire(limits):
    for invalid in (gzip.compress(b""), gzip.compress(b"\xff")):
        with pytest.raises(ApplePreparationError, match="invalid_protobuf"):
            prepare(invalid, limits)


def test_input_limit_before_process(monkeypatch, limits):
    async def no_process(*args, **kwargs):
        raise AssertionError("Oversized input entered parser process")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", no_process)
    with pytest.raises(ApplePreparationError, match="input_limit"):
        prepare(b"X" * (limits.maximum_input_bytes + 1), limits)


def test_child_environment_strips_provider_credentials(monkeypatch, limits):
    real_spawn = asyncio.create_subprocess_exec
    observed = []
    monkeypatch.setenv("WORKOS_API_KEY", "fixture-only-secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fixture-only-secret")

    async def observe(*args, **kwargs):
        observed.append(kwargs["env"])
        assert "-I" in args
        assert "WORKOS_API_KEY" not in kwargs["env"]
        assert "AWS_SECRET_ACCESS_KEY" not in kwargs["env"]
        return await real_spawn(*args, **kwargs)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", observe)
    assert prepare(native_body("hello"), limits).text == "hello"
    assert observed == [{"PATH": os.defpath, "LANG": "C.UTF-8"}]


def test_wall_timeout_is_reaped(monkeypatch, limits):
    real_spawn = asyncio.create_subprocess_exec
    observed = []

    async def sleepy(*args, **kwargs):
        process = await real_spawn(sys.executable, "-I", "-c", "import time; time.sleep(30)", **kwargs)
        observed.append(process)
        return process
    monkeypatch.setattr(asyncio, "create_subprocess_exec", sleepy)
    short = limits.model_copy(update={"wall_seconds": 0.05})
    with pytest.raises(ApplePreparationError, match="worker_timeout"):
        prepare(native_body("hello"), short)
    assert observed[0].returncode is not None


def test_excess_worker_output_is_killed_and_reaped(monkeypatch, limits):
    real_spawn = asyncio.create_subprocess_exec
    observed = []

    async def noisy(*args, **kwargs):
        process = await real_spawn(
            sys.executable, "-I", "-c",
            "import os,time; os.write(1,b'X'*100000); time.sleep(30)", **kwargs
        )
        observed.append(process)
        return process
    monkeypatch.setattr(asyncio, "create_subprocess_exec", noisy)
    short = limits.model_copy(update={"maximum_text_bytes": 4})
    with pytest.raises(ApplePreparationError, match="worker_output_limit"):
        prepare(native_body("hi"), short)
    assert observed[0].returncode is not None


def test_cancellation_reaps_child(monkeypatch, limits):
    real_spawn = asyncio.create_subprocess_exec
    observed = []

    async def scenario():
        started = asyncio.Event()

        async def sleepy(*args, **kwargs):
            process = await real_spawn(
                sys.executable, "-I", "-c", "import time; time.sleep(30)", **kwargs
            )
            observed.append(process)
            started.set()
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", sleepy)
        task = asyncio.create_task(prepare_notes_body(native_body("hello"), locked=False, limits=limits))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert observed[0].returncode is not None

    asyncio.run(scenario())
