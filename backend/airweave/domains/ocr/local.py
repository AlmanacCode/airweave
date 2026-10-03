"""Optional local OCR provider with one bounded, disposable process per input."""

import asyncio
import hashlib
import re
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from airweave.domains.entities.canonical.preparation_recipe import ModelIdentity
from airweave.domains.ocr.local_worker import (
    MAX_INPUT_BYTES,
    MAX_OUTPUT_BYTES,
    SUPPORTED_EXTENSIONS,
)

OCR_TIMEOUT_SECONDS = 120
WORKER_PATH = Path(__file__).with_name("local_worker.py")


async def _stop(process: asyncio.subprocess.Process) -> None:
    """Reap the worker after killing it, including a process that exited concurrently."""
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    await process.wait()


class LocalOcrProvider:
    """Use explicitly provisioned Tesseract language files without runtime downloads."""

    def __init__(self, tessdata_path: str | Path, languages: tuple[str, ...] = ("eng",)) -> None:
        """Fail configuration early for absent models or unsafe language selectors."""
        directory = Path(tessdata_path).resolve()
        if not directory.is_dir() or not languages:
            raise ValueError("Local OCR requires a tessdata directory and languages")
        models = []
        for language in languages:
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", language):
                raise ValueError("Invalid local OCR language selector")
            model = directory / f"{language}.traineddata"
            if not model.is_file() or model.stat().st_size == 0:
                raise ValueError("Local OCR language model is unavailable")
            try:
                with model.open("rb") as handle:
                    digest = hashlib.file_digest(handle, "sha256").hexdigest()
            except OSError:
                raise ValueError("Local OCR language model is unreadable") from None
            models.append(ModelIdentity(identifier=language, sha256=digest, resolution="digest"))
        self._tessdata = directory
        self._languages = "+".join(languages)
        self.model_artifacts = tuple(models)

    async def convert_batch(self, file_paths: list[str]) -> dict[str, str | None]:
        """Process sequentially; each failed file remains available to the fallback chain."""
        results = {}
        for path in file_paths:
            results[path] = await self._convert(path)
        return results

    async def _convert(self, file_path: str) -> str | None:
        try:
            path = Path(file_path).resolve()
            if (
                path.suffix.lower() not in SUPPORTED_EXTENSIONS
                or not path.is_file()
                or path.stat().st_size > MAX_INPUT_BYTES
            ):
                return None
        except (OSError, RuntimeError):
            return None
        with TemporaryDirectory(prefix="airweave-local-ocr-") as directory:
            output = Path(directory) / "text.txt"
            spawn = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    str(WORKER_PATH),
                    "--input",
                    str(path),
                    "--output",
                    str(output),
                    "--tessdata",
                    str(self._tessdata),
                    "--languages",
                    self._languages,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
            )
            try:
                process = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                process = await spawn
                await _stop(process)
                raise
            except OSError:
                return None
            try:
                await asyncio.wait_for(process.wait(), timeout=OCR_TIMEOUT_SECONDS)
                if process.returncode != 0:
                    return None
                with output.open("rb") as handle:
                    data = handle.read(MAX_OUTPUT_BYTES + 1)
                if len(data) > MAX_OUTPUT_BYTES:
                    return None
                return data.decode("utf-8").strip() or None
            except (OSError, UnicodeDecodeError, TimeoutError):
                return None
            finally:
                await _stop(process)
