"""Local OCR isolation, bounded input/output, and optional real model qualification."""

import asyncio
import os
from pathlib import Path

import pymupdf
import pytest
from PIL import Image

from airweave.domains.ocr import local, local_worker
from airweave.domains.ocr.local import LocalOcrProvider
from airweave.domains.ocr.protocols import OcrProvider


@pytest.fixture
def models(tmp_path):
    directory = tmp_path / "tessdata"
    directory.mkdir()
    (directory / "eng.traineddata").write_bytes(b"synthetic model for process tests")
    return directory


def pdf(path, *, pages=1, size=300, text="Local OCR test"):
    with pymupdf.open() as document:
        for _ in range(pages):
            page = document.new_page(width=size, height=size)
            page.insert_text((30, 50), text)
        document.save(path)
    return path


def test_configuration_requires_explicit_safe_models(models):
    assert isinstance(LocalOcrProvider(models), OcrProvider)
    for languages in ((), ("../eng",), ("eng+hin",), ("hin",)):
        with pytest.raises(ValueError):
            LocalOcrProvider(models, languages=languages)
    with pytest.raises(ValueError):
        LocalOcrProvider(models / "missing")


async def test_unsupported_input_does_not_start_process(models, tmp_path, monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("Unsupported input started OCR")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden)
    path = tmp_path / "private.txt"
    path.write_text("not an OCR image")
    large = tmp_path / "large.pdf"
    with large.open("wb") as handle:
        handle.truncate(local_worker.MAX_INPUT_BYTES + 1)
    assert await LocalOcrProvider(models).convert_batch([str(path), str(large)]) == {
        str(path): None,
        str(large): None,
    }
    with pytest.raises(ValueError, match="Input size"):
        local_worker.extract(large, models, "eng")


@pytest.mark.parametrize("cancel", [False, True], ids=["timeout", "cancellation"])
async def test_worker_is_killed_reaped_and_scratch_removed(models, tmp_path, monkeypatch, cancel):
    worker = tmp_path / "sleeper.py"
    worker.write_text("import time\ntime.sleep(60)\n")
    monkeypatch.setattr(local, "WORKER_PATH", worker)
    monkeypatch.setattr(local, "OCR_TIMEOUT_SECONDS", 0.1 if not cancel else 120)
    actual_spawn = asyncio.create_subprocess_exec
    started = asyncio.Event()
    processes, outputs = [], []

    async def spawn(*args, **kwargs):
        assert kwargs["stderr"] == asyncio.subprocess.DEVNULL
        assert kwargs["stdout"] == asyncio.subprocess.DEVNULL
        assert args[1] == "-I"
        process = await actual_spawn(*args, **kwargs)
        processes.append(process)
        outputs.append(Path(args[args.index("--output") + 1]))
        started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    path = pdf(tmp_path / "private.pdf")
    task = asyncio.create_task(LocalOcrProvider(models).convert_batch([str(path)]))
    await started.wait()
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        assert await task == {str(path): None}
    assert processes[0].returncode is not None
    assert not outputs[0].parent.exists()


def test_render_limits_and_encryption_fail_before_ocr(tmp_path, models, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Unsafe page reached rendering")

    monkeypatch.setattr(pymupdf.Page, "get_textpage_ocr", forbidden)
    paths = [pdf(tmp_path / "too-many.pdf", pages=201), pdf(tmp_path / "too-big.pdf", size=10000)]
    encrypted = tmp_path / "encrypted.pdf"
    with pymupdf.open() as document:
        document.new_page()
        document.save(
            encrypted, encryption=pymupdf.PDF_ENCRYPT_AES_256, owner_pw="owner", user_pw="secret"
        )
    paths.append(encrypted)
    for path in paths:
        with pytest.raises(ValueError):
            local_worker.extract(path, models, "eng")


def test_multiframe_images_are_not_silently_truncated(tmp_path, models):
    path = tmp_path / "frames.tiff"
    Image.new("RGB", (10, 10)).save(path, save_all=True, append_images=[Image.new("RGB", (10, 10))])
    with pytest.raises(ValueError, match="Multi-frame"):
        local_worker.extract(path, models, "eng")


def test_output_limit_rejects_whole_result(tmp_path, models, monkeypatch):
    def synthetic_ocr(page, *, full, dpi, **kwargs):
        assert full is True and dpi == 150
        return page.get_textpage()

    monkeypatch.setattr(pymupdf.Page, "get_textpage_ocr", synthetic_ocr)
    monkeypatch.setattr(local_worker, "MAX_OUTPUT_BYTES", 3)
    with pytest.raises(ValueError, match="output limit"):
        local_worker.extract(pdf(tmp_path / "text.pdf"), models, "eng")


async def test_real_local_ocr_with_explicit_models(tmp_path):
    directory = os.environ.get("LOCAL_OCR_TEST_TESSDATA")
    if not directory:
        pytest.skip("Set LOCAL_OCR_TEST_TESSDATA to explicitly qualify real local OCR")
    path = tmp_path / "scan.pdf"
    with pymupdf.open() as born_digital:
        page = born_digital.new_page(width=400, height=150)
        page.insert_text((30, 65), "LOCAL OCR ORIGINAL 12345", fontsize=18)
        raster = page.get_pixmap(matrix=pymupdf.Matrix(2, 2)).tobytes("png")
    with pymupdf.open() as scanned:
        page = scanned.new_page(width=400, height=150)
        page.insert_image(page.rect, stream=raster)
        scanned.save(path)
    image_path = tmp_path / "scan.png"
    image_path.write_bytes(raster)
    tiff_path = tmp_path / "scan.tiff"
    with Image.open(image_path) as image:
        image.save(tiff_path)
    result = await LocalOcrProvider(directory).convert_batch(
        [str(path), str(image_path), str(tiff_path)]
    )
    assert "LOCAL OCR ORIGINAL 12345" in result[str(path)]
    assert "LOCAL OCR ORIGINAL 12345" in result[str(image_path)]
    assert "LOCAL OCR ORIGINAL 12345" in result[str(tiff_path)]


async def test_failed_file_does_not_discard_other_results(models, tmp_path, monkeypatch):
    worker = tmp_path / "controlled.py"
    worker.write_text(
        "import pathlib, sys\n"
        "args = dict(zip(sys.argv[1::2], sys.argv[2::2]))\n"
        "if pathlib.Path(args['--input']).name == 'broken.pdf':\n"
        "    print('synthetic private diagnostic', file=sys.stderr)\n"
        "    raise SystemExit(1)\n"
        "pathlib.Path(args['--output']).write_text('Recognized text')\n"
    )
    monkeypatch.setattr(local, "WORKER_PATH", worker)
    paths = [pdf(tmp_path / name) for name in ("first.pdf", "broken.pdf", "last.pdf")]
    actual_spawn = asyncio.create_subprocess_exec
    processes = []

    async def spawn(*args, **kwargs):
        assert all(process.returncode is not None for process in processes)
        process = await actual_spawn(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    assert await LocalOcrProvider(models).convert_batch(list(map(str, paths))) == {
        str(paths[0]): "Recognized text",
        str(paths[1]): None,
        str(paths[2]): "Recognized text",
    }
