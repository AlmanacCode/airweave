"""Real SQL publication/revision/access gates over native spreadsheet blobs."""

from unittest.mock import MagicMock

import httpx
import pytest

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.projection_mappers import _drive
from airweave.domains.entities.canonical.query import (
    CanonicalQueryService,
    RecordNotFound,
    StaleRecordRevision,
)
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.file_service import FileService
from airweave.platform.sources.records.google_drive import file_record
from airweave.platform.sources.records.google_drive_content import capture_file_content
from airweave.platform.sources.records.sheets_manifest import SHEETS_MIME, GridBounds


async def test_sheets_exact_revision_projection_and_removal(
    database, source, tmp_path, monkeypatch
):
    service, fence = source
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir", lambda _: str(tmp_path / "tmp")
    )
    files = FileService(fence.job_id, FilesystemBackend(tmp_path / "store"), sync_id=fence.sync_id)
    grid = {
        "spreadsheetId": "book",
        "sheets": [
            {
                "properties": {
                    "sheetId": 0,
                    "title": "Sheet",
                    "gridProperties": {"rowCount": 2, "columnCount": 2},
                }
            }
        ],
    }

    async def latest(*args, **kwargs):
        return {"version": "7"}

    def response(request):
        if request.url.path.endswith("/export"):
            return httpx.Response(200, content=b"exact-export")
        if "ranges" in request.url.params:
            return httpx.Response(
                200,
                json={
                    "spreadsheetId": "book",
                    "sheets": [
                        {
                            **grid["sheets"][0],
                            "data": [
                                {
                                    "rowData": [
                                        {
                                            "values": [
                                                {
                                                    "formattedValue": "42",
                                                    "userEnteredValue": {"formulaValue": "=6*7"},
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ],
                        }
                    ],
                },
            )
        return httpx.Response(200, json=grid)

    original = file_record({"id": "book", "version": "7", "mimeType": SHEETS_MIME})
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        retained = await capture_file_content(
            original,
            files=files,
            get=latest,
            client=client,
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            capture_native_sheets=True,
        )
    record = (await capture(database, service, fence, retained)).changes[0].record
    query = CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing"
    )
    async with database() as db:
        read = await query.spreadsheet(
            db,
            fence.organization_id,
            fence.sync_id,
            record.id,
            record.revision,
            files.storage,
            GridBounds(sheet_id=0, start_row=0, end_row=2, start_column=0, end_column=2),
        )
        assert len(read.cells) == 1
        assert read.cells[0].value["userEnteredValue"] == {"formulaValue": "=6*7"}
        assert read.grid_status == "complete" and not read.missing
        with pytest.raises(StaleRecordRevision):
            await query.spreadsheet(
                db,
                fence.organization_id,
                fence.sync_id,
                record.id,
                record.revision + 1,
                files.storage,
            )
    entities = (await _drive(record, files.storage, tmp_path / "projection")).entities
    assert len(entities) == 1
    from pathlib import Path

    assert "42" in Path(entities[0].local_path).read_text()
    assert "=6*7" not in Path(entities[0].local_path).read_text()
    monkeypatch.setattr(
        "airweave.platform.sources.records.google_sheets_content.MAX_GRID_REQUESTS", 0
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        partial = await capture_file_content(
            original,
            files=files,
            get=latest,
            client=client,
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            capture_native_sheets=True,
        )
    newer = (await capture(database, service, fence, partial)).changes[0].record
    assert newer.id == record.id and newer.revision == record.revision + 1
    projection = await _drive(newer, files.storage, tmp_path / "fallback")
    fallback = projection.entities
    assert len(projection.parts) == 2
    assert projection.parts[1].entity is None
    assert projection.parts[1].part.key == "/native/sheets/0/rows/0:2/columns/0:2"
    assert newer.completeness == "partial"
    assert len(fallback) == 1
    assert Path(fallback[0].local_path).read_bytes() == b"exact-export"
    assert fallback[0].file_type == "xlsx"
    async with database() as db:
        native_partial = await query.spreadsheet(
            db, fence.organization_id, fence.sync_id, newer.id, newer.revision, files.storage
        )
        assert native_partial.grid_status == "partial" and len(native_partial.missing) == 1
        assert not native_partial.captured and not native_partial.cells
    await capture(
        database,
        service,
        fence,
        original.model_copy(update={"kind": "delete", "removal_reason": "scope_removed"}),
    )
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await query.spreadsheet(
                db, fence.organization_id, fence.sync_id, record.id, record.revision, files.storage
            )


async def test_failed_native_tile_retries_same_file_without_advancing_checkpoint(
    database, source, tmp_path, monkeypatch
):
    from contextlib import aclosing

    from sqlalchemy import select

    from airweave.domains.entities.canonical.tests import test_drive_recovery as recovery
    from airweave.domains.entities.canonical.tests.test_workspace_capture import NativeDocs
    from airweave.models.entity import Entity
    from airweave.platform.configs.config import GoogleDriveConfig

    class NativeSheets(NativeDocs):
        async def handle(self, request):
            if request.url.host == "sheets.googleapis.com":
                value = {
                    "spreadsheetId": "doc",
                    "sheets": [
                        {
                            "properties": {
                                "sheetId": 0,
                                "title": "Sheet",
                                "gridProperties": {"rowCount": 1, "columnCount": 1},
                            }
                        }
                    ],
                }
                if "ranges" in request.url.params and self.fail:
                    raise ConnectionError("interrupted native tile")
                return httpx.Response(200, json=value)
            response = await super().handle(request)
            if request.url.path.endswith("/files/doc"):
                return httpx.Response(
                    200, json={"id": "doc", "version": "7", "mimeType": SHEETS_MIME}
                )
            return response

    monkeypatch.setattr(
        recovery,
        "GoogleDriveConfig",
        lambda **kwargs: GoogleDriveConfig(capture_native_sheets=True, **kwargs),
    )
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir", lambda _: str(tmp_path / "tmp")
    )
    service, fence = source
    files = FileService(fence.job_id, FilesystemBackend(tmp_path / "store"), sync_id=fence.sync_id)
    native = NativeSheets()
    pipeline, ctx, runtime, client = await recovery.setup(database, source, native)
    pipeline.files = files
    async with aclosing(client):
        with pytest.raises(ConnectionError, match="interrupted native tile"):
            await recovery.run(pipeline, ctx, runtime)
    async with database() as db:
        assert not (await db.scalars(select(Entity))).all()
        cycle = await service.read_cycle(db, pipeline._writer())
        assert cycle.promoted_checkpoint is None
    native.fail = False
    pipeline, ctx, runtime, client = await recovery.setup(database, source, native, attempt=2)
    pipeline.files = files
    async with aclosing(client):
        await recovery.run(pipeline, ctx, runtime)
    async with database() as db:
        records = (await db.scalars(select(Entity))).all()
        assert len(records) == 1
        cycle = await service.read_cycle(db, pipeline._writer())
        assert cycle.promoted_checkpoint.checkpoint.value == {"page_token": "end"}
