"""SQL metadata inventory; originals remain canonical, indexes remain optional."""

from uuid import UUID

from jose import JWTError, jwt
from pydantic import ValidationError
from sqlalchemy import BigInteger, and_, case, cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONPATH
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.drive_models import (
    DriveCursor,
    DriveListQuery,
    DriveMetadata,
    DriveMetadataPage,
    DriveMetadataRead,
)
from airweave.domains.entities.canonical.query import InvalidRecordCursor, RecordNotFound
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    SourceNotFound,
    content_is_available,
)
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class DriveChanged(CanonicalStoreError):
    """Never mix revisions across a metadata traversal."""

    code = "drive_changed_restart"


class DriveMetadataUnavailable(CanonicalStoreError):
    """A captured identity does not imply complete native metadata."""

    code = "drive_metadata_unavailable"


class CanonicalDriveQuery:
    """Reuse canonical entities, source gates and current projection evidence."""

    def __init__(self, signing_key: str):
        """Use the existing canonical cursor signing key."""
        self.signing_key = signing_key

    async def _sequence(self, db: AsyncSession, organization: UUID, sync: UUID) -> int:
        sequence = await db.scalar(
            select(Sync.observed_change_sequence).where(
                Sync.id == sync,
                Sync.organization_id == organization,
                source_is_readable(organization, sync),
                select(SourceConnection.id)
                .where(
                    SourceConnection.organization_id == organization,
                    SourceConnection.sync_id == sync,
                    SourceConnection.short_name == "google_drive",
                )
                .exists(),
            )
        )
        if sequence is None:
            raise SourceNotFound("Drive source is unavailable in this organization")
        return sequence

    @staticmethod
    def _scope(organization: UUID, sync: UUID):
        return and_(
            Entity.organization_id == organization,
            Entity.sync_id == sync,
            Entity.entity_definition_short_name == "file",
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
            source_is_readable(organization, sync),
        )

    @staticmethod
    def _facts():
        payload = Entity.source_payload
        parents = payload["parents"]
        parents_valid = and_(
            func.jsonb_typeof(parents) == "array",
            ~func.jsonb_path_exists(parents, cast('$[*] ? (@.type() != "string")', JSONPATH)),
        )
        required = and_(
            func.jsonb_typeof(payload["id"]) == "string",
            payload["id"].astext == Entity.native_id,
            Entity.native_id.op("~")(r"^[A-Za-z0-9_-]+$"),
            func.length(Entity.native_id) <= 256,
            func.jsonb_typeof(payload["name"]) == "string",
            func.jsonb_typeof(payload["mimeType"]) == "string",
            payload["mimeType"].astext.op("~")(r"^[\w.+-]+/[\w.+-]+$"),
            func.jsonb_typeof(payload["trashed"]) == "boolean",
            payload["trashed"].astext == "false",
        )
        return required, parents_valid

    def _statement(self, organization: UUID, sync: UUID):
        payload = Entity.source_payload
        required, parents_valid = self._facts()
        description = case(
            (func.jsonb_typeof(payload["description"]) == "string", payload["description"].astext),
            else_=None,
        )
        size = case(
            (
                payload["size"].astext.op("~")(r"^[0-9]{1,18}$"),
                cast(payload["size"].astext, BigInteger),
            ),
            else_=None,
        )
        generation_current = and_(
            ProjectionGeneration.id == Entity.indexed_generation,
            ProjectionGeneration.organization_id == organization,
            ProjectionGeneration.record_id == Entity.id,
            ProjectionGeneration.revision == Entity.record_revision,
            ProjectionGeneration.pipeline_version == Sync.index_pipeline_version,
            ProjectionGeneration.retired_at.is_(None),
            Entity.indexed_revision == Entity.record_revision,
            Entity.indexed_pipeline_version == Sync.index_pipeline_version,
        )
        return (
            select(
                Entity.id,
                Entity.sync_id,
                Entity.record_revision.label("revision"),
                Entity.native_id,
                payload["name"].astext.label("name"),
                payload["mimeType"].astext.label("mime_type"),
                description.label("description"),
                size.label("size_bytes"),
                case((parents_valid, payload["parents"]), else_=None).label("parents"),
                case(
                    (func.jsonb_typeof(payload["driveId"]) == "string", payload["driveId"].astext),
                    else_=None,
                ).label("drive_id"),
                Entity.source_created_at,
                Entity.source_updated_at,
                Entity.observed_at,
                Entity.completeness,
                ProjectionGeneration.extraction_coverage.label("extraction"),
            )
            .join(Sync, Sync.id == Entity.sync_id)
            .outerjoin(ProjectionGeneration, generation_current)
            .where(self._scope(organization, sync), required)
        )

    @staticmethod
    def _metadata(row) -> DriveMetadata:
        return DriveMetadata.model_validate({**row, "trashed": False})

    async def files(
        self, db: AsyncSession, organization: UUID, sync: UUID, query: DriveListQuery
    ) -> DriveMetadataPage:
        """Filter before pagination and fence capture changes across every page."""
        cursor = None
        if query.cursor is not None:
            try:
                cursor = DriveCursor.model_validate(
                    jwt.decode(query.cursor, self.signing_key, algorithms=["HS256"])
                )
            except (JWTError, ValidationError, ValueError) as error:
                raise InvalidRecordCursor("Invalid Drive cursor; restart the list") from error
            if (
                cursor.organization_id != organization
                or cursor.sync_id != sync
                or cursor.filters != query.filters
            ):
                raise InvalidRecordCursor("Continue with the same Drive account, filters and sort")
        sequence = await self._sequence(db, organization, sync)
        if cursor is not None and cursor.sequence != sequence:
            raise DriveChanged("Captured Drive changed; restart the list")
        statement, facts = self._filtered(organization, sync, query)
        statement = self._ordered(statement, query, cursor)
        rows = tuple((await db.execute(statement.limit(query.limit + 1))).mappings())
        missing = await db.scalar(
            select(func.count())
            .select_from(Entity)
            .where(self._scope(organization, sync), ~func.coalesce(facts, False))
        )
        capture = (await capture_coverage(db, organization, (sync,))).get(sync)
        if await self._sequence(db, organization, sync) != sequence:
            raise DriveChanged("Captured Drive changed during this page; restart the list")
        files = tuple(self._metadata(row) for row in rows[: query.limit])
        more = len(rows) > query.limit
        token = None
        if more:
            last = files[-1]
            token = jwt.encode(
                DriveCursor(
                    organization_id=organization,
                    sync_id=sync,
                    filters=query.filters,
                    sequence=sequence,
                    after_name=last.name,
                    after_updated_at=last.source_updated_at,
                    after_id=last.id,
                ).model_dump(mode="json"),
                self.signing_key,
                algorithm="HS256",
            )
        return DriveMetadataPage(
            files=files,
            next_cursor=token,
            has_more=more,
            capture=capture,
            metadata_missing=missing or 0,
            order=(
                "name_asc_id_asc"
                if query.filters.sort == "name"
                else "updated_desc_nulls_last_id_asc"
            ),
        )

    def _filtered(self, organization: UUID, sync: UUID, query: DriveListQuery):
        statement = self._statement(organization, sync)
        required, parents_valid = self._facts()
        payload = Entity.source_payload
        facts = required
        if query.filters.folder is not None:
            facts = and_(facts, parents_valid)
            statement = statement.where(
                parents_valid, payload["parents"].contains([query.filters.folder])
            )
        if query.filters.drive is not None:
            statement = statement.where(payload["driveId"].astext == query.filters.drive)
        if query.filters.name is not None:
            statement = statement.where(
                func.strpos(func.lower(payload["name"].astext), query.filters.name.lower()) > 0
            )
        if query.filters.mime_type is not None:
            statement = statement.where(payload["mimeType"].astext == query.filters.mime_type)
        if query.filters.updated_after is not None:
            statement = statement.where(Entity.source_updated_at >= query.filters.updated_after)
        if query.filters.updated_before is not None:
            statement = statement.where(Entity.source_updated_at < query.filters.updated_before)
        return statement, facts

    @staticmethod
    def _ordered(statement, query: DriveListQuery, cursor: DriveCursor | None):
        payload = Entity.source_payload
        name = payload["name"].astext.collate("C")
        updated = Entity.source_updated_at
        if query.filters.sort == "name":
            if cursor is not None:
                statement = statement.where(
                    or_(
                        name > cursor.after_name,
                        and_(name == cursor.after_name, Entity.id > cursor.after_id),
                    )
                )
            statement = statement.order_by(name.asc(), Entity.id.asc())
        else:
            if cursor is not None:
                if cursor.after_updated_at is None:
                    statement = statement.where(updated.is_(None), Entity.id > cursor.after_id)
                else:
                    statement = statement.where(
                        or_(
                            updated < cursor.after_updated_at,
                            updated.is_(None),
                            and_(updated == cursor.after_updated_at, Entity.id > cursor.after_id),
                        )
                    )
            statement = statement.order_by(updated.desc().nulls_last(), Entity.id.asc())
        return statement

    async def file(
        self, db: AsyncSession, organization: UUID, sync: UUID, native_id: str
    ) -> DriveMetadataRead:
        """Resolve an exact native identity without a provider or an inventory scan."""
        sequence = await self._sequence(db, organization, sync)
        row = (
            (
                await db.execute(
                    self._statement(organization, sync).where(Entity.native_id == native_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            exists = await db.scalar(
                select(Entity.id).where(
                    self._scope(organization, sync), Entity.native_id == native_id
                )
            )
            if exists is not None:
                raise DriveMetadataUnavailable(
                    "This saved file has incomplete metadata; use its original record read"
                )
            raise RecordNotFound("File is not available in this captured Drive")
        capture = (await capture_coverage(db, organization, (sync,))).get(sync)
        if await self._sequence(db, organization, sync) != sequence:
            raise DriveChanged("Captured Drive changed during this read; read it again")
        return DriveMetadataRead(file=self._metadata(row), capture=capture)
