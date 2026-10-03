# ruff: noqa: E402
"""Opt-in retained local evaluation, using production capture, index and HTTP services."""

import argparse
import asyncio
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import UUID, uuid4

os.environ.update(
    DENSE_EMBEDDER="local_minilm",
    EMBEDDING_DIMENSIONS="384",
    SPARSE_EMBEDDER="fastembed_bm25",
    TEXT2VEC_INFERENCE_URL="http://127.0.0.1:9878",
    REDIS_PORT="16379",
    DISABLE_RATE_LIMIT="true",
)

import canonical_capture as capture  # noqa: E402
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.core import credentials
from airweave.core.config import AuthMode, settings
from airweave.core.datetime_utils import utc_now_naive
from airweave.core.logging import logger
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.search_metadata import SEARCH_METADATA_PIPELINE_VERSION
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models import Entity, Organization, Sync
from airweave.models.api_key import APIKey
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata
from airweave.platform.configs.config import (
    GmailConfig,
    GoogleCalendarConfig,
    GoogleDriveConfig,
    SlackConfig,
)
from airweave.platform.destinations.vespa.destination import VespaDestination


class Source(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str
    account_id: UUID = Field(default_factory=uuid4)
    organization_id: UUID
    sync_id: UUID
    source_connection_id: UUID
    external_account_id: str
    external_user_id: str | None = None
    label: str
    api_key: SecretStr


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_name: str = Field(pattern=r"^canonical_eval_[a-f0-9]{32}$")
    sources: list[Source] = Field(default_factory=list)
    encryption_key: SecretStr
    state_secret: SecretStr
    origin: str = "http://127.0.0.1:18081"


def private_root(value: str, *, create: bool = False) -> Path:
    root = Path(value).absolute()
    if root.is_symlink():
        raise ValueError("Evaluation directory must not be a symlink")
    if create:
        root.mkdir(mode=0o700, parents=False, exist_ok=False)
    if not root.is_dir() or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
        raise ValueError("Evaluation directory must be private and current-user owned")
    return root


def save(root: Path, manifest: Manifest) -> None:
    """Secrets enter only the explicit mode0600 handoff, never stdout."""
    value = manifest.model_dump(mode="json")
    value["encryption_key"] = manifest.encryption_key.get_secret_value()
    value["state_secret"] = manifest.state_secret.get_secret_value()
    for row, source in zip(value["sources"], manifest.sources, strict=True):
        row["api_key"] = source.api_key.get_secret_value()
    with NamedTemporaryFile(mode="w", dir=root, prefix=".manifest-", delete=False) as staged:
        temporary = Path(staged.name)
        try:
            json.dump(value, staged)
            staged.flush()
            os.fsync(staged.fileno())
            temporary.replace(root / "manifest.json")
        finally:
            temporary.unlink(missing_ok=True)


def configure(root: Path, manifest: Manifest):
    """Production container; no authentication dependency overrides."""
    from airweave.core import container

    settings.AUTH_MODE = AuthMode.API_KEY
    settings.ENCRYPTION_KEY = manifest.encryption_key.get_secret_value()
    settings.STATE_SECRET = manifest.state_secret.get_secret_value()
    settings.DENSE_EMBEDDER = "local_minilm"
    settings.EMBEDDING_DIMENSIONS = 384
    settings.SPARSE_EMBEDDER = "fastembed_bm25"
    settings.TEXT2VEC_INFERENCE_URL = "http://127.0.0.1:9878"
    settings.VESPA_URL = "http://127.0.0.1"
    settings.VESPA_PORT = 8081
    settings.STORAGE_BACKEND = "filesystem"
    settings.STORAGE_PATH = str(root / "blobs")
    settings.REDIS_PORT = 16379
    settings.DISABLE_RATE_LIMIT = True
    container.initialize_container(settings)
    return container.container


def capture_configuration(provider):
    if provider == "gmail":
        native = os.environ["LIVE_EXPECTED_EMAIL"]
        return (
            GmailConfig(expected_mailbox=native, gmail_query="newer_than:1d smaller:100K"),
            native,
            None,
        )
    if provider == "google_drive":
        native = os.environ["LIVE_DRIVE_PERMISSION_ID"]
        return GoogleDriveConfig(expected_permission_id=native), native, None
    if provider == "slack":
        native, user = os.environ["LIVE_SLACK_TEAM_ID"], os.environ["LIVE_SLACK_USER_ID"]
        return SlackConfig(expected_team_id=native, expected_user_id=user), native, user
    if provider == "google_calendar":
        native = os.environ["LIVE_CALENDAR_PRIMARY_ID"]
        now = datetime.now(timezone.utc).replace(microsecond=0)
        config = GoogleCalendarConfig(
            expected_primary_calendar_id=native,
            calendar_ids=[native],
            occurrence_window={"start": now, "end": now + timedelta(days=7)},
        )
        return config, native, None
    raise ValueError("Provider requires a native identity before evaluation binding")


async def capture_corpus(root: Path, manifest: Manifest, sessions, providers):
    runtime = configure(root, manifest)
    projector = CanonicalProjector(
        CanonicalProjectionStore(),
        lambda _organization: sessions(),
        ChunkEmbedProcessor(
            runtime.converter_registry, runtime.dense_embedder, runtime.sparse_embedder
        ),
        runtime.storage_backend,
    )
    for provider in providers:
        if any(source.provider == provider for source in manifest.sources):
            raise ValueError("Source already captured; resume its existing cursor instead")
        config, native, native_user = capture_configuration(provider)
        from provider_lifecycle import child

        async with sessions() as db:
            organization = Organization(name="Private bounded UI evaluation")
            db.add(organization)
            await db.flush()
            sync = Sync(
                organization_id=organization.id,
                name="Bounded live " + provider,
                index_pipeline_version=SEARCH_METADATA_PIPELINE_VERSION,
            )
            db.add(sync)
            await db.commit()
        # Production durable-page lifecycle; bounds interrupt without inventing completion.
        outcome = await child(
            {
                "schema": manifest.schema_name,
                "root": str(root),
                "provider": provider,
                "organization_id": str(organization.id),
                "sync_id": str(sync.id),
                "request_limit": 180,
                "record_limit": 500,
                "timeout": 180,
                "blob_byte_limit": 50 * 1024 * 1024,
                "file_byte_limit": 10 * 1024 * 1024,
                "query": "newer_than:1d smaller:100K",
                **(
                    {"calendar_config": config.model_dump(mode="json")}
                    if provider == "google_calendar"
                    else {}
                ),
            }
        )
        async with sessions() as db:
            count = await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == sync.id)
            )
            if not count:
                raise ValueError("Bounded lifecycle retained no original records")
            evidence = {"records": count, "lifecycle_exit": outcome, "bounded_evaluation": True}
            deployment = await db.scalar(select(VectorDbDeploymentMetadata))
            if deployment is None:
                deployment = VectorDbDeploymentMetadata(
                    dense_embedder="local_minilm",
                    embedding_dimensions=384,
                    sparse_embedder="fastembed_bm25",
                )
                db.add(deployment)
                await db.flush()
            if (
                deployment.dense_embedder,
                deployment.embedding_dimensions,
                deployment.sparse_embedder,
            ) != ("local_minilm", 384, "fastembed_bm25"):
                raise ValueError("Evaluation embedding deployment mismatch")
            collection = Collection(
                name="Bounded private evaluation",
                readable_id="evaluation-" + uuid4().hex,
                organization_id=sync.organization_id,
                vector_db_deployment_metadata_id=deployment.id,
            )
            db.add(collection)
            await db.flush()
            connection = SourceConnection(
                name="Bounded " + provider,
                short_name=provider,
                config_fields=config.model_dump(mode="json"),
                organization_id=sync.organization_id,
                readable_collection_id=collection.readable_id,
                sync_id=sync.id,
                is_authenticated=True,
            )
            db.add(connection)
            key = secrets.token_urlsafe(32)
            db.add(
                APIKey(
                    organization_id=sync.organization_id,
                    encrypted_key=credentials.encrypt({"key": key}),
                    expiration_date=utc_now_naive() + timedelta(days=1),
                )
            )
            await db.commit()
        source = Source(
            provider=provider,
            organization_id=sync.organization_id,
            sync_id=sync.id,
            source_connection_id=connection.id,
            external_account_id=native,
            external_user_id=native_user,
            label=os.environ["LIVE_EXPECTED_EMAIL"],
            api_key=SecretStr(key),
        )
        manifest.sources.append(source)
        save(root, manifest)
        destination = await VespaDestination.create(
            collection_id=collection.id, organization_id=sync.organization_id, logger=logger
        )
        try:
            after = None
            while True:
                batch = await projector.batch(
                    sync.organization_id, sync.id, provider, destination, logger, after_id=after
                )
                print(
                    json.dumps(
                        {
                            "provider": provider,
                            "capture": evidence,
                            "projection": batch.model_dump(mode="json"),
                        }
                    ),
                    flush=True,
                )
                if not batch.has_more:
                    break
                after = batch.after_id

        finally:
            await destination.close_connection()


async def project(root: Path, manifest: Manifest, sessions):
    runtime = configure(root, manifest)
    projector = CanonicalProjector(
        CanonicalProjectionStore(),
        lambda _organization: sessions(),
        ChunkEmbedProcessor(
            runtime.converter_registry, runtime.dense_embedder, runtime.sparse_embedder
        ),
        runtime.storage_backend,
    )
    for source in manifest.sources:
        async with sessions() as db:
            collection_id = await db.scalar(
                select(Collection.id)
                .join(
                    SourceConnection,
                    SourceConnection.readable_collection_id == Collection.readable_id,
                )
                .where(SourceConnection.id == source.source_connection_id)
            )
        destination = await VespaDestination.create(
            collection_id=collection_id, organization_id=source.organization_id, logger=logger
        )
        try:
            after = None
            while True:
                batch = await projector.batch(
                    source.organization_id,
                    source.sync_id,
                    source.provider,
                    destination,
                    logger,
                    after_id=after,
                )
                print(
                    json.dumps(
                        {"provider": source.provider, "projection": batch.model_dump(mode="json")}
                    ),
                    flush=True,
                )
                if not batch.has_more:
                    break
                after = batch.after_id
        finally:
            await destination.close_connection()


async def serve(root: Path, manifest: Manifest, sessions):
    """Serve the production HTTP app without running schedules or startup services."""
    import uvicorn

    from airweave.db import session

    runtime = configure(root, manifest)
    session.AsyncSessionLocal = sessions
    from airweave.main import app

    app.state.http_metrics = runtime.metrics.http
    await uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=18081,
            access_log=False,
            log_config=None,
            lifespan="off",
        )
    ).serve()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("capture", "extend", "project", "serve"))
    parser.add_argument("--directory", required=True)
    parser.add_argument("--provider", choices=("google_calendar", "slack"))
    args = parser.parse_args()
    if (args.command == "extend") != (args.provider is not None):
        parser.error("--provider is required only for extend")
    os.umask(0o077)
    root = private_root(args.directory, create=args.command == "capture")
    if args.command == "capture":
        from cryptography.fernet import Fernet

        manifest = Manifest(
            schema_name="canonical_eval_" + uuid4().hex,
            encryption_key=SecretStr(Fernet.generate_key().decode()),
            state_secret=SecretStr(secrets.token_urlsafe(32)),
        )
        save(root, manifest)
    else:
        manifest = Manifest.model_validate_json((root / "manifest.json").read_text())
    url = capture.test_database_url()
    engine = create_async_engine(
        url, connect_args={"server_settings": {"search_path": manifest.schema_name}}
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if args.command == "capture":
            async with engine.begin() as db:
                await db.execute(text(f'CREATE SCHEMA "{manifest.schema_name}"'))
                for path in sorted((Path(__file__).parents[2] / "alembic/versions").glob("*.py")):
                    if path.name[:4].isdigit():
                        await db.run_sync(capture.migrate, path.name)
            capture.StoragePaths.TEMP_PROCESSING = str(root / "downloads")
            await capture_corpus(root, manifest, sessions, ("gmail", "google_drive"))
        elif args.command == "extend":
            if args.provider is None:
                raise ValueError("Extend requires an explicit provider")
            capture.StoragePaths.TEMP_PROCESSING = str(root / "downloads")
            await capture_corpus(root, manifest, sessions, (args.provider,))
        elif args.command == "project":
            await project(root, manifest, sessions)
        else:
            await serve(root, manifest, sessions)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(json.dumps({"failed": True, "error_type": type(error).__name__}))
        raise SystemExit(1) from None
