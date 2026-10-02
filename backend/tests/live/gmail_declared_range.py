"""One opt-in retained Gmail range using the existing lifecycle, no indexing/models.

Private inputs and saved native attestation paths are explicit arguments. The
requested mailbox must match both before the one live binding check. A new private
Unix-socket schema/storage is retained for downstream relevance qualification.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import mkdtemp
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import provider_lifecycle as lifecycle  # isolated settings before application imports
from pydantic import BaseModel, ConfigDict, Field, SecretStr, TypeAdapter
from sqlalchemy import Text, func, select, text

from airweave.domains.auth_provider.providers.composio import ComposioAuthProvider
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.entities.canonical.mail_body import prepared_mail_body
from airweave.domains.entities.canonical.mail_models import MailMessageQuery
from airweave.domains.entities.canonical.mail_query import CanonicalMailQuery
from airweave.domains.entities.canonical.projection_inputs import ProjectionInputs
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import (
    ProjectionContext,
    ProjectionRuntime,
    StrictProjectionTracker,
    _select_inputs,
)
from airweave.domains.entities.canonical.requests import BlobReference
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder
from airweave.models import Collection, Entity, Organization, SourceConnection, Sync
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata
from airweave.platform.configs.config import GmailConfig


class PrivateInputs(BaseModel):
    """Only credential-store fields this operator is allowed to consume."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    COMPOSIO_API_KEY: SecretStr
    LIVE_GMAIL_ACCOUNT_ID: str = Field(min_length=1, repr=False)
    LIVE_EXPECTED_EMAIL: str = Field(min_length=1, repr=False)


class AuthConfig(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    id: str = Field(min_length=1, repr=False)
    is_disabled: bool


class Toolkit(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    slug: str


class AccountBinding(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    id: str = Field(min_length=1, repr=False)
    user_id: str = Field(min_length=1, repr=False)
    status: str
    is_disabled: bool
    toolkit: Toolkit
    auth_config: AuthConfig


class Attestation(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    binding: AccountBinding
    binding_verified: bool
    native_profile_read: bool
    email: str = Field(repr=False)


class RetainedCorpus(BaseModel):
    """Only the already-private local routing needed for read-only verification."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    schema_name: str = Field(alias="schema", pattern=r"^canonical_gmail14_[a-f0-9]{32}$")
    root: Path
    organization_id: UUID
    sync_id: UUID


def verify_blobs(blob_root: Path, blobs: tuple[BlobReference, ...]) -> dict[str, BlobReference]:
    """Hash exact immutable references without logging filenames or loading whole assets."""
    references = {}
    for blob in blobs:
        if blob.key in references and (
            references[blob.key].sha256 != blob.sha256
            or references[blob.key].size_bytes != blob.size_bytes
        ):
            raise ValueError("Retained blob references conflict")
        references[blob.key] = blob
    for blob in references.values():
        path = (blob_root / blob.key).resolve()
        if not path.is_relative_to(blob_root) or path.stat().st_size != blob.size_bytes:
            raise ValueError("Retained blob byte boundary differs")
        digest = hashlib.sha256()
        with path.open("rb") as content:
            for chunk in iter(lambda: content.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != blob.sha256:
            raise ValueError("Retained blob digest differs")
    return references


async def verify_corpus(manifest_path: Path) -> None:
    """No provider calls: hash retained bytes and traverse real sequence-fenced mail SQL."""
    corpus = RetainedCorpus.model_validate(private_json(manifest_path))
    engine = lifecycle.create_async_engine(
        lifecycle.harness.test_database_url(),
        connect_args={"server_settings": {"search_path": corpus.schema_name}},
    )
    sessions = lifecycle.async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as db:
            raw = list(
                await db.scalars(
                    select(Entity.blob_references).where(
                        Entity.organization_id == corpus.organization_id,
                        Entity.sync_id == corpus.sync_id,
                        Entity.record_revision > 0,
                    )
                )
            )
        blobs = TypeAdapter(tuple[BlobReference, ...])
        references = verify_blobs(
            (corpus.root / "blobs").resolve(),
            tuple(blob for entries in raw for blob in blobs.validate_python(entries or [])),
        )
        query = CanonicalMailQuery(secrets.token_urlsafe(32))
        seen, cursors, pages, cursor = set(), set(), 0, None
        while True:
            async with sessions() as db:
                page = await query.messages(
                    db,
                    corpus.organization_id,
                    corpus.sync_id,
                    MailMessageQuery(limit=100, cursor=cursor),
                )
            pages += 1
            for message in page.messages:
                if message.id in seen:
                    raise ValueError("Retained mail traversal duplicated an identity")
                seen.add(message.id)
            if not page.has_more:
                break
            if page.next_cursor is None or page.next_cursor in cursors:
                raise ValueError("Retained mail traversal repeated a cursor")
            cursors.add(page.next_cursor)
            cursor = page.next_cursor
        verified = {
            "provider_calls": 0,
            "blob_hashes_verified": len(references),
            "referenced_blob_bytes": sum(blob.size_bytes for blob in references.values()),
            "mail_metadata_records_enumerated": len(seen),
            "mail_metadata_pages": pages,
            "mail_indexing": page.indexing.model_dump(mode="json"),
            "capture_coverage": page.capture.model_dump(mode="json") if page.capture else None,
            "consistency": page.consistency,
            "body_discovery_payloads": False,
        }
        write_private(corpus.root / "verification.json", verified)
        print(json.dumps(verified), flush=True)
    finally:
        await engine.dispose()


async def original_digest(sessions, corpus: RetainedCorpus) -> str:
    """Stream the retained original/revision state, without printing or duplicating it."""
    digest = hashlib.sha256()
    async with sessions() as db:
        rows = await db.stream(
            select(
                Entity.id,
                Entity.record_revision,
                Entity.source_payload.cast(Text),
                Entity.blob_references.cast(Text),
            )
            .where(
                Entity.organization_id == corpus.organization_id,
                Entity.sync_id == corpus.sync_id,
            )
            .order_by(Entity.id)
            .execution_options(yield_per=25)
        )
        async for row in rows:
            digest.update(json.dumps(tuple(str(value) for value in row)).encode())
    return digest.hexdigest()


async def prepare_body(work, sessions, store, storage, registry, builder) -> bool:
    """The existing mapper/converter/body fact stage, ending before chunk/embed/feed."""
    async with sessions() as db:
        if not await store.admit(db, work):
            return False
    generation = uuid4()
    async with map_record(work.record, "gmail", storage) as mapped:
        message = ProjectionInputs(
            parts=tuple(item for item in mapped.parts if item.part.part_index == 0)
        )
        selected, _ = _select_inputs(
            message,
            work,
            "gmail",
            generation,
            lambda extension: registry.for_extension(extension) is not None,
        )
        built = await builder.build_with_text(
            selected,
            ProjectionContext(logging.getLogger("private-mail-text"), "gmail"),
            ProjectionRuntime(StrictProjectionTracker()),
        )
        body = prepared_mail_body(built.representations, generation, work.record.completeness)
        if body is None:
            raise ValueError("No complete converter body was prepared")
        async with sessions() as db:
            return await store.prepare_mail_body(db, work, generation, body)


async def prepare_corpus(manifest_path: Path) -> None:
    """Explicit operator proof of real retained body preparation; no automatic activation."""
    corpus = RetainedCorpus.model_validate(private_json(manifest_path))
    engine = lifecycle.create_async_engine(
        lifecycle.harness.test_database_url(),
        connect_args={"server_settings": {"search_path": corpus.schema_name}},
    )
    sessions = lifecycle.async_sessionmaker(engine, expire_on_commit=False)
    store = CanonicalProjectionStore()
    storage = lifecycle.harness.FilesystemBackend(corpus.root / "blobs")
    registry = ConverterRegistry(ocr_provider=None)
    builder = TextualRepresentationBuilder(registry)
    began, prepared, superseded, errors, after = time.monotonic(), 0, 0, {}, None
    try:
        before = await original_digest(sessions, corpus)
        while True:
            async with sessions() as db:
                work = await store.pending(
                    db,
                    corpus.organization_id,
                    corpus.sync_id,
                    after_id=after,
                    limit=25,
                )
            if not work:
                break
            for item in work:
                try:
                    if await prepare_body(item, sessions, store, storage, registry, builder):
                        prepared += 1
                    else:
                        superseded += 1
                except Exception as error:
                    name = type(error).__name__
                    errors[name] = errors.get(name, 0) + 1
                    async with sessions() as db:
                        await store.fail(db, item, name)
            after = work[-1].record.id
        async with sessions() as db:
            body_bytes = await db.scalar(
                select(func.sum(func.octet_length(ProjectionGeneration.mail_body_text))).where(
                    ProjectionGeneration.organization_id == corpus.organization_id,
                    ProjectionGeneration.sync_id == corpus.sync_id,
                )
            )
        evidence = {
            "prepared_messages": prepared,
            "superseded_messages": superseded,
            "failed_messages": sum(errors.values()),
            "error_classes": errors,
            "prepared_body_bytes": body_bytes or 0,
            "seconds": round(time.monotonic() - began, 3),
            "provider_calls": 0,
            "embedding_calls": 0,
            "paid_model_calls": 0,
            "index_publication_claimed": False,
            "automatic_preparation_qualified": False,
            "originals_unchanged": before == await original_digest(sessions, corpus),
        }
        write_private(corpus.root / "preparation.json", evidence)
        print(json.dumps(evidence), flush=True)
    finally:
        await engine.dispose()


def private_json(path: Path):
    """Refuse public credential/identity files rather than weakening their mode."""
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077:
        raise ValueError("Operator inputs must be private and owned by the current user")
    return json.loads(path.read_text())


def write_private(path: Path, value) -> None:
    with path.open("x") as output:
        os.chmod(path, 0o600)
        json.dump(value, output, indent=2, allow_nan=False)


async def main(inputs_path: Path, attestation_path: Path, mailbox: str) -> None:
    inputs = PrivateInputs.model_validate(private_json(inputs_path))
    accounts = [Attestation.model_validate(item) for item in private_json(attestation_path)]
    selected = [item for item in accounts if item.binding.id == inputs.LIVE_GMAIL_ACCOUNT_ID]
    if (
        len(selected) != 1
        or not selected[0].binding_verified
        or not selected[0].native_profile_read
        or selected[0].email.casefold() != mailbox.casefold()
        or inputs.LIVE_EXPECTED_EMAIL.casefold() != mailbox.casefold()
    ):
        raise ValueError("Explicit requested mailbox does not match the saved binding")
    account = selected[0].binding
    if (
        account.toolkit.slug != "gmail"
        or account.status != "ACTIVE"
        or account.is_disabled
        or account.auth_config.is_disabled
    ):
        raise ValueError("Saved Gmail binding is unavailable")
    url = lifecycle.harness.test_database_url()
    # Existing Composio provider revalidates exact account/user/auth-config/toolkit.
    provider = await ComposioAuthProvider.create(
        credentials={"api_key": inputs.COMPOSIO_API_KEY.get_secret_value()},
        config={
            "account_id": account.id,
            "user_id": account.user_id,
            "auth_config_id": account.auth_config.id,
        },
    )
    provider._logger = MagicMock()
    await provider.get_auth_result("gmail", [])
    os.environ.update(
        COMPOSIO_API_KEY=inputs.COMPOSIO_API_KEY.get_secret_value(),
        LIVE_GMAIL_ACCOUNT_ID=account.id,
        LIVE_EXPECTED_EMAIL=inputs.LIVE_EXPECTED_EMAIL,
    )
    os.umask(0o077)
    root = Path(mkdtemp(prefix="almanac-gmail14-private-"))
    schema = "canonical_gmail14_" + uuid4().hex
    organization_id, sync_id, source_id, collection_id = (uuid4() for _ in range(4))
    end = int(time.time())
    start = end - 14 * 86400
    config = GmailConfig(
        expected_mailbox=inputs.LIVE_EXPECTED_EMAIL,
        gmail_query=f"after:{start} before:{end}",
    )
    manifest = {
        "provider": "gmail",
        "request_limit": 4000,
        "record_limit": 2000,
        "timeout": 1800,
        "blob_byte_limit": 512 * 1024 * 1024,
        "file_byte_limit": lifecycle.FileService.MAX_FILE_SIZE_BYTES,
        "schema": schema,
        "root": str(root),
        "organization_id": str(organization_id),
        "sync_id": str(sync_id),
        "job_id": str(uuid4()),
        "query": config.gmail_query,
        "retained_binding": {
            "enabled": True,
            "collection_id": str(collection_id),
            "source_connection_id": str(source_id),
        },
    }
    write_private(root / "manifest.json", manifest)
    write_private(
        root / "serving.private.json",
        {
            **manifest,
            "database_url": url,
            "storage_root": str(root / "blobs"),
            "api_key": secrets.token_urlsafe(32),
            "cursor_secret": secrets.token_urlsafe(32),
            "inputs_path": str(inputs_path),
            "credential_values_persisted": False,
        },
    )
    admin = lifecycle.create_async_engine(url)
    engine = lifecycle.create_async_engine(
        url, connect_args={"server_settings": {"search_path": schema}}
    )
    sessions = lifecycle.async_sessionmaker(engine, expire_on_commit=False)
    began = time.monotonic()
    try:
        async with admin.begin() as db:
            await db.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as db:
            for migration in sorted(
                (Path(__file__).resolve().parents[2] / "alembic/versions").glob("*.py")
            ):
                await db.run_sync(lifecycle.harness.migrate, migration.name)
        async with sessions() as db:
            db.add(Organization(id=organization_id, name="Private Gmail14 qualification"))
            await db.flush()
            metadata = VectorDbDeploymentMetadata(
                dense_embedder="local_minilm",
                embedding_dimensions=384,
                sparse_embedder="fastembed_bm25",
            )
            db.add(metadata)
            await db.flush()
            db.add(
                Collection(
                    id=collection_id,
                    organization_id=organization_id,
                    name="Private declared Gmail14",
                    readable_id=str(collection_id),
                    vector_db_deployment_metadata_id=metadata.id,
                )
            )
            db.add(
                Sync(
                    id=sync_id,
                    organization_id=organization_id,
                    name="Fixed declared Gmail14 scope",
                    index_pipeline_version=2,
                )
            )
            await db.flush()
            db.add(
                SourceConnection(
                    id=source_id,
                    organization_id=organization_id,
                    sync_id=sync_id,
                    name="Private Gmail14",
                    short_name="gmail",
                    readable_collection_id=str(collection_id),
                    is_authenticated=False,
                    config_fields=config.model_dump(mode="json"),
                    auth_provider_config={
                        "account_id": account.id,
                        "user_id": account.user_id,
                        "auth_config_id": account.auth_config.id,
                    },
                )
            )
            await db.commit()
        print(
            json.dumps(
                {"stage": "capture_started", "private_corpus": str(root), "provider_writes": False}
            ),
            flush=True,
        )
        code, result = await lifecycle.execute_trial(root / "manifest.json", 1860)
        async with sessions() as db:
            records = await db.scalar(
                select(func.count(Entity.id)).where(
                    Entity.sync_id == sync_id,
                    Entity.record_revision > 0,
                )
            )
            original_bytes = await db.scalar(
                select(func.sum(func.octet_length(Entity.source_payload.cast(Text)))).where(
                    Entity.sync_id == sync_id, Entity.record_revision > 0
                )
            )
        blob_files = list((root / "blobs").rglob("*")) if (root / "blobs").exists() else []
        blobs = [item for item in blob_files if item.is_file()]
        evidence = {
            "provider": "gmail",
            "provider_writes": False,
            "exit_code": code,
            "window_start_utc": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "window_end_utc": datetime.fromtimestamp(end, timezone.utc).isoformat(),
            "query_date_semantics": "Gmail after/before Unix seconds; observed filtered view",
            "binding_metadata_reads": 1,
            "binding_metadata_verified": True,
            "unique_stored_records": records,
            "retained_original_json_bytes": original_bytes or 0,
            "retained_blob_files": len(blobs),
            "retained_blob_bytes": sum(item.stat().st_size for item in blobs),
            "seconds": round(time.monotonic() - began, 2),
            "schema_retained": True,
            "blob_directory_retained": True,
            "index_publication_claimed": False,
            "paid_model_calls": 0,
            "bounds": {
                key: manifest[key]
                for key in (
                    "request_limit",
                    "record_limit",
                    "timeout",
                    "blob_byte_limit",
                    "file_byte_limit",
                )
            },
            "capture": result,
        }
        write_private(root / "evidence.json", evidence)
        print(
            json.dumps({"stage": "capture_finished", "private_corpus": str(root), **evidence}),
            flush=True,
        )
    finally:
        await engine.dispose()
        await admin.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--attestation", type=Path)
    parser.add_argument("--mailbox")
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--prepare-text", type=Path)
    args = parser.parse_args()
    if not (args.verify or args.prepare_text) and not (
        args.inputs and args.attestation and args.mailbox
    ):
        parser.error("Capture requires --inputs, --attestation and --mailbox")
    os.umask(0o077)
    try:
        if args.prepare_text:
            asyncio.run(prepare_corpus(args.prepare_text))
        elif args.verify:
            asyncio.run(verify_corpus(args.verify))
        else:
            asyncio.run(main(args.inputs, args.attestation, args.mailbox))
    except Exception as error:
        print(
            json.dumps(
                {
                    "stage": "operator_failed",
                    "error_type": type(error).__name__,
                    "full_scope_completed": False,
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None
