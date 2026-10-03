"""Actual PostgreSQL RLS mechanics in one synthetic schema, never the corpus."""

import asyncio
import os
import secrets
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import String, select, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from airweave.db.tenant_session import TenantScope, tenant_session_factory
from airweave.db.unit_of_work import UnitOfWork


class Base(DeclarativeBase):
    """Only a tiny synthetic test table is protected by this fixture."""


class Original(Base):
    """Two synthetic tenant rows let SQL and identity-map behavior be inspected."""

    __tablename__ = "tenant_original"
    id: Mapped[UUID] = mapped_column(PGUUID, primary_key=True)
    organization_id: Mapped[UUID] = mapped_column(PGUUID)
    body: Mapped[str] = mapped_column(String)


@pytest.fixture
async def runtime_engine():
    """Own one fresh schema and non-owner role; do not run real table migrations."""
    url = os.environ.get("CANONICAL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set CANONICAL_TEST_DATABASE_URL to disposable PostgreSQL")
    suffix = uuid4().hex
    schema, role = "tenant_session_" + suffix, "tenant_runtime_" + suffix
    password = secrets.token_hex(24)
    admin = create_async_engine(url)
    a, b, first, second = [uuid4() for _ in range(4)]
    async with admin.begin() as db:
        admin_role = await db.scalar(text("SELECT current_user"))
        await db.execute(text(f'CREATE SCHEMA "{schema}"'))
        await db.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        await db.run_sync(Base.metadata.create_all)
        await db.execute(
            Original.__table__.insert(),
            [
                {"id": first, "organization_id": a, "body": "synthetic A"},
                {"id": second, "organization_id": b, "body": "synthetic B"},
            ],
        )
        await db.execute(
            text(
                f'CREATE ROLE "{role}" LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS '
                f"NOCREATEDB NOCREATEROLE PASSWORD '{password}'"
            )
        )
        await db.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
        await db.execute(
            text(f'GRANT SELECT, INSERT, UPDATE, DELETE ON "{schema}".tenant_original TO "{role}"')
        )
        await db.execute(text("ALTER TABLE tenant_original ENABLE ROW LEVEL SECURITY"))
        await db.execute(text("ALTER TABLE tenant_original FORCE ROW LEVEL SECURITY"))
        await db.execute(
            text(
                f'CREATE POLICY tenant_boundary ON tenant_original TO "{role}" '
                "USING (organization_id = "
                "NULLIF(current_setting('airweave.organization_id',true),'')::uuid) "
                "WITH CHECK (organization_id = "
                "NULLIF(current_setting('airweave.organization_id',true),'')::uuid)"
            )
        )
    engine = create_async_engine(
        make_url(url).set(username=role, password=password),
        pool_size=1,
        max_overflow=0,
        connect_args={"server_settings": {"search_path": schema}},
    )
    try:
        yield engine, a, b, first, second, admin_role
    finally:
        await engine.dispose()
        async with admin.begin() as db:
            await db.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await db.execute(text(f'DROP ROLE "{role}"'))
        await admin.dispose()


async def test_scope_reapplied_after_commit_rollback_and_pool_reuse(runtime_engine):
    """Unscoped SQL cannot leak B; repeated UnitOfWork commits retain A authority."""
    engine, a, b, first, second, admin_role = runtime_engine
    plain = async_sessionmaker(engine)
    async with plain() as db:
        role = (
            await db.execute(
                text("SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            )
        ).one()
        assert tuple(role) == (False, False)
        assert await db.scalar(text("SELECT session_user = current_user"))
        with pytest.raises(DBAPIError, match="permission denied"):
            quoted_admin = admin_role.replace('"', '""')
            await db.execute(text(f'SET ROLE "{quoted_admin}"'))
        await db.rollback()
        assert not (await db.scalars(select(Original))).all()
        pid = await db.scalar(text("SELECT pg_backend_pid()"))
    async with tenant_session_factory(engine, a)() as db:
        for body in ("first commit", "second commit"):
            async with UnitOfWork(db):
                assert [row.id for row in (await db.scalars(select(Original))).all()] == [first]
                original = await db.get(Original, first)
                original.body = body
        await db.rollback()
        assert await db.get(Original, second) is None
        assert await db.scalar(text("SELECT current_setting('airweave.organization_id')")) == str(a)
        assert await db.scalar(text("SELECT pg_backend_pid()")) == pid
        with pytest.raises(AttributeError):
            db.sync_session.tenant_scope = TenantScope(organization_id=b)
        with pytest.raises(ValidationError):
            db.sync_session.tenant_scope.organization_id = b
        assert (await db.get(Original, first)).organization_id == a
        with pytest.raises(DBAPIError, match="row-level security"):
            await db.execute(
                Original.__table__.insert().values(id=uuid4(), organization_id=b, body="forbidden")
            )
        await db.rollback()
        assert await db.scalar(text("SELECT current_setting('airweave.organization_id')")) == str(a)
    async with plain() as db:
        assert await db.scalar(text("SELECT pg_backend_pid()")) == pid
        assert not (await db.scalars(select(Original))).all()
    async with tenant_session_factory(engine, b)() as db:
        assert [row.id for row in (await db.scalars(select(Original))).all()] == [second]
        assert await db.get(Original, first) is None


async def test_timeout_rollback_clears_context_for_next_borrower(runtime_engine):
    """A cancelled SQL statement cannot leave tenant context on a pooled connection."""
    engine, a, b, first, second, admin_role = runtime_engine
    async with tenant_session_factory(engine, a)() as db:
        await db.execute(text("SET LOCAL statement_timeout='20ms'"))
        with pytest.raises(DBAPIError, match="statement timeout"):
            await db.execute(text("SELECT pg_sleep(1)"))
        await db.rollback()
        assert await db.scalar(text("SELECT current_setting('airweave.organization_id')")) == str(a)
    started = asyncio.Event()

    async def interrupted_borrower():
        async with tenant_session_factory(engine, a)() as db:
            await db.scalar(text("SELECT current_setting('airweave.organization_id')"))
            started.set()
            await db.execute(text("SELECT pg_sleep(10)"))

    task = asyncio.create_task(interrupted_borrower())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with async_sessionmaker(engine)() as db:
        assert not (await db.scalars(select(Original))).all()
    async with tenant_session_factory(engine, b)() as db:
        assert await db.get(Original, second) is not None
        assert await db.get(Original, first) is None


def test_factory_rejects_unvalidated_scope_before_any_connection():
    """The internal boundary requires UUID, not a raw header or mutable scope dict."""
    with pytest.raises(ValidationError):
        tenant_session_factory(None, "not-a-uuid")
    with pytest.raises(ValidationError):
        tenant_session_factory(None, str(uuid4()))
