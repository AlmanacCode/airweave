"""Database session configuration."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import UUID

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from airweave.core.config import settings
from airweave.db.tenant_session import tenant_session_factory

# Explicit per-process application capacity. Tenant mode reserves one base slot
# for control and gives content the remaining base slots plus finite overflow.
# The independent health engine adds one connection. Multiply by all API/worker
# processes and rollout overlap; per-sync record workers do not size SQL pools.

POOL_SIZE = settings.db_pool_size
MAX_OVERFLOW = settings.db_pool_max_overflow

if settings.TENANT_DATABASE_URI is not None and POOL_SIZE < 2:
    raise RuntimeError("Tenant/control database isolation requires db_pool_size >= 2")

# Connection Pool Timeout Behavior:
# - pool_timeout=30: Wait up to 30 seconds for a connection to become available
# - If all connections are busy for 30+ seconds, raises TimeoutError
# - This prevents unbounded queueing and alerts to connection leaks
#
# Alternative configurations:
# - pool_timeout=0: Don't wait at all, fail immediately if no connections
# - pool_timeout=None: Wait forever (NOT RECOMMENDED - can cause deadlocks)

# Build connect_args based on environment
connect_args_config = {
    "server_settings": {
        # Kill idle transactions after 5 minutes
        "idle_in_transaction_session_timeout": "300000",
    },
    "command_timeout": 60,
}

# Disable SSL for PgBouncer connections (internal cluster traffic)
if settings.POSTGRES_SSLMODE == "disable":
    connect_args_config["ssl"] = False

async_engine = create_async_engine(
    str(settings.SQLALCHEMY_ASYNC_DATABASE_URI),
    pool_size=1 if settings.TENANT_DATABASE_URI is not None else POOL_SIZE,
    max_overflow=0 if settings.TENANT_DATABASE_URI is not None else MAX_OVERFLOW,
    pool_pre_ping=True,
    pool_recycle=300,  # Recycle connections after 5 minutes
    pool_timeout=30,  # Wait up to 30 seconds for a connection
    isolation_level="READ COMMITTED",
    # Note: async engines automatically use AsyncAdaptedQueuePool
    # Settings to prevent connection buildup:
    connect_args=connect_args_config,
)

AsyncSessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=async_engine)
# Keep the previous total application-pool ceiling. One slot is reserved for
# bootstrap/control; tenant reads and shared workers use the remaining capacity.
tenant_engine = (
    create_async_engine(
        str(settings.TENANT_DATABASE_URI),
        pool_size=POOL_SIZE - 1,
        max_overflow=MAX_OVERFLOW,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_timeout=30,
        isolation_level="READ COMMITTED",
        connect_args=connect_args_config,
    )
    if settings.TENANT_DATABASE_URI is not None
    else None
)


def _require_runtime_role(engine: AsyncEngine, grant_role: str) -> None:
    """Reject owner/admin credentials before a configured runtime connection is pooled."""

    @event.listens_for(engine.sync_engine, "connect")
    def validate_role(connection, _record):
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT pg_has_role(current_user,$1,'MEMBER') AND NOT EXISTS "
                "(SELECT 1 FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER') "
                "AND (r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole "
                "OR r.rolname IN ('airweave_discovery',$2))) AND NOT EXISTS "
                "(SELECT 1 FROM pg_class c WHERE c.relkind IN ('r','p') "
                "AND pg_has_role(current_user,c.relowner,'MEMBER'))",
                (
                    grant_role,
                    "airweave_control" if grant_role == "airweave_tenant" else "airweave_tenant",
                ),
            )
            if cursor.fetchone() != (True,):
                raise RuntimeError("Owned runtime database credential has forbidden authority")
        finally:
            cursor.close()


if tenant_engine is not None:
    _require_runtime_role(tenant_engine, "airweave_tenant")
    _require_runtime_role(async_engine, "airweave_control")


def get_tenant_engine() -> AsyncEngine:
    """Never fall back to the control or migration-owner credential for content."""
    if tenant_engine is None:
        raise RuntimeError("TENANT_DATABASE_URI is required for owned content operations")
    return tenant_engine


# Dedicated engine for health checks — isolated from the application pool so that
# a fully-saturated app pool cannot cause the readiness probe to false-negative.
health_check_engine = create_async_engine(
    str(settings.SQLALCHEMY_ASYNC_DATABASE_URI),
    pool_size=1,
    max_overflow=0,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_timeout=10,
    connect_args=connect_args_config,
)


@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session that can be used as a context manager.

    Yields:
        AsyncSession: An async database session

    Example:
    -------
        async with get_db_context() as db:
            await db.execute(...)

    """
    async with AsyncSessionLocal() as db:
        try:
            yield db
        finally:
            try:
                await db.close()
            except Exception:
                # Connection may have been closed by server due to idle timeout; ignore on close
                pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session to be used in dependency injection.

    Yields:
    ------
        AsyncSession: An async database session

    """
    async with AsyncSessionLocal() as db:
        try:
            yield db
        finally:
            try:
                await db.close()
            except Exception:
                # Connection may have been closed by server due to idle timeout; ignore on close
                pass


@asynccontextmanager
async def get_tenant_db_context(organization_id: UUID) -> AsyncGenerator[AsyncSession, None]:
    """Open a fresh immutable tenant session after authority resolves the organization."""
    async with tenant_session_factory(get_tenant_engine(), organization_id)() as db:
        yield db
