"""Explicit database setup: run after reviewed Alembic migrations, never on API startup.

Usage: python -m airweave.db.bootstrap
Local demo only: AUTH_MODE=local python -m airweave.db.bootstrap --local-superuser
Service organization/key provisioning remains an explicit operator action.
"""

import argparse
import asyncio

from airweave.core.config import AuthMode, settings
from airweave.db.init_db import init_db
from airweave.db.init_db_native import init_db_with_native_connections
from airweave.db.session import AsyncSessionLocal
from airweave.domains.embedders.config import initialize_embedding_config


async def bootstrap(*, local_superuser: bool = False) -> None:
    """Install native definitions, optionally opting into local demo identity."""
    if local_superuser and settings.AUTH_MODE != AuthMode.LOCAL:
        raise ValueError("--local-superuser requires AUTH_MODE=local")
    if local_superuser and not (settings.FIRST_SUPERUSER and settings.FIRST_SUPERUSER_PASSWORD):
        raise ValueError("Local bootstrap requires FIRST_SUPERUSER and FIRST_SUPERUSER_PASSWORD")
    async with AsyncSessionLocal() as db:
        await initialize_embedding_config(db)
        if local_superuser:
            await init_db(db)
        else:
            await init_db_with_native_connections(db)


def main() -> None:
    """Run the explicitly requested bootstrap operation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-superuser", action="store_true")
    args = parser.parse_args()
    asyncio.run(bootstrap(local_superuser=args.local_superuser))


if __name__ == "__main__":
    main()
