from sqlalchemy import text

from ..alembic_utils import DatabaseUpgradeNeeded, UninitializedDatabase, check_database
from .base import Base

# This is the alembic revision ID of the database revision
# required by this version of Tiled.
REQUIRED_REVISION = "3db11ff95b6c"

# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = ["3db11ff95b6c", "0b033e7fbe30", "83889e049ddc", "6825c778aa3c"]


async def initialize_database(engine):
    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as connection:
        # Install extensions
        if engine.dialect.name == "postgresql":
            await connection.execute(text("create extension btree_gin;"))
        # Create all tables.
        await connection.run_sync(Base.metadata.create_all)
        if engine.dialect.name == "sqlite":
            # Use write-ahead log mode. This persists across all future connections
            # until/unless manually switched.
            # https://www.sqlite.org/wal.html
            await connection.execute(text("PRAGMA journal_mode=WAL;"))
        await connection.commit()


async def check_catalog_database(engine):
    redacted_url = engine.url._replace(password="[redacted]")
    try:
        await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
    except UninitializedDatabase:
        raise UninitializedDatabase(
            f"""

No catalog database found at {redacted_url}

To create one, run:

tiled catalog init {redacted_url}
""",
        )
    except DatabaseUpgradeNeeded:
        raise DatabaseUpgradeNeeded(
            f"""

The catalog found at

{redacted_url}

was created using an older version of Tiled. It needs to be upgraded
to work with this version. Back up the database, and the run:

tiled catalog upgrade-database {redacted_url}
""",
        )
