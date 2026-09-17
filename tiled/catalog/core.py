from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..alembic_utils import DatabaseUpgradeNeeded, UninitializedDatabase, check_database
from .base import Base

# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = [
    "de302a096358",
    "c31f6a1d7e20",
    "9bc9b57294b9",
    "b93c79d197f4",
    "e8956581ecd5",
    "bf2fe0eb8ee8",
    "85a47342e78e",
    "4cf6011a8db5",
    "a86a48befff6",
    "dfbb7478c6bd",
    "a963a6c32a0c",
    "e05e918092c3",
    "7809873ea2c7",
    "9331ed94d6ac",
    "45a702586b2a",
    "ed3a4223a600",
    "e756b9381c14",
    "2ca16566d692",
    "1cd99c02d0c7",
    "a66028395cab",
    "3db11ff95b6c",
    "0b033e7fbe30",
    "83889e049ddc",
    "6825c778aa3c",
]
REQUIRED_REVISION = ALL_REVISIONS[0]


async def initialize_database(engine: AsyncEngine):
    # The definitions in .orm alter Base.metadata.
    # The graph (splash-links) tables also live in the catalog database and
    # attach to Base.metadata, so importing them here ensures create_all
    # provisions them on fresh databases (existing databases get them via the
    # Alembic migration c31f6a1d7e20).
    from ..graph import orm as graph_orm  # noqa: F401
    from . import orm  # noqa: F401

    async with engine.connect() as connection:
        # Install extensions
        if engine.dialect.name == "postgresql":
            await connection.execute(text("create extension btree_gin;"))
        # Create all tables.
        await connection.run_sync(Base.metadata.create_all)
        root_access_blob_id = await connection.scalar(
            select(orm.NodeAccessBlob.access_blob_id).where(
                orm.NodeAccessBlob.node_id == 0
            )
        )
        if root_access_blob_id is None:
            result = await connection.execute(
                insert(orm.AccessBlob).values(kind="tags", tags=["public"])
            )
            await connection.execute(
                insert(orm.NodeAccessBlob).values(
                    node_id=0,
                    access_blob_id=result.inserted_primary_key[0],
                )
            )
        if engine.dialect.name == "sqlite":
            # Use write-ahead log mode. This persists across all future connections
            # until/unless manually switched.
            # https://www.sqlite.org/wal.html
            await connection.execute(text("PRAGMA journal_mode=WAL;"))
        await connection.commit()


async def check_catalog_database(engine: AsyncEngine):
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
