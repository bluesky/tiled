from sqlalchemy import text

from .base import Base

ALL_REVISIONS = ["6825c778aa3c"]
REQUIRED_REVISION = "6825c778aa3c"


async def initialize_database(engine):
    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as connection:
        # Create all tables.
        await connection.run_sync(Base.metadata.create_all)
        if engine.dialect.name == "sqlite":
            # Use write-ahead log mode. This persists across all future connections
            # until/unless manually switched.
            # https://www.sqlite.org/wal.html
            await connection.execute(text("PRAGMA journal_mode=WAL;"))
        await connection.commit()
