import asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from .base import Base


async def initialize_database(engine):
    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as conn:
        # Create all tables.
        await conn.run_sync(Base.metadata.create_all)


class Adapter:
    def __init__(self, engine, segments=None, queries=None):
        self.engine = engine
        self.segments = segments or []
        self.queries = queries or []

    @classmethod
    def from_uri(cls, database_uri):
        "Connect to an existing database."
        # TODO Check that database exists and has the expected alembic revision.
        engine = create_async_engine(database_uri)
        return cls(engine)

    @classmethod
    def create_from_uri(cls, database_uri):
        "Create a new database and connect to it."
        engine = create_async_engine(database_uri)
        asyncio.run(initialize_database(engine))
        return cls(engine)

    @classmethod
    def in_memory(cls):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        asyncio.run(initialize_database(engine))
        return cls(engine)

    def session(self, engine):
        "Convenience method for constructing an AsyncSessoin context"
        return AsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    async def lookup(self, segments, principal=None):  # TODO: Accept filter for predicate-pushdown.
        async with self.session():
            ...
            # Return something that can be searched or keys()/values()/items().

    def search(self, query):
        # Return something that can be searched or keys()/values()/items().
        return type(self)(self.engine, self.queries + [query])


class DatabaseNotFound(ValueError):
    pass
