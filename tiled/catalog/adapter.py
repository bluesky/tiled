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
    def __init__(self, engine):
        self.engine = engine

    @classmethod
    def from_uri(cls, database_uri):
        # TODO Check that database exists and is initialized.
        engine = create_async_engine(database_uri)
        return cls(engine)

    @classmethod
    def create_from_uri(cls, database_uri):
        engine = create_async_engine(database_uri)
        asyncio.run(initialize_database(engine))
        return cls(engine)

    @classmethod
    def in_memory(cls):
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        asyncio.run(initialize_database(engine))
        return cls(engine)

    def session(self, engine):
        return AsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    async def lookup(self, segments, principal):  # TODO: Accept filter for predicate-pushdown.
        async with self.session():
            ...

    async def search(self, query):
        ...


class DatabaseNotFound(ValueError):
    pass
