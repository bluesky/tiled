import asyncio
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select

from . import orm
from .base import Base


async def initialize_database(engine):
    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as conn:
        # Create all tables.
        await conn.run_sync(Base.metadata.create_all)


class Adapter:
    def __init__(self, engine, segments=None, queries=None, key_maker=lambda: str(uuid.uuid4()), metadata=None, specs=None, references=None):
        self.engine = engine
        self.segments = segments or []
        self.queries = queries or []
        self.key_maker = key_maker

        self.metadata = metadata or {}
        self.specs = specs or []
        self.references = references or []

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

    def session(self):
        "Convenience method for constructing an AsyncSessoin context"
        return AsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    async def lookup(self, segments, principal=None):  # TODO: Accept filter for predicate-pushdown.
        async with self.session() as db:
            ...
            # Return something that can be searched or keys()/values()/items().

    def search(self, query):
        # Return something that can be searched or keys()/values()/items().
        return type(self)(self.engine, self.queries + [query])

    async def create_node(self, metadata, structure_family, specs, references, key=None):
        key = key or self.key_maker()
        node = orm.Node(
            key=key,
            ancestors=self.segments,
            metadata_=metadata,
            structure_family=structure_family,
            specs=specs,
            references=references,
        )
        async with self.session() as db:
            db.add(node)
            await db.commit()

    async def keys_slice(self, start, stop, direction):
        if direction != 1:
            raise NotImplementedError
        async with self.session() as db:
            return (
                await db.execute(
                    select(orm.Node.key)
                    .filter(orm.Node.ancestors == self.segments)
                    .offset(start)
                    .limit(stop - start)
                )
            ).all()

    async def items_slice(self, start, stop, direction):
        if direction != 1:
            raise NotImplementedError
        async with self.session() as db:
            return (
                await db.execute(
                    select(orm.Node)
                    .filter(orm.Node.ancestors == self.segments)
                    .offset(start)
                    .limit(stop - start)
                )
            ).all()


class DatabaseNotFound(ValueError):
    pass
