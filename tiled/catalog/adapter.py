import asyncio
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select

from ..utils import UNCHANGED
from . import orm
from .base import Base


async def initialize_database(engine):
    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as conn:
        # Create all tables.
        await conn.run_sync(Base.metadata.create_all)


class Adapter:
    def __init__(
        self,
        engine,
        segments=None,
        queries=None,
        sorting=None,
        key_maker=lambda: str(uuid.uuid4()),
        metadata=None,
        specs=None,
        references=None,
        time_created=None,
        time_updated=None,
    ):
        self.engine = engine
        self.segments = segments or []
        self.sorting = sorting or [("time_created", 1)]
        self.queries = queries or []
        self.key_maker = key_maker

        self.metadata = metadata or {}
        self.specs = specs or []
        self.references = references or []
        self.time_created = time_created
        self.time_updated = time_updated

    def __repr__(self):
        return f"<{type(self).__name__} {self.segments}>"

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
    def in_memory(cls, echo=False):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
        asyncio.run(initialize_database(engine))
        return cls(engine)

    @classmethod
    async def async_in_memory(cls, echo=False):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
        await initialize_database(engine)
        return cls(engine)

    def session(self):
        "Convenience method for constructing an AsyncSessoin context"
        return AsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    async def lookup(
        self, segments, principal=None
    ):  # TODO: Accept filter for predicate-pushdown.
        if not segments:
            return self
        async with self.session() as db:
            *ancestors, key = segments
            node = (
                await db.execute(
                    select(orm.Node)
                    .filter(orm.Node.ancestors == self.segments + ancestors)
                    .filter(orm.Node.key == key)
                    # TODO Apply queries.
                )
            ).scalar()
            # TODO Do binary search to find where database stops and
            # (for example) HDF5 file begins.
        if node is None:
            return
        return self.from_orm(node)

    def from_orm(self, node):
        return type(self)(
            engine=self.engine,
            segments=node.ancestors + [node.key],
            key_maker=self.key_maker,
            metadata=node.metadata,
            specs=node.specs,
            references=node.references,
            time_created=node.time_created,
            time_updated=node.time_updated,
        )

    def new_variation(
        self,
        *args,
        sorting=UNCHANGED,
        queries=UNCHANGED,
        # must_revalidate=UNCHANGED,
        **kwargs,
    ):
        if sorting is UNCHANGED:
            sorting = self.sorting
        # if must_revalidate is UNCHANGED:
        #     must_revalidate = self.must_revalidate
        if queries is UNCHANGED:
            queries = self.queries
        return type(self)(
            engine=self.engine,
            segments=self.segments,
            queries=queries,
            sorting=sorting,
            key_maker=self.key_maker,
            metadata=self.metadata,
            specs=self.specs,
            references=self.references,
            time_created=self.time_created,
            time_updated=self.time_updated,
            # access_policy=self.access_policy,
            # entries_stale_after=self.entries_stale_after,
            # metadata_stale_after=self.entries_stale_after,
            # must_revalidate=must_revalidate,
            **kwargs,
        )

    def search(self, query):
        return self.new_variation(queries=self.queries + [query])

    async def create_node(
        self, metadata, structure_family, specs, references, key=None
    ):
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
        # TODO Is this the right thing to return here?
        # Should we return anything at all?
        return self.from_orm(node)

    async def keys_slice(self, start, stop, direction):
        if direction != 1:
            raise NotImplementedError
        async with self.session() as db:
            return (
                (
                    await db.execute(
                        select(orm.Node.key)
                        .filter(orm.Node.ancestors == self.segments)
                        # TODO Apply queries.
                        .offset(start)
                        .limit(stop - start)
                    )
                )
                .scalars()
                .all()
            )

    async def items_slice(self, start, stop, direction):
        if direction != 1:
            raise NotImplementedError
        async with self.session() as db:
            return (
                await db.execute(
                    select(orm.Node)
                    .filter(orm.Node.ancestors == self.segments)
                    # TODO Apply queries.
                    .offset(start)
                    .limit(stop - start)
                )
            ).all()
