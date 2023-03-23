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


class RootNode:
    """
    Node representing the root of the tree.

    This node is special because the state configuring this node arises from
    server initialization (typically a configuration file) not from the
    database.
    """

    def __init__(self, metadata, specs, references):
        # This is self.metadata_ to match orm.Node.
        self.metadata_ = metadata or {}
        self.specs = specs or []
        self.references = references or []
        self.ancestors = []
        self.key = None
        self.time_created = None
        self.time_updated = None


class Adapter:
    def __init__(
        self,
        engine,
        node,
        *,
        queries=None,
        sorting=None,
        key_maker=lambda: str(uuid.uuid4()),
    ):
        self.engine = engine
        self.node = node
        if node.key is None:
            # Special case for RootNode
            self.segments = []
        else:
            self.segments = node.ancestors + [node.key]
        self.sorting = sorting or [("time_created", 1)]
        self.order_by_clauses = order_by_clauses(self.sorting)
        self.queries = queries or []
        self.key_maker = key_maker
        self.metadata = node.metadata_
        self.specs = node.specs
        self.references = node.references
        self.time_creatd = node.time_created
        self.time_updated = node.time_updated

    def __repr__(self):
        return f"<{type(self).__name__} {self.segments}>"

    @classmethod
    def from_uri(
        cls, database_uri, metadata=None, specs=None, references=None, echo=False
    ):
        "Connect to an existing database."
        # TODO Check that database exists and has the expected alembic revision.
        engine = create_async_engine(database_uri, echo=echo)
        return cls(engine, RootNode(metadata, specs, references))

    @classmethod
    def create_from_uri(
        cls, database_uri, metadata=None, specs=None, references=None, echo=False
    ):
        "Create a new database and connect to it."
        engine = create_async_engine(database_uri, echo=echo)
        asyncio.run(initialize_database(engine))
        return cls(engine, RootNode(metadata, specs, references))

    @classmethod
    async def async_create_from_uri(
        cls, database_uri, metadata=None, specs=None, references=None, echo=False
    ):
        "Create a new database and connect to it."
        engine = create_async_engine(database_uri, echo=echo)
        await initialize_database(engine)
        return cls(engine, RootNode(metadata, specs, references))

    @classmethod
    def in_memory(cls, metadata=None, specs=None, references=None, echo=False):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
        asyncio.run(initialize_database(engine))
        return cls(engine, RootNode(metadata, specs, references))

    @classmethod
    async def async_in_memory(
        cls, metadata=None, specs=None, references=None, echo=False
    ):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
        await initialize_database(engine)
        return cls(engine, RootNode(metadata, specs, references))

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
            # TODO Do binary search or use some tricky JSON query to find where
            # database stops and (for example) HDF5 file begins.
        if node is None:
            return
        return self.from_orm(node)

    def from_orm(self, node):
        return type(self)(engine=self.engine, node=node, key_maker=self.key_maker)

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
            node=self.node,
            queries=queries,
            sorting=sorting,
            key_maker=self.key_maker,
            # access_policy=self.access_policy,
            # entries_stale_after=self.entries_stale_after,
            # metadata_stale_after=self.entries_stale_after,
            # must_revalidate=must_revalidate,
            **kwargs,
        )

    def search(self, query):
        return self.new_variation(queries=self.queries + [query])

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

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
                        .order_by(*self.order_by_clauses)
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
            nodes = (
                (
                    await db.execute(
                        select(orm.Node)
                        .filter(orm.Node.ancestors == self.segments)
                        .order_by(*self.order_by_clauses)
                        # TODO Apply queries.
                        .offset(start)
                        .limit(stop - start)
                    )
                )
                .scalars()
                .all()
            )
            return [(node.key, self.from_orm(node)) for node in nodes]


# Map sort key to Node ORM attribute.
_STANDARD_SORT_KEYS = {
    "time_created": "time_created",
    "time_updated": "time_created",
    "id": "key",
    # Could support structure_family...others?
}


def order_by_clauses(sorting):
    clauses = []
    for key, direction in sorting:
        if key in _STANDARD_SORT_KEYS:
            clause = getattr(orm.Node, _STANDARD_SORT_KEYS[key])
        else:
            # We are sorting by something within the user metadata namespace.
            # This can be given bare like "color" or namedspaced like
            # "metadata.color" to avoid the possibility of a name collision
            # with the standard sort keys.
            if key.startswith("metadata."):
                key = key[len("metadata.") :]  # noqa: E203
            clause = orm.Node.metadata_
            for segment in key.split("."):
                clause = clause[segment]
        if direction == -1:
            clause = clause.desc()
        clauses.append(clause)
    return clauses
