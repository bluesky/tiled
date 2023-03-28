import asyncio
import os
import re
import uuid

from sqlalchemy import text, type_coerce
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select

from ..queries import Eq
from ..query_registration import QueryTranslationRegistry
from ..utils import UNCHANGED
from . import orm
from .base import Base
from .explain import ExplainAsyncSession

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))
INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


async def initialize_database(engine):
    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as connection:
        # Create all tables.
        await connection.run_sync(Base.metadata.create_all)
        await connection.commit()


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
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        engine,
        node,
        *,
        conditions=None,
        sorting=None,
        key_maker=lambda: str(uuid.uuid4()),
        new_database=False,
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
        self.conditions = conditions or []
        self.key_maker = key_maker
        self.metadata = node.metadata_
        self.specs = node.specs
        self.references = node.references
        self.time_creatd = node.time_created
        self.time_updated = node.time_updated
        self.new_database = new_database

    def __repr__(self):
        return f"<{type(self).__name__} {self.segments}>"

    async def __aenter__(self):
        if self.new_database:
            await initialize_database(self.engine)
        return self

    async def __aexit__(self, *args):
        await self.engine.dispose()

    @classmethod
    def from_uri(
        cls, database_uri, metadata=None, specs=None, references=None, echo=DEFAULT_ECHO
    ):
        "Connect to an existing database."
        # TODO Check that database exists and has the expected alembic revision.
        engine = create_async_engine(database_uri, echo=echo)
        return cls(engine, RootNode(metadata, specs, references))

    @classmethod
    def create_from_uri(
        cls, database_uri, metadata=None, specs=None, references=None, echo=DEFAULT_ECHO
    ):
        "Create a new database and connect to it."
        engine = create_async_engine(database_uri, echo=echo)
        asyncio.run(initialize_database(engine))
        return cls(engine, RootNode(metadata, specs, references), new_database=True)

    @classmethod
    def async_create_from_uri(
        cls,
        database_uri,
        metadata=None,
        specs=None,
        references=None,
        echo=DEFAULT_ECHO,
    ):
        "Create a new database and connect to it."
        engine = create_async_engine(database_uri, echo=echo)
        return cls(engine, RootNode(metadata, specs, references), new_database=True)

    @classmethod
    def in_memory(cls, metadata=None, specs=None, references=None, echo=DEFAULT_ECHO):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
        asyncio.run(initialize_database(engine))
        return cls(engine, RootNode(metadata, specs, references), new_database=True)

    @classmethod
    def async_in_memory(
        cls,
        metadata=None,
        specs=None,
        references=None,
        echo=DEFAULT_ECHO,
    ):
        "Create a transient database in memory."
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
        return cls(engine, RootNode(metadata, specs, references), new_database=True)

    def session(self):
        "Convenience method for constructing an AsyncSession context"
        return ExplainAsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    # This is heading in a reasonable direction but does not actually work yet.
    # Pausing development for now.
    #
    #     async def list_metadata_indexes(self):
    #         dialect_name = self.engine.url.get_dialect().name
    #         async with self.session() as db:
    #             if dialect_name == "sqlite":
    #                 statement = """
    # SELECT name, sql
    # FROM SQLite_master
    # WHERE type = 'index'
    # AND tbl_name = 'nodes'
    # AND name LIKE 'tiled_md_%'
    # ORDER BY tbl_name;
    #
    # """
    #             elif dialect_name == "postgresql":
    #                 statement = """
    # SELECT
    # indexname, indexdef
    # FROM
    # pg_indexes
    # WHERE tablename = 'nodes'
    # AND indexname LIKE 'tiled_md_%'
    # ORDER BY
    # indexname;
    #
    # """
    #             else:
    #                 raise NotImplementedError(
    #                     f"Cannot list indexes for dialect {dialect_name}"
    #                 )
    #             indexes = (
    #                 await db.execute(
    #                     text(statement),
    #                     explain=False,
    #                 )
    #             ).all()
    #         return indexes
    #
    #     async def create_metadata_index(self, index_name, key):
    #         """
    #
    #         References
    #         ----------
    #         https://scalegrid.io/blog/using-jsonb-in-postgresql-how-to-effectively-store-index-json-data-in-postgresql/
    #         https://pganalyze.com/blog/gin-index
    #         """
    #         dialect_name = self.engine.url.get_dialect().name
    #         if INDEX_PATTERN.match(index_name) is None:
    #             raise ValueError(f"Index name must match pattern {INDEX_PATTERN}")
    #         if dialect_name == "sqlite":
    #             expression = orm.Node.metadata_[key].as_string()
    #         elif dialect_name == "postgresql":
    #             expression = orm.Node.metadata_[key].label("md")
    #         else:
    #             raise NotImplementedError
    #         index = Index(
    #             f"tiled_md_{index_name}",
    #             "ancestors",
    #             expression,
    #             # postgresql_ops={"md": "jsonb_ops"},
    #             postgresql_using="gin",
    #         )
    #
    #         def create_index(connection):
    #             index.create(connection)
    #
    #         async with self.engine.connect() as connection:
    #             await connection.run_sync(create_index)
    #             await connection.commit()
    #
    #     async def drop_metadata_index(self, index_name):
    #         if INDEX_PATTERN.match(index_name) is None:
    #             raise ValueError(f"Index name must match pattern {INDEX_PATTERN}")
    #         if not index_name.startswith("tiled_md_"):
    #             index_name = f"tiled_md_{index_name}"
    #         async with self.session() as db:
    #             await db.execute(text(f"DROP INDEX {index_name};"), explain=False)
    #             await db.commit()
    #
    #     async def drop_all_metadata_indexes(self):
    #         indexes = await self.list_metadata_indexes()
    #         async with self.session() as db:
    #             for index_name, sql in indexes:
    #                 await db.execute(text(f"DROP INDEX {index_name};"), explain=False)
    #             await db.commit()

    async def _execute(self, statement, explain=None):
        "Debugging convenience utility, not exposed to server"
        async with self.session() as db:
            return await db.execute(text(statement), explain=explain)
            await db.commit()

    async def lookup(
        self, segments, principal=None
    ):  # TODO: Accept filter for predicate-pushdown.
        if not segments:
            return self
        *ancestors, key = segments
        statement = select(orm.Node).filter(
            orm.Node.ancestors == self.segments + ancestors
        )
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.session() as db:
            node = (await db.execute(statement.filter(orm.Node.key == key))).scalar()
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
        conditions=UNCHANGED,
        # must_revalidate=UNCHANGED,
        **kwargs,
    ):
        if sorting is UNCHANGED:
            sorting = self.sorting
        # if must_revalidate is UNCHANGED:
        #     must_revalidate = self.must_revalidate
        if conditions is UNCHANGED:
            conditions = self.conditions
        return type(self)(
            engine=self.engine,
            node=self.node,
            conditions=conditions,
            sorting=sorting,
            key_maker=self.key_maker,
            # access_policy=self.access_policy,
            # entries_stale_after=self.entries_stale_after,
            # metadata_stale_after=self.entries_stale_after,
            # must_revalidate=must_revalidate,
            **kwargs,
        )

    def search(self, query):
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    async def create_node(
        self,
        structure_family,
        metadata,
        key=None,
        specs=None,
        references=None,
        data_sources=None,
    ):
        key = key or self.key_maker()
        data_sources = data_sources or []
        node = orm.Node(
            key=key,
            ancestors=self.segments,
            metadata_=metadata,
            structure_family=structure_family,
            specs=specs or [],
            references=references or [],
        )
        # TODO Is there a way to insert related objects without
        # going back to commit/refresh so much?
        async with self.session() as db:
            db.add(node)
            await db.commit()
            await db.refresh(node)
            print(data_sources)
            for data_source in data_sources:
                data_source_orm = orm.DataSource(
                    node_id=node.id,
                    structure=data_source.structure.dict(),
                    mimetype=data_source.mimetype,
                    externally_managed=data_source.externally_managed,
                    parameters=data_source.parameters,
                )
                db.add(data_source_orm)
                await db.commit()
                await db.refresh(data_source_orm)
                for asset in data_source.assets:
                    asset_orm = orm.Asset(data_uri=asset.data_uri)
                    db.add(asset_orm)
                    await db.commit()

        # TODO Is this the right thing to return here?
        # Should we return anything at all?
        return self.from_orm(node)

    async def patch_node(datasources=None):
        ...

    async def keys_slice(self, start, stop, direction):
        if direction != 1:
            raise NotImplementedError
        statement = select(orm.Node.key).filter(orm.Node.ancestors == self.segments)
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.session() as db:
            return (
                (
                    await db.execute(
                        statement.order_by(*self.order_by_clauses)
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
        statement = select(orm.Node).filter(orm.Node.ancestors == self.segments)
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.session() as db:
            nodes = (
                (
                    await db.execute(
                        statement.order_by(*self.order_by_clauses)
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


_TYPE_CONVERSION_MAP = {
    int: "as_integer",
    float: "as_float",
    str: "as_string",
    bool: "as_boolean",
}


def _get_value(value, type):
    # This is used only for SQLite, to get the types right
    # so that that index is used. There is probably a
    # cleaner way to handle this.
    # Study https://gist.github.com/brthor/e3d23ae549ee53cdea56d72d39ad1288
    # which may or may not be relevant anymore.
    return getattr(value, _TYPE_CONVERSION_MAP[type])()


def eq(query, tree):
    dialect_name = tree.engine.url.get_dialect().name
    attr = orm.Node.metadata_[query.key.split(".")]
    if dialect_name == "sqlite":
        condition = _get_value(attr, type(query.value)) == query.value
    else:
        condition = attr == type_coerce(query.value, orm.Node.metadata_.type)
    return tree.new_variation(conditions=tree.conditions + [condition])


Adapter.register_query(Eq, eq)
