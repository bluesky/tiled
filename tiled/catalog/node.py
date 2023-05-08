import asyncio
import base64
import collections
import importlib
import os
import re
import sys
import uuid
from pathlib import Path
from urllib.parse import quote_plus

import httpx
from sqlalchemy import text, type_coerce
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select

from ..queries import Eq
from ..query_registration import QueryTranslationRegistry
from ..serialization.dataframe import XLSX_MIME_TYPE
from ..server.schemas import Asset, Management, Node
from ..server.utils import ensure_awaitable
from ..structures.core import StructureFamily
from ..utils import UNCHANGED, OneShotCachedMap, import_object
from . import orm
from .base import Base
from .explain import ExplainAsyncSession

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))
INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
ZARR_MIMETYPE = "application/x-zarr"
PARQUET_MIMETYPE = "application/x-parquet"

# This maps MIME types (i.e. file formats) for appropriate Readers.
# OneShotCachedMap is used to defer imports. We don't want to pay up front
# for importing Readers that we will not actually use.
DEFAULT_ADAPTERS_BY_MIMETYPE = OneShotCachedMap(
    {
        "image/tiff": lambda: importlib.import_module(
            "...adapters.tiff", __name__
        ).TiffAdapter,
        "text/csv": lambda: importlib.import_module(
            "...adapters.dataframe", __name__
        ).DataFrameAdapter.read_csv,
        XLSX_MIME_TYPE: lambda: importlib.import_module(
            "...adapters.excel", __name__
        ).ExcelAdapter.from_file,
        "application/x-hdf5": lambda: importlib.import_module(
            "...adapters.hdf5", __name__
        ).HDF5Adapter.from_file,
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "...adapters.zarr", __name__
        ).ZarrAdapter.from_directory,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.parquet", __name__
        ).ParquetDatasetAdapter,
    }
)
DEFAULT_CREATION_MIMETYPE = {
    "array": ZARR_MIMETYPE,
    "dataframe": PARQUET_MIMETYPE,
    "sparse": PARQUET_MIMETYPE,
}


def from_uri(
    database_uri,
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    echo=DEFAULT_ECHO,
):
    "Connect to an existing database."
    # TODO Check that database exists and has the expected alembic revision.
    engine = create_async_engine(database_uri, echo=echo)
    return NodeAdapter(
        Context(engine, writable_storage),
        RootNode(metadata, specs, references, access_policy),
    )


def create_from_uri(
    database_uri,
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    echo=DEFAULT_ECHO,
):
    "Create a new database and connect to it."
    engine = create_async_engine(database_uri, echo=echo)
    asyncio.run(initialize_database(engine))
    return NodeAdapter(
        Context(engine, writable_storage),
        RootNode(metadata, specs, references, access_policy),
        new_database=True,
    )


def async_create_from_uri(
    database_uri,
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    echo=DEFAULT_ECHO,
):
    "Create a new database and connect to it."
    engine = create_async_engine(database_uri, echo=echo)
    return NodeAdapter(
        Context(engine, writable_storage),
        RootNode(metadata, specs, references, access_policy),
        new_database=True,
    )


def in_memory(
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    echo=DEFAULT_ECHO,
):
    "Create a transient database in memory."
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
    asyncio.run(initialize_database(engine))
    return NodeAdapter(
        Context(engine, writable_storage),
        RootNode(metadata, specs, references, access_policy),
        new_database=True,
    )


def async_in_memory(
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    echo=DEFAULT_ECHO,
):
    "Create a transient database in memory."
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=echo)
    return NodeAdapter(
        Context(engine, writable_storage),
        RootNode(metadata, specs, references, access_policy),
        new_database=True,
    )


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

    structure_family = StructureFamily.node

    def __init__(self, metadata, specs, references, access_policy):
        self.metadata = metadata or {}
        self.specs = specs or []
        self.references = references or []
        self.ancestors = []
        self.key = None
        self.time_created = None
        self.time_updated = None
        self.data_sources = None


class Context:
    def __init__(
        self,
        engine,
        writable_storage=None,
        adapters_by_mimetype=None,
        mimetype_detection_hook=None,
        key_maker=lambda: str(uuid.uuid4()),
    ):
        self.engine = engine
        if writable_storage:
            writable_storage = httpx.URL(writable_storage)
            if not writable_storage.scheme:
                writable_storage = writable_storage.copy_with(scheme="file")
            if not writable_storage.scheme == "file":
                raise NotImplementedError(
                    "Only file://... writable storage is currently supported."
                )
        self.writable_storage = writable_storage
        self.key_maker = key_maker
        adapters_by_mimetype = adapters_by_mimetype or {}
        if mimetype_detection_hook is not None:
            mimetype_detection_hook = import_object(mimetype_detection_hook)
        # If adapters_by_mimetype comes from a configuration file,
        # objects are given as importable strings, like "package.module:Reader".
        for key, value in list(adapters_by_mimetype.items()):
            if isinstance(value, str):
                adapters_by_mimetype[key] = import_object(value)
        # User-provided adapters take precedence over defaults.
        merged_adapters_by_mimetype = collections.ChainMap(
            adapters_by_mimetype, DEFAULT_ADAPTERS_BY_MIMETYPE
        )
        self.adapters_by_mimetype = merged_adapters_by_mimetype

    def session(self):
        "Convenience method for constructing an AsyncSession context"
        return ExplainAsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    # This is heading in a reasonable direction but does not actually work yet.
    # Pausing development for now.
    #
    #     async def list_metadata_indexes(self):
    #         dialect_name = self.engine.url.get_dialect().name
    #         async with self.context.session() as db:
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
    #         async with self.context.session() as db:
    #             await db.execute(text(f"DROP INDEX {index_name};"), explain=False)
    #             await db.commit()
    #
    #     async def drop_all_metadata_indexes(self):
    #         indexes = await self.list_metadata_indexes()
    #         async with self.context.session() as db:
    #             for index_name, sql in indexes:
    #                 await db.execute(text(f"DROP INDEX {index_name};"), explain=False)
    #             await db.commit()

    async def _execute(self, statement, explain=None):
        "Debugging convenience utility, not exposed to server"
        async with self.context.session() as db:
            return await db.execute(text(statement), explain=explain)
            await db.commit()


class BaseAdapter:
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        context,
        node,
        *,
        conditions=None,
        sorting=None,
        access_policy=None,
        new_database=False,
    ):
        self.context = context
        self.engine = self.context.engine
        self.node = node
        if node.key is None:
            # Special case for RootNode
            self.segments = []
        else:
            self.segments = node.ancestors + [node.key]
        self.sorting = sorting or [("time_created", 1)]
        self.order_by_clauses = order_by_clauses(self.sorting)
        self.conditions = conditions or []
        self.structure_family = node.structure_family
        self.metadata = node.metadata
        self.specs = node.specs
        self.references = node.references
        self.time_creatd = node.time_created
        self.time_updated = node.time_updated
        self.access_policy = access_policy
        self.new_database = new_database

    @property
    def writable(self):
        return bool(self.context.writable_storage)

    def __repr__(self):
        return f"<{type(self).__name__} {self.segments}>"

    async def __aenter__(self):
        if self.new_database:
            await initialize_database(self.engine)
        return self

    async def __aexit__(self, *args):
        await self.engine.dispose()


class UnallocatedAdapter(BaseAdapter):
    # Raise clear error if you try to read or write chunks.
    pass


class NodeAdapter(BaseAdapter):
    async def lookup_node(
        self, segments
    ):  # TODO: Accept filter for predicate-pushdown.
        if not segments:
            return self
        *ancestors, key = segments
        statement = select(orm.Node).filter(
            orm.Node.ancestors == self.segments + ancestors
        )
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            node = (await db.execute(statement.filter(orm.Node.key == key))).scalar()
            # TODO Do binary search or use some tricky JSON query to find where
            # database stops and (for example) HDF5 file begins.
        if node is None:
            return None
        return Node.from_orm(node, sorting=self.sorting)

    async def lookup_adapter(
        self, segments
    ):  # TODO: Accept filter for predicate-pushdown.
        node = await self.lookup_node(segments)
        if node is None:
            return None
        return self.adapter_from_node(node)

    def adapter_from_node(self, node):
        num_data_sources = len(node.data_sources)
        if num_data_sources > 1:
            raise NotImplementedError
        if num_data_sources == 1:
            (data_source,) = node.data_sources
            adapter_factory = self.context.adapters_by_mimetype[data_source.mimetype]
            data_uris = [httpx.URL(asset.data_uri) for asset in data_source.assets]
            paths = []
            for data_uri in data_uris:
                if data_uri.scheme != "file":
                    raise NotImplementedError(
                        f"Only 'file://...'.scheme URLs are currently supported, not {data_uri!r}"
                    )
                paths.append(data_uri.path)
            kwargs = dict(data_source.parameters)
            if node.structure_family == StructureFamily.array:
                # kwargs["dtype"] = data_source.structure.micro.to_numpy_dtype()
                kwargs["shape"] = data_source.structure.macro.shape
                kwargs["chunks"] = data_source.structure.macro.chunks
            elif node.structure_family == StructureFamily.dataframe:
                kwargs["npartitions"] = data_source.structure.macro.npartitions
                kwargs["meta"] = data_source.structure.micro.meta
                kwargs["divisions"] = data_source.structure.micro.divisions
            adapter = adapter_factory(*paths, **kwargs)
            return adapter
        else:  # num_data_sources == 0
            if node.structure_family != StructureFamily.node:
                raise NotImplementedError  # array or dataframe that is uninitialized
            # A node with no underlying data source
            return NodeAdapter(self.context, node)

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
            self.context,
            node=self.node,
            conditions=conditions,
            sorting=sorting,
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
        key = key or self.context.key_maker()
        data_sources = data_sources or []
        node = orm.Node(
            key=key,
            ancestors=self.segments,
            metadata_=metadata,
            structure_family=structure_family,
            specs=specs or [],
            references=references or [],
        )
        async with self.context.session() as db:
            for data_source in data_sources:
                if data_source.management != Management.external:
                    data_source.mimetype = DEFAULT_CREATION_MIMETYPE[structure_family]
                    data_source.parameters = {}
                    data_uri = str(self.context.writable_storage) + "".join(
                        f"/{quote_plus(segment)}" for segment in (self.segments + [key])
                    )
                    adapter = self.context.adapters_by_mimetype[data_source.mimetype]
                    assets = []
                    if structure_family == StructureFamily.array:
                        assets.append(Asset(data_uri=data_uri, is_directory=True))
                        init_storage_args = (
                            httpx.URL(data_uri).path,
                            data_source.structure.micro.to_numpy_dtype(),
                            data_source.structure.macro.shape,
                            data_source.structure.macro.chunks,
                        )
                    if structure_family == StructureFamily.dataframe:
                        for i in range(data_source.structure.macro.npartitions):
                            assets.append(
                                Asset(
                                    data_uri=data_uri + f"/partition-{i}",
                                    is_directory=False,
                                )
                            )
                        init_storage_args = (httpx.URL(data_uri).path,)
                    await ensure_awaitable(adapter.init_storage, *init_storage_args)
                    data_source.assets.extend(assets)
                data_source_orm = orm.DataSource(
                    structure=_prepare_structure(
                        structure_family, data_source.structure
                    ),
                    mimetype=data_source.mimetype,
                    management=data_source.management,
                    parameters=data_source.parameters,
                )
                node.data_sources.append(data_source_orm)
                await db.flush(data_source_orm)
                for asset in data_source.assets:
                    asset_orm = orm.Asset(
                        data_uri=asset.data_uri,
                        is_directory=asset.is_directory,
                    )
                    data_source_orm.assets.append(asset_orm)
            db.add(node)
            await db.commit()
            await db.refresh(node)
            return key, Node.from_orm(node, sorting=self.sorting)

    async def patch_node(datasources=None):
        ...

    async def keys_range(self, offset, limit):
        statement = select(orm.Node.key).filter(orm.Node.ancestors == self.segments)
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            return (
                (
                    await db.execute(
                        statement.order_by(*self.order_by_clauses)
                        .offset(offset)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )

    async def items_range(self, offset, limit):
        statement = select(orm.Node).filter(orm.Node.ancestors == self.segments)
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            nodes = (
                (
                    await db.execute(
                        statement.order_by(*self.order_by_clauses)
                        .offset(offset)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
            return [(node.key, Node.from_orm(node)) for node in nodes]


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


def _safe_path(path):
    if sys.platform == "win32" and path[0] == "/":
        path = path[1:]
    return Path(path)


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


def _prepare_structure(structure_family, structure):
    "Convert from pydantic model to dict and base64-encode binary values."
    structure = structure.dict()
    if structure_family == StructureFamily.dataframe:
        structure["micro"]["meta"] = base64.b64encode(
            structure["micro"]["meta"]
        ).decode()
        structure["micro"]["divisions"] = base64.b64encode(
            structure["micro"]["divisions"]
        ).decode()
    return structure


def eq(query, tree):
    dialect_name = tree.engine.url.get_dialect().name
    attr = orm.Node.metadata_[query.key.split(".")]
    if dialect_name == "sqlite":
        condition = _get_value(attr, type(query.value)) == query.value
    else:
        condition = attr == type_coerce(query.value, orm.Node.metadata_.type)
    return tree.new_variation(conditions=tree.conditions + [condition])


NodeAdapter.register_query(Eq, eq)
