import base64
import collections
import importlib
import operator
import os
import re
import sys
import uuid
from functools import partial
from pathlib import Path
from urllib.parse import quote_plus

import httpx
from fastapi import HTTPException
from sqlalchemy import event, func, text, type_coerce
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select

from tiled.queries import (
    Comparison,
    Contains,
    Eq,
    In,
    KeysFilter,
    NotEq,
    NotIn,
    Operator,
)
from tiled.queries import StructureFamily as StructureFamilyQuery

from ..query_registration import QueryTranslationRegistry
from ..serialization.dataframe import XLSX_MIME_TYPE
from ..server.schemas import Management
from ..server.utils import ensure_awaitable
from ..structures.core import StructureFamily
from ..utils import UNCHANGED, OneShotCachedMap, UnsupportedQueryType, import_object
from . import orm
from .base import Base
from .explain import ExplainAsyncSession
from .node import Node

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))
INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
ZARR_MIMETYPE = "application/x-zarr"
PARQUET_MIMETYPE = "application/x-parquet"
SPARSE_BLOCKS_PARQUET_MIMETYPE = "application/x-parquet-sparse"  # HACK!

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
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter,
    }
)
DEFAULT_CREATION_MIMETYPE = {
    "array": ZARR_MIMETYPE,
    "dataframe": PARQUET_MIMETYPE,
    "sparse": SPARSE_BLOCKS_PARQUET_MIMETYPE,
}
CREATE_ADAPTER_BY_MIMETYPE = OneShotCachedMap(
    {
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "...adapters.zarr", __name__
        ).ZarrAdapter.init_storage,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.parquet", __name__
        ).ParquetDatasetAdapter.init_storage,
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter.init_storage,
    }
)


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


class RootNode:
    """
    Node representing the root of the tree.

    This node is special because the state configuring this node arises from
    server initialization (typically a configuration file) not from the
    database.

    It mocks the relevant part of the interface of .orm.Node.
    """

    structure_family = StructureFamily.node

    def __init__(self, metadata, specs, references, access_policy):
        self.metadata = metadata or {}
        self.specs = specs or []
        self.references = references or []
        self.ancestors = []
        self.key = None
        self.data_sources = None


class Context:
    def __init__(
        self,
        engine,
        writable_storage=None,
        readable_storage=None,
        adapters_by_mimetype=None,
        mimetype_detection_hook=None,
        key_maker=lambda: str(uuid.uuid4()),
    ):
        self.engine = engine
        readable_storage = readable_storage or []
        if writable_storage:
            writable_storage = httpx.URL(str(writable_storage))
            if not writable_storage.scheme:
                writable_storage = writable_storage.copy_with(scheme="file")
            if not writable_storage.scheme == "file":
                raise NotImplementedError(
                    "Only file://... writable storage is currently supported."
                )
            # If it is writable, it is automatically also readable.
            readable_storage.append(writable_storage.path)
        self.writable_storage = writable_storage
        self.readable_storage = [Path(p).absolute() for p in readable_storage or []]
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

    async def execute(self, statement, explain=None):
        "Debugging convenience utility, not exposed to server"
        async with self.session() as db:
            result = await db.execute(text(statement), explain=explain)
            await db.commit()
            return result


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
        initialize_database_at_startup=False,
    ):
        self.context = context
        self.engine = self.context.engine
        self.node = node
        if node.key is None:
            # Special case for RootNode
            self.segments = []
        else:
            self.segments = node.ancestors + [node.key]
        self.sorting = sorting or [("", 1)]
        self.order_by_clauses = order_by_clauses(self.sorting)
        self.conditions = conditions or []
        self.structure_family = node.structure_family
        self.metadata = node.metadata
        self.specs = node.specs
        self.references = node.references
        self.access_policy = access_policy
        self.initialize_database_at_startup = initialize_database_at_startup
        self.startup_tasks = [self.startup]
        self.shutdown_tasks = [self.shutdown]

    async def startup(self):
        if self.initialize_database_at_startup:
            await initialize_database(self.context.engine)

    async def shutdown(self):
        await self.context.engine.dispose()

    @property
    def writable(self):
        return bool(self.context.writable_storage)

    def __repr__(self):
        return f"<{type(self).__name__} {self.segments}>"


class UnallocatedAdapter(BaseAdapter):
    # Raise clear error if you try to read or write chunks.
    pass


class CatalogNodeAdapter(BaseAdapter):
    async def __aiter__(self):
        statement = select(orm.Node.key).filter(orm.Node.ancestors == self.segments)
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            return (
                (await db.execute(statement.order_by(*self.order_by_clauses)))
                .scalars()
                .all()
            )
        statement = select(orm.Node.key).filter(orm.Node.ancestors == self.segments)
        async with self.context.session() as db:
            return (await db.execute(statement)).scalar().all()

    async def async_len(self):
        statement = select(func.count(orm.Node.key)).filter(
            orm.Node.ancestors == self.segments
        )
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            return (await db.execute(statement)).scalar_one()

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
        return Node.from_orm(
            node, self.context, access_policy=self.access_policy, sorting=self.sorting
        )

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
            for data_uri in data_uris:
                if data_uri.scheme == "file":
                    # Protect against misbehaving clients reading from unintended
                    # parts of the filesystem.
                    for readable_storage in self.context.readable_storage:
                        if (
                            Path(os.path.commonpath([readable_storage, data_uri.path]))
                            == readable_storage
                        ):
                            break
                    else:
                        raise RuntimeError(
                            f"Refusing to serve {data_uri} because it is outside "
                            "the readable storage area for this server."
                        )
            paths = []
            for data_uri in data_uris:
                if data_uri.scheme != "file":
                    raise NotImplementedError(
                        f"Only 'file://...' scheme URLs are currently supported, not {data_uri!r}"
                    )
                paths.append(data_uri.path)
            kwargs = dict(data_source.parameters)
            kwargs["specs"] = node.specs
            kwargs["metadata"] = node.metadata
            if node.structure_family == StructureFamily.array:
                # kwargs["dtype"] = data_source.structure.micro.to_numpy_dtype()
                kwargs["shape"] = data_source.structure.macro.shape
                kwargs["chunks"] = data_source.structure.macro.chunks
                kwargs["dims"] = node.structure.macro.dims
            elif node.structure_family == StructureFamily.dataframe:
                kwargs["meta"] = data_source.structure.micro.meta_decoded
                kwargs["divisions"] = data_source.structure.micro.divisions_decoded
            elif node.structure_family == StructureFamily.sparse:
                kwargs["chunks"] = data_source.structure.chunks
                kwargs["shape"] = data_source.structure.shape
                kwargs["dims"] = data_source.structure.dims
            else:
                raise NotImplementedError(node.structure_family)
            adapter = adapter_factory(*paths, **kwargs)
            adapter.access_policy = self.access_policy  # HACK
            return adapter
        else:  # num_data_sources == 0
            if node.structure_family != StructureFamily.node:
                raise NotImplementedError  # array or dataframe that is uninitialized
            # A node with no underlying data source
            return type(self)(self.context, node, access_policy=self.access_policy)

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
            specs=[s.dict() for s in specs or []],
            references=[r.dict() for r in references or []],
        )
        async with self.context.session() as db:
            # TODO Consider using nested transitions to ensure that
            # both the node is created (name not already taken)
            # and the directory/file is created---or neither are.
            try:
                db.add(node)
                await db.commit()
            except IntegrityError as exc:
                UNIQUE_CONSTRAINT_FAILED = "gkpj"
                if exc.code == UNIQUE_CONSTRAINT_FAILED:
                    await db.rollback()
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Cannot create new node here because path is already taken: "
                            f"{'/'.join(self.segments + [key])}"
                        ),
                    )
                raise
            await db.refresh(node)
            for data_source in data_sources:
                if data_source.management != Management.external:
                    data_source.mimetype = DEFAULT_CREATION_MIMETYPE[structure_family]
                    data_source.parameters = {}
                    data_uri = str(self.context.writable_storage) + "".join(
                        f"/{quote_plus(segment)}" for segment in (self.segments + [key])
                    )
                    init_storage = CREATE_ADAPTER_BY_MIMETYPE[data_source.mimetype]
                    if structure_family == StructureFamily.array:
                        init_storage_args = (
                            httpx.URL(data_uri).path,
                            data_source.structure.micro.to_numpy_dtype(),
                            data_source.structure.macro.shape,
                            data_source.structure.macro.chunks,
                        )
                    elif structure_family == StructureFamily.dataframe:
                        init_storage_args = (
                            httpx.URL(data_uri).path,
                            data_source.structure.macro.npartitions,
                        )
                    elif structure_family == StructureFamily.sparse:
                        init_storage_args = (
                            httpx.URL(data_uri).path,
                            data_source.structure.chunks,
                        )
                    else:
                        raise NotImplementedError(structure_family)
                    assets = await ensure_awaitable(init_storage, *init_storage_args)
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
                # await db.flush(data_source_orm)
                for asset in data_source.assets:
                    asset_orm = orm.Asset(
                        data_uri=asset.data_uri,
                        is_directory=asset.is_directory,
                    )
                    data_source_orm.assets.append(asset_orm)
            db.add(node)
            await db.commit()
            await db.refresh(node)
            return key, Node.from_orm(
                node,
                self.context,
                access_policy=self.access_policy,
                sorting=self.sorting,
            )

    # async def patch_node(datasources=None):
    #     ...

    async def read(self, fields=None):
        return self.search(KeysFilter(fields))

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
            return [
                (
                    node.key,
                    Node.from_orm(node, self.context, access_policy=self.access_policy),
                )
                for node in nodes
            ]

    async def adapters_range(self, offset, limit):
        items = await self.items_range(offset, limit)
        return [(key, self.adapter_from_node(value)) for key, value in items]


_STANDARD_SORT_KEYS = {
    "id": "key",
    # Maybe add things like structure_family here, but it's not clear what the
    # sort order would be.
}


def order_by_clauses(sorting):
    clauses = []
    default_sorting_direction = 1
    for key, direction in sorting:
        if key == "":
            default_sorting_direction = direction
            continue
            # TODO Really we should insist that if this is given, it is last,
            # because we always apply the default sorting last.
        if key in _STANDARD_SORT_KEYS:
            clause = getattr(orm.Node, _STANDARD_SORT_KEYS[key])
        else:
            clause = orm.Node.metadata_
            # This can be given bare like "color" or namedspaced like
            # "metadata.color" to avoid the possibility of a name collision
            # with the standard sort keys.
            if key.startswith("metadata."):
                key = key[len("metadata.") :]  # noqa: E203

            for segment in key.split("."):
                clause = clause[segment]
        if direction == -1:
            clause = clause.desc()
        clauses.append(clause)
    # Ensure deterministic ordering for all queries by sorting by
    # 'time_created' and then by 'id' last.
    for clause in [orm.Node.time_created, orm.Node.id]:
        if default_sorting_direction == -1:
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


def binary_op(query, tree, operation):
    dialect_name = tree.engine.url.get_dialect().name
    attr = orm.Node.metadata_[query.key.split(".")]
    if dialect_name == "sqlite":
        condition = operation(_get_value(attr, type(query.value)), query.value)
    else:
        condition = operation(attr, type_coerce(query.value, orm.Node.metadata_.type))
    return tree.new_variation(conditions=tree.conditions + [condition])


def comparison(query, tree):
    OPERATORS = {
        Operator.lt: operator.lt,
        Operator.le: operator.le,
        Operator.gt: operator.gt,
        Operator.ge: operator.ge,
    }
    return binary_op(query, tree, OPERATORS[query.operator])


def contains(query, tree):
    dialect_name = tree.engine.url.get_dialect().name
    attr = orm.Node.metadata_[query.key.split(".")]
    if dialect_name == "sqlite":
        condition = _get_value(attr, type(query.value)).contains(query.value)
    else:
        raise UnsupportedQueryType("Contains")
    return tree.new_variation(conditions=tree.conditions + [condition])


def specs(query, tree):
    raise UnsupportedQueryType("Specs")
    # conditions = []
    # for spec in query.include:
    #     conditions.append(func.json_contains(orm.Node.specs, spec))
    # for spec in query.exclude:
    #     conditions.append(not_(func.json_contains(orm.Node.specs.contains, spec)))
    # return tree.new_variation(conditions=tree.conditions + conditions)


def in_or_not_in(query, tree, method):
    dialect_name = tree.engine.url.get_dialect().name
    attr = orm.Node.metadata_[query.key.split(".")]
    if dialect_name == "sqlite":
        condition = getattr(_get_value(attr, type(query.value[0])), method)(query.value)
    else:
        raise UnsupportedQueryType("In/NotIn")
    return tree.new_variation(conditions=tree.conditions + [condition])


def keys_filter(query, tree):
    condition = orm.Node.key.in_(query.keys)
    return tree.new_variation(conditions=tree.conditions + [condition])


def structure_family(query, tree):
    condition = orm.Node.structure_family == query.value
    return tree.new_variation(conditions=tree.conditions + [condition])


CatalogNodeAdapter.register_query(Eq, partial(binary_op, operation=operator.eq))
CatalogNodeAdapter.register_query(NotEq, partial(binary_op, operation=operator.ne))
CatalogNodeAdapter.register_query(Comparison, comparison)
CatalogNodeAdapter.register_query(Contains, contains)
CatalogNodeAdapter.register_query(In, partial(in_or_not_in, method="in_"))
CatalogNodeAdapter.register_query(NotIn, partial(in_or_not_in, method="not_in"))
CatalogNodeAdapter.register_query(KeysFilter, keys_filter)
CatalogNodeAdapter.register_query(StructureFamilyQuery, structure_family)
# CatalogNodeAdapter.register_query(Specs, specs)
# TODO: FullText, Regex, Specs


def in_memory(
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    readable_storage=None,
    echo=DEFAULT_ECHO,
):
    uri = "sqlite+aiosqlite:///:memory:"
    return from_uri(
        uri=uri,
        metadata=metadata,
        specs=specs,
        references=references,
        access_policy=access_policy,
        writable_storage=writable_storage,
        readable_storage=readable_storage,
        echo=echo,
        # An in-memory database will always need initialization.
        initialize_database_at_startup=True,
    )


def from_uri(
    uri,
    metadata=None,
    specs=None,
    references=None,
    access_policy=None,
    writable_storage=None,
    readable_storage=None,
    echo=DEFAULT_ECHO,
    initialize_database_at_startup=False,
):
    engine = create_async_engine(uri, echo=echo)
    if engine.dialect.name == "sqlite":
        event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragma)
    return CatalogNodeAdapter(
        Context(engine, writable_storage, readable_storage),
        RootNode(metadata, specs, references, access_policy),
        initialize_database_at_startup=initialize_database_at_startup,
        access_policy=access_policy,
    )


def _set_sqlite_pragma(conn, record):
    cursor = conn.cursor()
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#foreign-key-support
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
