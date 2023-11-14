import collections
import importlib
import operator
import os
import re
import shutil
import sys
import uuid
from functools import partial, reduce
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import anyio
import httpx
from fastapi import HTTPException
from sqlalchemy import delete, event, func, select, text, type_coerce, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from tiled.queries import (
    Comparison,
    Contains,
    Eq,
    In,
    KeysFilter,
    NotEq,
    NotIn,
    Operator,
    StructureFamilyQuery,
)

from ..query_registration import QueryTranslationRegistry
from ..server.schemas import DataSource, Management, Revision, Spec
from ..structures.core import StructureFamily
from ..utils import (
    UNCHANGED,
    Conflicts,
    OneShotCachedMap,
    UnsupportedQueryType,
    ensure_awaitable,
    import_object,
    safe_json_dump,
)
from . import orm
from .core import check_catalog_database, initialize_database
from .explain import ExplainAsyncSession
from .mimetypes import (
    DEFAULT_ADAPTERS_BY_MIMETYPE,
    PARQUET_MIMETYPE,
    SPARSE_BLOCKS_PARQUET_MIMETYPE,
    ZARR_MIMETYPE,
    ZIP_MIMETYPE,
)
from .utils import SCHEME_PATTERN, ensure_uri, safe_path

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))
INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

DEFAULT_CREATION_MIMETYPE = {
    StructureFamily.array: ZARR_MIMETYPE,
    StructureFamily.awkward: ZIP_MIMETYPE,
    StructureFamily.table: PARQUET_MIMETYPE,
    StructureFamily.sparse: SPARSE_BLOCKS_PARQUET_MIMETYPE,
}
CREATE_ADAPTER_BY_MIMETYPE = OneShotCachedMap(
    {
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "...adapters.zarr", __name__
        ).ZarrArrayAdapter.init_storage,
        ZIP_MIMETYPE: lambda: importlib.import_module(
            "...adapters.awkward_buffers", __name__
        ).AwkwardBuffersAdapter.init_storage,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.parquet", __name__
        ).ParquetDatasetAdapter.init_storage,
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter.init_storage,
    }
)


class RootNode:
    """
    Node representing the root of the tree.

    This node is special because the state configuring this node arises from
    server initialization (typically a configuration file) not from the
    database.

    It mocks the relevant part of the interface of .orm.Node.
    """

    structure_family = StructureFamily.container

    def __init__(self, metadata, specs, access_policy):
        self.metadata_ = metadata or {}
        self.specs = [Spec.parse_obj(spec) for spec in specs or []]
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
        key_maker=lambda: str(uuid.uuid4()),
    ):
        self.engine = engine
        readable_storage = readable_storage or []
        if not isinstance(readable_storage, list):
            raise ValueError("readable_storage should be a list of URIs or paths")
        if writable_storage:
            writable_storage = ensure_uri(str(writable_storage))
            if not writable_storage.scheme == "file":
                raise NotImplementedError(
                    "Only file://... writable storage is currently supported."
                )
            # If it is writable, it is automatically also readable.
            readable_storage.append(writable_storage)
        self.writable_storage = writable_storage
        self.readable_storage = [ensure_uri(path) for path in readable_storage]
        self.key_maker = key_maker
        adapters_by_mimetype = adapters_by_mimetype or {}
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


class CatalogNodeAdapter:
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        context,
        node,
        *,
        conditions=None,
        queries=None,
        sorting=None,
        access_policy=None,
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
        self.queries = queries or []
        self.structure_family = node.structure_family
        self.specs = [Spec.parse_obj(spec) for spec in node.specs]
        self.ancestors = node.ancestors
        self.key = node.key
        self.access_policy = access_policy
        self.startup_tasks = [self.startup]
        self.shutdown_tasks = [self.shutdown]

    def metadata(self):
        return self.node.metadata_

    async def startup(self):
        if (self.context.engine.dialect.name == "sqlite") and (
            self.context.engine.url.database == ":memory:"
        ):
            # Special-case for in-memory SQLite: Because it is transient we can
            # skip over anything related to migrations.
            await initialize_database(self.context.engine)
        else:
            await check_catalog_database(self.context.engine)

    async def shutdown(self):
        await self.context.engine.dispose()

    @property
    def writable(self):
        return bool(self.context.writable_storage)

    def __repr__(self):
        return f"<{type(self).__name__} {self.segments}>"

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

    @property
    def data_sources(self):
        return [DataSource.from_orm(ds) for ds in self.node.data_sources or []]

    def structure(self):
        if self.data_sources:
            assert len(self.data_sources) == 1  # more not yet implemented
            return self.data_sources[0].structure
        return None

    async def async_len(self):
        statement = select(func.count(orm.Node.key)).filter(
            orm.Node.ancestors == self.segments
        )
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            return (await db.execute(statement)).scalar_one()

    async def lookup_adapter(
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
        if node is None:
            # Maybe the node does not exist, or maybe we have jumped _inside_ a file
            # whose internal contents are not indexed.

            # TODO As a performance optimization, do binary search or use some
            # tricky JSON query to find where database stops and (for example)
            # HDF5 file begins.

            for i in range(len(segments)):
                catalog_adapter = await self.lookup_adapter(segments[:i])
                if catalog_adapter.data_sources:
                    adapter = await catalog_adapter.get_adapter()
                    for segment in segments[i:]:
                        adapter = await anyio.to_thread.run_sync(adapter.get, segment)
                        if adapter is None:
                            break
                    return adapter
            return None
        return STRUCTURES[node.structure_family](
            self.context, node, access_policy=self.access_policy
        )

    async def get_adapter(self):
        num_data_sources = len(self.data_sources)
        if num_data_sources > 1:
            raise NotImplementedError
        if num_data_sources == 1:
            (data_source,) = self.data_sources
            try:
                adapter_factory = self.context.adapters_by_mimetype[
                    data_source.mimetype
                ]
            except KeyError:
                raise RuntimeError(
                    f"Server configuration has no adapter for mimetype {data_source.mimetype!r}"
                )
            data_uris = [httpx.URL(asset.data_uri) for asset in data_source.assets]
            for data_uri in data_uris:
                if data_uri.scheme == "file":
                    # Protect against misbehaving clients reading from unintended
                    # parts of the filesystem.
                    for readable_storage in self.context.readable_storage:
                        if Path(
                            os.path.commonpath(
                                [safe_path(readable_storage), safe_path(data_uri)]
                            )
                        ) == safe_path(readable_storage):
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
                paths.append(safe_path(data_uri))
            adapter_kwargs = dict(data_source.parameters)
            adapter_kwargs["specs"] = self.node.specs
            adapter_kwargs["metadata"] = self.node.metadata_
            adapter_kwargs["structure"] = data_source.structure
            adapter_kwargs["access_policy"] = self.access_policy
            adapter = await anyio.to_thread.run_sync(
                partial(adapter_factory, *paths, **adapter_kwargs)
            )
            for query in self.queries:
                adapter = adapter.search(query)
            return adapter
        else:  # num_data_sources == 0
            assert False

    def new_variation(
        self,
        *args,
        sorting=UNCHANGED,
        conditions=UNCHANGED,
        queries=UNCHANGED,
        # must_revalidate=UNCHANGED,
        **kwargs,
    ):
        if sorting is UNCHANGED:
            sorting = self.sorting
        # if must_revalidate is UNCHANGED:
        #     must_revalidate = self.must_revalidate
        if conditions is UNCHANGED:
            conditions = self.conditions
        if queries is UNCHANGED:
            queries = self.queries
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
        if self.data_sources:
            # Stand queries, which will be passed down to the adapter
            # when / if get_adapter() is called.
            self.queries.append(query)
            return self
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    async def get_distinct(self, metadata, structure_families, specs, counts):
        if self.data_sources:
            return (await self.get_adapter()).get_disinct(
                metadata, structure_families, specs, counts
            )
        data = {}

        async with self.context.session() as db:
            if metadata:
                data["metadata"] = {}
                for key in metadata:
                    clause = orm.Node.metadata_
                    for segment in key.split("."):
                        clause = clause[segment]
                    if counts:
                        columns = (clause, func.count(clause))
                    else:
                        columns = (clause,)
                    statement = select(*columns).group_by(clause)
                    for condition in self.conditions:
                        statement = statement.filter(condition)
                    results = (await db.execute(statement)).all()
                    data["metadata"][key] = format_distinct_result(results, counts)

            if structure_families:
                if counts:
                    columns = (
                        orm.Node.structure_family,
                        func.count(orm.Node.structure_family),
                    )
                else:
                    columns = (orm.Node.structure_family,)
                statement = select(*columns).group_by(orm.Node.structure_family)
                for condition in self.conditions:
                    statement = statement.filter(condition)
                results = (await db.execute(statement)).all()

                data["structure_families"] = format_distinct_result(results, counts)

            if specs:
                if counts:
                    columns = (orm.Node.specs, func.count(orm.Node.specs))
                else:
                    columns = (orm.Node.specs,)
                statement = select(*columns).group_by(orm.Node.specs)
                for condition in self.conditions:
                    statement = statement.filter(condition)
                results = (await db.execute(statement)).all()

                data["specs"] = format_distinct_result(results, counts)

        return data

    async def create_node(
        self,
        structure_family,
        metadata,
        key=None,
        specs=None,
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
                    raise Collision(f"/{'/'.join(self.segments + [key])}")
                raise
            await db.refresh(node)
            for data_source in data_sources:
                if data_source.management != Management.external:
                    if structure_family == StructureFamily.container:
                        raise NotImplementedError(structure_family)
                    data_source.mimetype = DEFAULT_CREATION_MIMETYPE[structure_family]
                    data_source.parameters = {}
                    data_uri = str(self.context.writable_storage) + "".join(
                        f"/{quote_plus(segment)}" for segment in (self.segments + [key])
                    )
                    init_storage = CREATE_ADAPTER_BY_MIMETYPE[data_source.mimetype]
                    assets = await ensure_awaitable(
                        init_storage, safe_path(data_uri), data_source.structure
                    )
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
            return key, type(self)(self.context, node, access_policy=self.access_policy)

    # async def patch_node(datasources=None):
    #     ...

    async def revisions(self, offset, limit):
        async with self.context.session() as db:
            revision_orms = (
                await db.execute(
                    select(orm.Revision)
                    .where(orm.Revision.node_id == self.node.id)
                    .offset(offset)
                    .limit(limit)
                )
            ).all()
            return [Revision.from_orm(o[0]) for o in revision_orms]

    async def delete(self):
        """
        Delete a single Node.

        Any DataSources belonging to this Node and any Assets associated (only) with
        those DataSources will also be deleted.
        """
        async with self.context.session() as db:
            is_child = orm.Node.ancestors == self.ancestors + [self.key]
            num_children = (
                await db.execute(select(func.count(orm.Node.key)).where(is_child))
            ).scalar()
            if num_children:
                raise Conflicts(
                    "Cannot delete container that is not empty. Delete contents first."
                )
            for data_source in self.data_sources:
                if data_source.management != Management.external:
                    # TODO Handle case where the same Asset is associated
                    # with multiple DataSources. This is not possible yet
                    # but it is expected to become possible in the future.
                    for asset in data_source.assets:
                        delete_asset(asset.data_uri, asset.is_directory)
                        await db.execute(
                            delete(orm.Asset).where(orm.Asset.id == asset.id)
                        )
            result = await db.execute(
                delete(orm.Node).where(orm.Node.id == self.node.id)
            )
            if result.rowcount == 0:
                # TODO Abstract this from FastAPI?
                raise HTTPException(
                    status_code=404,
                    detail=f"No node {self.node.id}",
                )
            assert (
                result.rowcount == 1
            ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def delete_tree(self, external_only=True):
        """
        Delete a Node and of the Nodes beneath it in the tree.

        That is, delete all Nodes that have this Node as an ancestor, any number
        of "generators" up.

        Any DataSources belonging to those Nodes and any Assets associated (only) with
        those DataSources will also be deleted.
        """
        conditions = []
        segments = self.ancestors + [self.key]
        for generation in range(len(segments)):
            conditions.append(orm.Node.ancestors[generation] == segments[0])
        async with self.context.session() as db:
            if external_only:
                count_int_asset_statement = select(
                    func.count(orm.Asset.data_uri)
                ).filter(
                    orm.Asset.data_sources.any(
                        orm.DataSource.management != Management.external
                    )
                )
                for condition in conditions:
                    count_int_asset_statement.filter(condition)
                count_int_assets = (
                    await db.execute(count_int_asset_statement)
                ).scalar()
                if count_int_assets > 0:
                    raise WouldDeleteData(
                        "Some items in this tree are internally managed. "
                        "Delete the records will also delete the underlying data files. "
                        "If you want to delete them, pass external_only=False."
                    )
            else:
                sel_int_asset_statement = select(
                    orm.Asset.data_uri, orm.Asset.is_directory
                ).filter(
                    orm.Asset.data_sources.any(
                        orm.DataSource.management != Management.external
                    )
                )
                for condition in conditions:
                    sel_int_asset_statement.filter(condition)
                int_assets = (await db.execute(sel_int_asset_statement)).all()
                for data_uri, is_directory in int_assets:
                    delete_asset(data_uri, is_directory)
            # TODO Deal with Assets belonging to multiple DataSources.
            del_asset_statement = delete(orm.Asset)
            for condition in conditions:
                del_asset_statement.filter(condition)
            await db.execute(del_asset_statement)
            del_node_statement = delete(orm.Node)
            for condition in conditions:
                del_node_statement.filter(condition)
            result = await db.execute(del_node_statement)
            await db.commit()
        return result.rowcount

    async def delete_revision(self, number):
        async with self.context.session() as db:
            result = await db.execute(
                delete(orm.Revision)
                .where(orm.Revision.node_id == self.node.id)
                .where(orm.Revision.revision_number == number)
            )
            if result.rowcount == 0:
                # TODO Abstract this from FastAPI?
                raise HTTPException(
                    status_code=404,
                    detail=f"No revision {number} for node {self.node.id}",
                )
            assert (
                result.rowcount == 1
            ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def update_metadata(self, metadata=None, specs=None):
        values = {}
        if metadata is not None:
            # Trailing underscore in 'metadata_' avoids collision with
            # SQLAlchemy reserved word 'metadata'.
            values["metadata_"] = metadata
        if specs is not None:
            values["specs"] = [s.dict() for s in specs]
        async with self.context.session() as db:
            current = (
                await db.execute(select(orm.Node).where(orm.Node.id == self.node.id))
            ).scalar_one()
            next_revision_number = 1 + (
                (
                    await db.execute(
                        select(func.max(orm.Revision.revision_number)).where(
                            orm.Revision.node_id == self.node.id
                        )
                    )
                ).scalar_one()
                or 0
            )
            revision = orm.Revision(
                # Trailing underscore in 'metadata_' avoids collision with
                # SQLAlchemy reserved word 'metadata'.
                metadata_=current.metadata_,
                specs=current.specs,
                node_id=current.id,
                revision_number=next_revision_number,
            )
            db.add(revision)
            await db.execute(
                update(orm.Node).where(orm.Node.id == self.node.id).values(**values)
            )
            await db.commit()


class CatalogContainerAdapter(CatalogNodeAdapter):
    async def keys_range(self, offset, limit):
        if self.data_sources:
            return (await self.get_adapter()).keys()[
                offset : offset + limit  # noqa: E203
            ]
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
        if self.data_sources:
            return (await self.get_adapter()).items()[
                offset : offset + limit  # noqa: E203
            ]
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
                    type(self)(self.context, node, access_policy=self.access_policy),
                )
                for node in nodes
            ]

    async def read(self, *args, **kwargs):
        if not self.data_sources:
            fields = kwargs.get("fields")
            if fields:
                return self.search(KeysFilter(fields))
            return self
        return await ensure_awaitable((await self.get_adapter()).read, *args, **kwargs)


class CatalogArrayAdapter(CatalogNodeAdapter):
    async def read(self, *args, **kwargs):
        if not self.data_sources:
            fields = kwargs.get("fields")
            if fields:
                return self.search(KeysFilter(fields))
            return self
        return await ensure_awaitable((await self.get_adapter()).read, *args, **kwargs)

    async def read_block(self, *args, **kwargs):
        return await ensure_awaitable(
            (await self.get_adapter()).read_block, *args, **kwargs
        )

    async def write(self, *args, **kwargs):
        return await ensure_awaitable((await self.get_adapter()).write, *args, **kwargs)

    async def write_block(self, *args, **kwargs):
        return await ensure_awaitable(
            (await self.get_adapter()).write_block, *args, **kwargs
        )


class CatalogAwkwardAdapter(CatalogNodeAdapter):
    async def read(self, *args, **kwargs):
        return await ensure_awaitable((await self.get_adapter()).read, *args, **kwargs)

    async def read_buffers(self, *args, **kwargs):
        return await ensure_awaitable(
            (await self.get_adapter()).read_buffers, *args, **kwargs
        )

    async def write(self, *args, **kwargs):
        return await ensure_awaitable((await self.get_adapter()).write, *args, **kwargs)


class CatalogSparseAdapter(CatalogArrayAdapter):
    pass


class CatalogTableAdapter(CatalogNodeAdapter):
    async def read(self, *args, **kwargs):
        return await ensure_awaitable((await self.get_adapter()).read, *args, **kwargs)

    async def write(self, *args, **kwargs):
        return await ensure_awaitable((await self.get_adapter()).write, *args, **kwargs)

    async def read_partition(self, *args, **kwargs):
        return await ensure_awaitable(
            (await self.get_adapter()).read_partition, *args, **kwargs
        )

    async def write_partition(self, *args, **kwargs):
        return await ensure_awaitable(
            (await self.get_adapter()).write_partition, *args, **kwargs
        )


def delete_asset(data_uri, is_directory):
    url = urlparse(data_uri)
    if url.scheme == "file":
        path = safe_path(data_uri)
        if is_directory:
            shutil.rmtree(path)
        else:
            Path(path).unlink()
    else:
        raise NotImplementedError(url.scheme)


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
    "Convert from pydantic model to dict."
    if structure is None:
        return None
    return structure.dict()


def binary_op(query, tree, operation):
    dialect_name = tree.engine.url.get_dialect().name
    keys = query.key.split(".")
    attr = orm.Node.metadata_[keys]
    if dialect_name == "sqlite":
        condition = operation(_get_value(attr, type(query.value)), query.value)
    # specific case where GIN optomized index can be used to speed up POSTGRES equals queries
    elif (dialect_name == "postgresql") and (operation == operator.eq):
        condition = orm.Node.metadata_.op("@>")(
            type_coerce(
                key_array_to_json(keys, query.value),
                orm.Node.metadata_.type,
            )
        )
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
        access_policy=access_policy,
        writable_storage=writable_storage,
        readable_storage=readable_storage,
        echo=echo,
    )


def from_uri(
    uri,
    *,
    metadata=None,
    specs=None,
    access_policy=None,
    writable_storage=None,
    readable_storage=None,
    init_if_not_exists=False,
    echo=DEFAULT_ECHO,
    adapters_by_mimetype=None,
):
    uri = str(uri)
    if init_if_not_exists:
        # The alembic stamping can only be does synchronously.
        # The cleanest option available is to start a subprocess
        # because SQLite is allergic to threads.
        import subprocess

        # TODO Check if catalog exists.
        subprocess.run(
            [sys.executable, "-m", "tiled", "catalog", "init", "--if-not-exists", uri],
            capture_output=True,
            check=True,
        )
    if not SCHEME_PATTERN.match(uri):
        # Interpret URI as filepath.
        uri = f"sqlite+aiosqlite:///{uri}"

    engine = create_async_engine(uri, echo=echo, json_serializer=json_serializer)
    if engine.dialect.name == "sqlite":
        event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragma)
    return CatalogContainerAdapter(
        Context(engine, writable_storage, readable_storage, adapters_by_mimetype),
        RootNode(metadata, specs, access_policy),
        access_policy=access_policy,
    )


def _set_sqlite_pragma(conn, record):
    cursor = conn.cursor()
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#foreign-key-support
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def format_distinct_result(results, counts):
    if counts:
        formatted_result = [
            {"value": value, "count": count} for value, count in results
        ]
    else:
        formatted_result = [{"value": value} for value, in results]
    return formatted_result


class WouldDeleteData(RuntimeError):
    pass


class Collision(Conflicts):
    pass


def json_serializer(obj):
    "The PostgreSQL JSON serializer requires str, not bytes."
    return safe_json_dump(obj).decode()


def key_array_to_json(keys, value):
    """Take JSON accessor information as an array of keys and value

    Parameters
    ----------
    keys : iterable
        An array of keys to be created in the object.
    value : string
        Value assigned to the final key.

    Returns
    -------
    json
        JSON object for use in postgresql queries.

    Examples
    --------
    >>> key_array_to_json(['x','y','z'], 1)
    {'x': {'y': {'z': 1}}
    """
    return {keys[0]: reduce(lambda x, y: {y: x}, keys[1:][::-1], value)}


STRUCTURES = {
    StructureFamily.container: CatalogContainerAdapter,
    StructureFamily.array: CatalogArrayAdapter,
    StructureFamily.awkward: CatalogAwkwardAdapter,
    StructureFamily.table: CatalogTableAdapter,
    StructureFamily.sparse: CatalogSparseAdapter,
}
