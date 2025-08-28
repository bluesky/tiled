import asyncio
import collections
import copy
import dataclasses
import importlib
import itertools as it
import logging
import operator
import os
import re
import shutil
import sys
import uuid
from contextlib import closing
from datetime import datetime
from functools import partial, reduce
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union, cast
from urllib.parse import urlparse

import anyio
import orjson
from fastapi import HTTPException, WebSocketDisconnect
from sqlalchemy import (
    and_,
    delete,
    event,
    exists,
    false,
    func,
    literal,
    not_,
    or_,
    select,
    text,
    true,
    type_coerce,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, REGCONFIG, TEXT
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import aliased, selectinload
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql.expression import cast as sql_cast
from sqlalchemy.sql.sqltypes import MatchType
from starlette.status import HTTP_404_NOT_FOUND, HTTP_415_UNSUPPORTED_MEDIA_TYPE

from tiled.queries import (
    AccessBlobFilter,
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    KeyPresent,
    KeysFilter,
    Like,
    NotEq,
    NotIn,
    Operator,
    SpecsQuery,
    StructureFamilyQuery,
)

from ..mimetypes import (
    APACHE_ARROW_FILE_MIME_TYPE,
    AWKWARD_BUFFERS_MIMETYPE,
    DEFAULT_ADAPTERS_BY_MIMETYPE,
    PARQUET_MIMETYPE,
    SPARSE_BLOCKS_PARQUET_MIMETYPE,
    TILED_SQL_TABLE_MIMETYPE,
    ZARR_MIMETYPE,
)
from ..query_registration import QueryTranslationRegistry
from ..server.core import NoEntry
from ..server.schemas import Asset, DataSource, Management, Revision
from ..storage import (
    FileStorage,
    SQLStorage,
    get_storage,
    parse_storage,
    register_storage,
)
from ..structures.core import Spec, StructureFamily
from ..utils import (
    UNCHANGED,
    Conflicts,
    OneShotCachedMap,
    UnsupportedQueryType,
    ensure_awaitable,
    ensure_specified_sql_driver,
    import_object,
    path_from_uri,
    safe_json_dump,
)
from . import orm
from .core import check_catalog_database, initialize_database
from .explain import ExplainAsyncSession
from .utils import compute_structure_id

logger = logging.getLogger(__name__)

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))
INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

# When data is uploaded, how is it saved?
# TODO: Make this configurable at Catalog construction time.
DEFAULT_CREATION_MIMETYPE = {
    StructureFamily.array: ZARR_MIMETYPE,
    StructureFamily.awkward: AWKWARD_BUFFERS_MIMETYPE,
    StructureFamily.table: PARQUET_MIMETYPE,
    StructureFamily.sparse: SPARSE_BLOCKS_PARQUET_MIMETYPE,
}

# TODO: make type[Adapter] after #1047
STORAGE_ADAPTERS_BY_MIMETYPE = OneShotCachedMap[str, type](
    {
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "...adapters.zarr", __name__
        ).ZarrArrayAdapter,
        AWKWARD_BUFFERS_MIMETYPE: lambda: importlib.import_module(
            "...adapters.awkward_buffers", __name__
        ).AwkwardBuffersAdapter,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.parquet", __name__
        ).ParquetDatasetAdapter,
        "text/csv": lambda: importlib.import_module(
            "...adapters.csv", __name__
        ).CSVAdapter,
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter,
        APACHE_ARROW_FILE_MIME_TYPE: lambda: importlib.import_module(
            "...adapters.arrow", __name__
        ).ArrowAdapter,
        TILED_SQL_TABLE_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sql", __name__
        ).SQLAdapter,
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

    def __init__(self, metadata, specs, top_level_access_blob):
        self.id = 0
        self.parent = None
        self.metadata_ = metadata or {}
        self.specs = specs or []
        self.key = ""
        self.data_sources = None
        self.access_blob = top_level_access_blob or {}


class Context:
    def __init__(
        self,
        engine: AsyncEngine,
        writable_storage=None,
        readable_storage=None,
        adapters_by_mimetype=None,
        cache_settings=None,
        key_maker=lambda: str(uuid.uuid4()),
        storage_pool_size=None,
        storage_max_overflow=None,
    ):
        self.engine = engine

        self.writable_storage = []
        self.readable_storage = set()

        # Back-compat: `writable_storage` used to be a dict: we want its values.
        if isinstance(writable_storage, dict):
            writable_storage = list(writable_storage.values())
        # Back-compat: `writable_storage` used to be a filepath.
        if isinstance(writable_storage, (str, Path)):
            writable_storage = [writable_storage]
        if isinstance(readable_storage, str):
            raise ValueError("readable_storage should be a list of URIs or paths")

        for item in writable_storage or []:
            self.writable_storage.append(
                parse_storage(
                    item, pool_size=storage_pool_size, max_overflow=storage_max_overflow
                )
            )
        for item in readable_storage or []:
            self.readable_storage.add(parse_storage(item))
        # Writable storage should also be readable.
        self.readable_storage.update(self.writable_storage)
        # Register all storage in a registry that enables Adapters to access
        # credentials (if applicable).
        for item in self.readable_storage:
            register_storage(item)

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
        self.cache_settings = cache_settings

    def session(self):
        "Convenience method for constructing an AsyncSession context"
        return ExplainAsyncSession(self.engine, autoflush=False, expire_on_commit=False)

    async def execute(self, statement, explain=None):
        "Debugging convenience utility, not exposed to server"
        async with self.session() as db:
            result = await db.execute(text(statement), explain=explain)
            await db.commit()
            return result

    async def startup(self):
        if (self.engine.dialect.name == "sqlite") and (
            self.engine.url.database == ":memory:"
            or self.engine.url.query.get("mode") == "memory"
        ):
            # Special-case for in-memory SQLite: Because it is transient we can
            # skip over anything related to migrations.
            await initialize_database(self.engine)
        else:
            await check_catalog_database(self.engine)

        cache_client = None
        cache_ttl = 0
        if self.cache_settings:
            if self.cache_settings["uri"].startswith("redis"):
                from redis import asyncio as redis

                socket_timeout = self.cache_settings.get("socket_timeout", 10.0)
                socket_connect_timeout = self.cache_settings.get(
                    "socket_connect_timeout", 10.0
                )
                cache_client = redis.from_url(
                    self.cache_settings["uri"],
                    socket_timeout=socket_timeout,
                    socket_connect_timeout=socket_connect_timeout,
                )
                cache_ttl = self.cache_settings.get("ttl", 3600)
        self.cache_client = cache_client
        self.cache_ttl = cache_ttl


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
        mount_path: Optional[list[str]] = None,
    ):
        self.context = context
        self.node = node
        self.sorting = sorting or [("", 1)]
        self.order_by_clauses = order_by_clauses(self.sorting)
        self.conditions = conditions or []
        self.queries = queries or []
        self.structure_family = node.structure_family
        self.specs = [Spec(**spec) for spec in node.specs]
        self.startup_tasks = [self.startup]
        if mount_path:
            self.startup_tasks.append(partial(self.create_mount, mount_path))
        self.shutdown_tasks = [self.shutdown]

    async def path_segments(self):
        statement = (
            select(orm.Node.key)
            .where(orm.Node.id != 0)
            .join(orm.NodesClosure, orm.NodesClosure.ancestor == orm.Node.id)
            .where(orm.NodesClosure.descendant == self.node.id)
            .order_by(orm.NodesClosure.depth.desc())
        )

        async with self.context.session() as db:
            return (await db.execute(statement)).scalars().all()

    @property
    def access_blob(self):
        return self.node.access_blob

    def metadata(self):
        return self.node.metadata_

    async def startup(self):
        await self.context.startup()

    async def create_mount(self, mount_path: list[str]):
        statement = node_from_segments(mount_path).with_only_columns(orm.Node.id)
        async with self.context.engine.connect() as conn:
            self.node.id = (await conn.execute(statement)).scalar()
        self.node.key = mount_path[-1]

    async def shutdown(self):
        await self.context.engine.dispose()

    @property
    def writable(self):
        return bool(self.context.writable_storage)

    @property
    def key(self):
        return self.node.key

    def __repr__(self):
        return f"<{type(self).__name__} {self.key}>"

    async def __aiter__(self):
        statement = select(orm.Node.key).filter(orm.Node.parent == self.node.id)
        for condition in self.conditions:
            statement = statement.filter(condition)
        async with self.context.session() as db:
            return (
                (await db.execute(statement.order_by(*self.order_by_clauses)))
                .scalars()
                .all()
            )
        statement = select(orm.Node.key).filter(orm.Node.parent == self.node.id)
        async with self.context.session() as db:
            return (await db.execute(statement)).scalar().all()

    @property
    def data_sources(self):
        return [DataSource.from_orm(ds) for ds in (self.node.data_sources or [])]

    async def asset_by_id(self, asset_id):
        statement = (
            select(orm.Asset)
            .join(
                orm.DataSourceAssetAssociation,
                orm.DataSourceAssetAssociation.asset_id == orm.Asset.id,
            )
            .join(
                orm.DataSource,
                orm.DataSource.id == orm.DataSourceAssetAssociation.data_source_id,
            )
            .join(orm.Node, orm.Node.id == orm.DataSource.node_id)
            .where((orm.Node.id == self.node.id) & (orm.Asset.id == asset_id))
        )
        async with self.context.session() as db:
            asset = (await db.execute(statement)).scalar()
        if asset is None:
            return None  # no match
        return Asset.from_orm(asset)

    def structure(self):
        if self.data_sources:
            assert len(self.data_sources) == 1  # more not yet implemented
            return self.data_sources[0].structure
        return None

    def apply_conditions(self, statement):
        # IF this is a sqlite database and we are doing a full text MATCH
        # query, we need a JOIN with the FTS5 virtual table.
        if (self.context.engine.dialect.name == "sqlite") and any(
            isinstance(condition.type, MatchType) for condition in self.conditions
        ):
            statement = statement.join(
                orm.metadata_fts5, orm.metadata_fts5.c.rowid == orm.Node.id
            )
        for condition in self.conditions:
            statement = statement.filter(condition)
        return statement

    async def exact_len(self):
        "Get the exact number of child nodes."
        statement = (
            select(func.count())
            .select_from(orm.Node)
            .filter(orm.Node.parent == self.node.id)
        )
        statement = self.apply_conditions(statement)

        async with self.context.session() as db:
            return (await db.execute(statement)).scalar_one()

    async def approx_len(self) -> Optional[int]:
        """Get an approximate number of child nodes using table statistics.

        This only works for PostgreSQL databases and does not take into account
        any filtering conditions. To be able to use these queries, the `nodes`
        must be vacuumed and analyzed regularly (at least once).

        If the database is not PostgreSQL, or if the statistics can not be
        obtained, return None.
        """

        if self.context.engine.dialect.name == "postgresql":
            async with self.context.session() as db:
                parent_and_freqs = await db.execute(
                    text(
                        """
                SELECT unnest(most_common_vals::text::int[])::int AS parent,
                       unnest(most_common_freqs) AS freq
                FROM pg_stats
                WHERE schemaname = 'public' AND tablename = 'nodes' AND attname = 'parent';
                                """
                    )
                )
                for parent, freq in parent_and_freqs:
                    if parent == self.node.id:
                        total = (
                            await db.execute(
                                text(
                                    """
                            SELECT reltuples::bigint FROM pg_class
                            WHERE  oid = 'public.nodes'::regclass;
                                            """
                                )
                            )
                        ).scalar_one()
                        return int(total * freq)
                else:
                    return None  # Statistics can not be obtained

        elif self.context.engine.dialect.name == "sqlite":
            # SQLite has no statistics tables, so we fall back to exact count.
            return None

    async def lbound_len(self, threshold) -> int:
        """Get a fast lower bound on the number of child nodes.

        This only counts up to `threshold`+1 nodes, so is fast even for large
        containers. If result is <= `threshold`, it is exact.
        """

        limited = (
            select(literal(1))
            .select_from(orm.Node)
            .where(orm.Node.parent == self.node.id)
            .limit(threshold + 1)
        )
        limited = self.apply_conditions(limited).cte("limited")
        statement = select(func.count()).select_from(limited)

        async with self.context.session() as db:
            return (await db.execute(statement)).scalar_one()

    async def lookup_adapter(self, segments: list[str]):
        # TODO: Accept filter for predicate-pushdown.
        if not segments:
            return self
        if self.conditions and len(segments) > 1:
            # There are some conditions (i.e. WHERE clauses) applied to
            # this node, either via user search queries or via access
            # control policy queries. Look up first the _direct_ child of this
            # node, if it exists within the filtered results.
            first_level = await self.lookup_adapter(segments[:1])

            # Now proceed to traverse further down the tree, if needed.
            # Search queries and access controls apply only at the top level.
            assert not first_level.conditions
            return await first_level.lookup_adapter(segments[1:])

        statement = node_from_segments(segments, root_id=self.node.id)
        statement = self.apply_conditions(statement)  # Conditions on the child node
        statement = statement.options(
            selectinload(orm.Node.data_sources).selectinload(orm.DataSource.structure)
        )

        async with self.context.session() as db:
            node = (await db.execute(statement)).scalar()
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
                            raise NoEntry(segments)
                    return adapter
            raise NoEntry(segments)

        return STRUCTURES[node.structure_family](self.context, node)

    async def get_adapter(self):
        (data_source,) = self.data_sources
        try:
            adapter_cls = self.context.adapters_by_mimetype[data_source.mimetype]
        except KeyError:
            raise RuntimeError(
                f"Server configuration has no adapter for mimetype {data_source.mimetype!r}"
            )
        for asset in data_source.assets:
            if asset.parameter is None:
                continue
            scheme = urlparse(asset.data_uri).scheme
            if scheme == "file":
                # Protect against misbehaving clients reading from unintended parts of the filesystem.
                asset_path = path_from_uri(asset.data_uri)
                for readable_storage in {
                    item
                    for item in self.context.readable_storage
                    if isinstance(item, FileStorage)
                }:
                    if (
                        Path(os.path.commonpath([readable_storage.path, asset_path]))
                        == readable_storage.path
                    ):
                        break
                else:
                    raise RuntimeError(
                        f"Refusing to serve {asset.data_uri} because it is outside "
                        "the readable storage area for this server."
                    )
        adapter = await anyio.to_thread.run_sync(
            partial(
                adapter_cls.from_catalog,
                data_source,
                self.node,
                **data_source.parameters,
            ),
        )
        for query in self.queries:
            if hasattr(adapter, "searc"):
                adapter = adapter.search(query)
        return adapter

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

    @property
    def insert(self):
        # The only way to do "insert if does not exist" i.e. ON CONFLICT
        # is to invoke dialect-specific insert.
        if self.context.engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        elif self.context.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            assert False  # future-proofing

        return insert

    async def create_node(
        self,
        structure_family,
        metadata,
        key=None,
        specs=None,
        data_sources=None,
        access_blob=None,
    ):
        access_blob = access_blob or {}
        key = key or self.context.key_maker()
        data_sources = data_sources or []

        node = orm.Node(
            key=key,
            parent=self.node.id,
            metadata_=metadata,
            structure_family=structure_family,
            specs=specs or [],
            access_blob=access_blob,
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
                    raise Collision(f"/{'/'.join(await self.path_segments() + [key])}")
                raise
            await db.refresh(node)
            for data_source in data_sources:
                if data_source.management != Management.external:
                    if structure_family == StructureFamily.container:
                        raise NotImplementedError(structure_family)
                    if data_source.mimetype is None:
                        data_source.mimetype = DEFAULT_CREATION_MIMETYPE[
                            data_source.structure_family
                        ]
                    if data_source.mimetype not in STORAGE_ADAPTERS_BY_MIMETYPE:
                        raise HTTPException(
                            status_code=415,
                            detail=(
                                f"The given data source mimetype, {data_source.mimetype}, "
                                "is not one that the Tiled server knows how to write."
                            ),
                        )
                    adapter_cls = STORAGE_ADAPTERS_BY_MIMETYPE[data_source.mimetype]
                    # Choose writable storage. Use the first writable storage item
                    # with a scheme that is supported by this adapter. # For
                    # back-compat, if an adapter does not declare `supported_storage`
                    # assume it supports file-based storage only.
                    supported_storage = getattr(
                        adapter_cls, "supported_storage", {FileStorage}
                    )
                    for storage in self.context.writable_storage:
                        if isinstance(storage, tuple(supported_storage)):
                            break
                    else:
                        raise RuntimeError(
                            f"The adapter {adapter_cls} supports storage types "
                            f"{[cls.__name__ for cls in supported_storage]} "
                            "but the only available storage types "
                            f"are {self.context.writable_storage}."
                        )
                    data_source = await ensure_awaitable(
                        adapter_cls.init_storage,
                        storage,
                        data_source,
                        await self.path_segments() + [key],
                    )
                else:
                    if data_source.mimetype not in self.context.adapters_by_mimetype:
                        raise HTTPException(
                            status_code=HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                            detail=(
                                f"The given data source mimetype, {data_source.mimetype}, "
                                "is not one that the Tiled server knows how to read."
                            ),
                        )

                if data_source.structure is None:
                    structure_id = None
                else:
                    # Obtain and hash the canonical (RFC 8785) representation of
                    # the JSON structure.
                    structure = _prepare_structure(
                        structure_family, data_source.structure
                    )
                    structure_id = compute_structure_id(structure)
                    statement = (
                        self.insert(orm.Structure).values(
                            id=structure_id,
                            structure=structure,
                        )
                    ).on_conflict_do_nothing(index_elements=["id"])
                    await db.execute(statement)
                data_source_orm = orm.DataSource(
                    structure_family=data_source.structure_family,
                    mimetype=data_source.mimetype,
                    management=data_source.management,
                    parameters=data_source.parameters,
                    structure_id=structure_id,
                )
                db.add(data_source_orm)
                node.data_sources.append(data_source_orm)
                await db.flush()  # Get data_source_orm.id.
                for asset in data_source.assets:
                    asset_id = await self._put_asset(db, asset)
                    assoc_orm = orm.DataSourceAssetAssociation(
                        asset_id=asset_id,
                        data_source_id=data_source_orm.id,
                        parameter=asset.parameter,
                        num=asset.num,
                    )
                    db.add(assoc_orm)
            await db.commit()
            # Load with DataSources each DataSource's Structure.
            refreshed_node = (
                await db.execute(
                    select(orm.Node)
                    .filter(orm.Node.id == node.id)
                    .options(
                        selectinload(orm.Node.data_sources).selectinload(
                            orm.DataSource.structure
                        ),
                    )
                )
            ).scalar()
            if self.context.cache_client:
                # Allocate a counter for the new node.
                await self.context.cache_client.setnx(f"sequence:{node.id}", 0)
                # Notify subscribers of the *parent* node about the new child.
                sequence = await self.context.cache_client.incr(
                    f"sequence:{self.node.id}"
                )
                metadata = {
                    "sequence": sequence,
                    "timestamp": datetime.now().isoformat(),
                    "key": key,
                    "structure_family": structure_family,
                    "specs": [spec.model_dump() for spec in (specs or [])],
                    "metadata": metadata,
                    "data_sources": [d.model_dump() for d in data_sources],
                }

                # Cache data in Redis with a TTL, and publish
                # a notification about it.
                pipeline = self.context.cache_client.pipeline()
                pipeline.hset(
                    f"data:{self.node.id}:{sequence}",
                    mapping={
                        "sequence": sequence,
                        "metadata": safe_json_dump(metadata),
                    },
                )
                pipeline.expire(
                    f"data:{self.node.id}:{sequence}", self.context.cache_ttl
                )
                pipeline.publish(f"notify:{self.node.id}", sequence)
                await pipeline.execute()
            return type(self)(self.context, refreshed_node)

    async def _put_asset(self, db: AsyncSession, asset):
        # Find an asset_id if it exists, otherwise create a new one
        statement = select(orm.Asset.id).where(orm.Asset.data_uri == asset.data_uri)
        result = await db.execute(statement)
        if row := result.fetchone():
            (asset_id,) = row
        else:
            statement = self.insert(orm.Asset).values(
                data_uri=asset.data_uri,
                is_directory=asset.is_directory,
            )
            result = await db.execute(statement)
            (asset_id,) = result.inserted_primary_key

        return asset_id

    async def put_data_source(self, data_source, patch):
        # Obtain and hash the canonical (RFC 8785) representation of
        # the JSON structure.
        structure = _prepare_structure(
            data_source.structure_family, data_source.structure
        )
        structure_id = compute_structure_id(structure)
        statement = (
            self.insert(orm.Structure).values(
                id=structure_id,
                structure=structure,
            )
        ).on_conflict_do_nothing(index_elements=["id"])
        async with self.context.session() as db:
            await db.execute(statement)
            values = dict(
                structure_family=data_source.structure_family,
                mimetype=data_source.mimetype,
                management=data_source.management,
                parameters=data_source.parameters,
                structure_id=structure_id,
            )
            result = await db.execute(
                update(orm.DataSource)
                .where(orm.DataSource.id == data_source.id)
                .values(**values)
            )
            if result.rowcount == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data_source {data_source.id} on this node.",
                )
            # Add assets and associate them with the data_source
            for asset in data_source.assets:
                asset_id = await self._put_asset(db, asset)
                statement = select(orm.DataSourceAssetAssociation).where(
                    (orm.DataSourceAssetAssociation.data_source_id == data_source.id)
                    & (orm.DataSourceAssetAssociation.asset_id == asset_id)
                )
                result = await db.execute(statement)
                if not result.fetchone():
                    assoc_orm = orm.DataSourceAssetAssociation(
                        asset_id=asset_id,
                        data_source_id=data_source.id,
                        parameter=asset.parameter,
                        num=asset.num,
                    )
                    db.add(assoc_orm)

            await db.commit()
        if self.context.cache_client:
            sequence = await self.context.cache_client.incr(f"sequence:{self.node.id}")
            metadata = {
                "sequence": sequence,
                "timestamp": datetime.now().isoformat(),
                "data_source": data_source.dict(),
                "patch": patch.dict() if patch else None,
            }

            # Cache data in Redis with a TTL, and publish
            # a notification about it.
            pipeline = self.context.cache_client.pipeline()
            pipeline.hset(
                f"data:{self.node.id}:{sequence}",
                mapping={
                    "sequence": sequence,
                    "metadata": orjson.dumps(metadata),
                },
            )
            pipeline.expire(f"data:{self.node.id}:{sequence}", self.context.cache_ttl)
            pipeline.publish(f"notify:{self.node.id}", sequence)
            await pipeline.execute()

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

    async def delete(self, recursive=False, external_only=True):
        """Delete the Node.

        Any DataSources belonging to this Node and any Assets associated (only) with
        those DataSources will also be deleted.

        If `recursive` is True, delete all Nodes beneath this Node in the tree.
        """
        async with self.context.session() as db:
            if not recursive:
                has_children_stmt = select(
                    exists().where(
                        and_(
                            orm.NodesClosure.ancestor == self.node.id,
                            orm.NodesClosure.descendant != self.node.id,
                        )
                    )
                )
                if (await db.execute(has_children_stmt)).scalar():
                    raise Conflicts(
                        "Cannot delete a node that is not empty. "
                        "Delete its contents first or pass `recursive=True`."
                    )

            affected_nodes_stmnt = (
                select(orm.NodesClosure.descendant)
                .where(orm.NodesClosure.ancestor == self.node.id)
                .distinct()
                .scalar_subquery()
            )
            if external_only:
                int_asset_exists_stmt = select(
                    exists()
                    .where(orm.Asset.id == orm.DataSourceAssetAssociation.asset_id)
                    .where(
                        orm.DataSourceAssetAssociation.data_source_id
                        == orm.DataSource.id
                    )
                    .where(orm.DataSource.node_id.in_(affected_nodes_stmnt))
                    .where(orm.DataSource.management != Management.external)
                )

                if (await db.execute(int_asset_exists_stmt)).scalar():
                    raise WouldDeleteData(
                        "Some items in this tree are internally managed. "
                        "Deleting the records will also delete the underlying data files. "
                        "If you want to delete them, pass external_only=False."
                    )

            sel_asset_stmnt = (
                select(
                    orm.Asset.id,
                    orm.Asset.data_uri,
                    orm.Asset.is_directory,
                    orm.DataSource.management,
                    orm.DataSource.parameters,
                )
                .select_from(orm.Asset)
                .join(
                    orm.Asset.data_sources
                )  # Join on secondary (mapping) relationship
                .join(orm.DataSource.node)
                .filter(orm.Node.id.in_(affected_nodes_stmnt))
                .distinct()
            )

            assets_to_delete = []
            for asset_id, data_uri, is_directory, management, parameters in (
                await db.execute(sel_asset_stmnt)
            ).all():
                # Check if this asset is referenced by other UNAFFECTED nodes
                is_referenced = select(
                    exists()
                    .where(
                        orm.Asset.id == asset_id,
                        orm.Asset.data_sources.any(
                            orm.DataSource.node_id.notin_(affected_nodes_stmnt)
                        ),
                    )
                    .distinct()
                )
                if not (await db.execute(is_referenced)).scalar():
                    # This asset is referenced only by AFFECTED nodes, so we can delete it
                    await db.execute(delete(orm.Asset).where(orm.Asset.id == asset_id))
                    if management != Management.external:
                        assets_to_delete.append((data_uri, is_directory, parameters))
                elif (management == Management.writable) and (
                    urlparse(data_uri).scheme in {"duckdb", "sqlite", "postgresql"}
                ):
                    # The tabular storage asset may be referenced by several data_sources
                    # and nodes, so we cannot delete it completely. However, we can delete
                    # the relevant rows and tables.
                    assets_to_delete.append((data_uri, is_directory, parameters))

            result = await db.execute(
                delete(orm.Node)
                .where(orm.Node.id.in_(affected_nodes_stmnt))
                .where(orm.Node.parent.isnot(None))
            )
            await db.commit()

            # Finally, delete the physical assets that are not externally managed
            for data_uri, is_directory, parameters in assets_to_delete:
                delete_asset(data_uri, is_directory, parameters=parameters)

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
                    status_code=HTTP_404_NOT_FOUND,
                    detail=f"No revision {number} for node {self.node.id}",
                )
            assert (
                result.rowcount == 1
            ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def replace_metadata(
        self, metadata=None, specs=None, access_blob=None, *, drop_revision=False
    ):
        values = {}
        if metadata is not None:
            # Trailing underscore in 'metadata_' avoids collision with
            # SQLAlchemy reserved word 'metadata'.
            values["metadata_"] = metadata
        if specs is not None:
            values["specs"] = specs
        if access_blob is not None:
            values["access_blob"] = access_blob
        async with self.context.session() as db:
            if not drop_revision:
                current = (
                    await db.execute(
                        select(orm.Node).where(orm.Node.id == self.node.id)
                    )
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
                    access_blob=current.access_blob,
                    node_id=current.id,
                    revision_number=next_revision_number,
                )
                db.add(revision)
            await db.execute(
                update(orm.Node).where(orm.Node.id == self.node.id).values(**values)
            )
            await db.commit()

    async def close_stream(self):
        # Check the node status.
        # ttl returns -2 if the key does not exist.
        # ttl returns -1 if the key exists but has no associated expire.
        # ttl greater than 0 means that it is marked to expire.
        node_ttl = await self.context.cache_client.ttl(f"sequence:{self.node.id}")
        if node_ttl > 0:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"Stream for node {self.node.id} is already closed.",
            )
        if node_ttl == -2:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"Node {self.node.id} not found.",
            )

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "end_of_stream": True,
        }
        # Increment the counter for this node.
        sequence = await self.context.cache_client.incr(f"sequence:{self.node.id}")

        # Cache data in Redis with a TTL, and publish
        # a notification about it.
        pipeline = self.context.cache_client.pipeline()
        pipeline.hset(
            f"data:{self.node.id}:{sequence}",
            mapping={
                "sequence": sequence,
                "metadata": orjson.dumps(metadata),
            },
        )
        pipeline.expire(f"data:{self.node.id}:{sequence}", self.context.cache_ttl)
        pipeline.expire(f"sequence:{self.node.id}", self.context.cache_ttl)
        pipeline.publish(f"notify:{self.node.id}", sequence)
        await pipeline.execute()

    def make_ws_handler(self, websocket, formatter, uri):
        async def handler(sequence: Optional[int] = None):
            await websocket.accept()
            end_stream = asyncio.Event()
            cache_client = self.context.cache_client

            async def stream_data(sequence):
                key = f"data:{self.node.id}:{sequence}"
                payload_bytes, metadata_bytes = await cache_client.hmget(
                    key, "payload", "metadata"
                )
                if metadata_bytes is None:
                    # This means that redis ttl has expired for this sequence
                    return
                metadata = orjson.loads(metadata_bytes)
                if metadata.get("end_of_stream"):
                    # This means that the stream is closed by the producer
                    end_stream.set()
                    return
                metadata["uri"] = uri
                if metadata.get("patch"):
                    s = ",".join(
                        f"{offset}:{offset+shape}"
                        for offset, shape in zip(
                            metadata["patch"]["offset"], metadata["patch"]["shape"]
                        )
                    )
                    metadata["uri"] = f"{uri}?slice={s}"
                await formatter(websocket, metadata, payload_bytes)

            # Setup buffer
            stream_buffer = asyncio.Queue()

            async def buffer_live_events():
                pubsub = cache_client.pubsub()
                await pubsub.subscribe(f"notify:{self.node.id}")
                try:
                    async for message in pubsub.listen():
                        if message.get("type") == "message":
                            try:
                                live_seq = int(message["data"])
                                await stream_buffer.put(live_seq)
                            except Exception as e:
                                print(f"Error parsing live message: {e}")
                except Exception as e:
                    print(f"Live subscription error: {e}")
                finally:
                    await pubsub.unsubscribe(f"notify:{self.node.id}")
                    await pubsub.aclose()

            live_task = asyncio.create_task(buffer_live_events())

            if sequence is not None:
                current_seq = await cache_client.get(f"sequence:{self.node.id}")
                current_seq = int(current_seq) if current_seq is not None else 0
                print("Replaying old data...")
                for s in range(sequence, current_seq + 1):
                    await stream_data(s)
            # New data
            try:
                while not end_stream.is_set():
                    live_seq = await stream_buffer.get()
                    await stream_data(live_seq)
                else:
                    await websocket.close(code=1000, reason="Producer ended stream")
            except WebSocketDisconnect:
                print(f"Client disconnected from node {self.node.id}")
            finally:
                live_task.cancel()

        return handler


class CatalogContainerAdapter(CatalogNodeAdapter):
    async def keys_range(self, offset, limit):
        if self.data_sources:
            return it.islice(
                (await self.get_adapter()).keys(),
                offset,
                (offset + limit) if limit is not None else None,  # noqa: E203
            )
        statement = select(orm.Node.key).filter(orm.Node.parent == self.node.id)
        statement = self.apply_conditions(statement)
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
            return it.islice(
                (await self.get_adapter()).items(),
                offset,
                (offset + limit) if limit is not None else None,  # noqa: E203
            )
        statement = select(orm.Node).filter(orm.Node.parent == self.node.id)
        statement = self.apply_conditions(statement)
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
                    STRUCTURES[node.structure_family](self.context, node),
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

    async def _stream(self, media_type, entry, body, shape, block=None, offset=None):
        sequence = await self.context.cache_client.incr(f"sequence:{self.node.id}")
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "content-type": media_type,
            "shape": shape,
            "offset": offset,
            "block": block,
        }

        pipeline = self.context.cache_client.pipeline()
        pipeline.hset(
            f"data:{self.node.id}:{sequence}",
            mapping={
                "sequence": sequence,
                "metadata": orjson.dumps(metadata),
                "payload": body,  # raw user input
            },
        )
        pipeline.expire(f"data:{self.node.id}:{sequence}", self.context.cache_ttl)
        pipeline.publish(f"notify:{self.node.id}", sequence)
        await pipeline.execute()

    async def write(self, media_type, deserializer, entry, body):
        shape = entry.structure().shape
        if self.context.cache_client:
            await self._stream(media_type, entry, body, shape)
        if entry.structure_family == "array":
            dtype = entry.structure().data_type.to_numpy_dtype()
            data = await ensure_awaitable(deserializer, body, dtype, shape)
        elif entry.structure_family == "sparse":
            data = await ensure_awaitable(deserializer, body)
        else:
            raise NotImplementedError(entry.structure_family)
        return await ensure_awaitable((await self.get_adapter()).write, data)

    async def write_block(self, block, media_type, deserializer, entry, body):
        from tiled.adapters.array import slice_and_shape_from_block_and_chunks

        _, shape = slice_and_shape_from_block_and_chunks(
            block, entry.structure().chunks
        )
        if self.context.cache_client:
            await self._stream(media_type, entry, body, shape, block=block)
        if entry.structure_family == "array":
            dtype = entry.structure().data_type.to_numpy_dtype()
            data = await ensure_awaitable(deserializer, body, dtype, shape)
        elif entry.structure_family == "sparse":
            data = await ensure_awaitable(deserializer, body)
        else:
            raise NotImplementedError(entry.structure_family)
        return await ensure_awaitable(
            (await self.get_adapter()).write_block, data, block
        )

    async def patch(self, shape, offset, extend, media_type, deserializer, entry, body):
        if self.context.cache_client:
            await self._stream(media_type, entry, body, shape, offset=offset)
        dtype = entry.structure().data_type.to_numpy_dtype()
        data = await ensure_awaitable(deserializer, body, dtype, shape)
        # assumes a single DataSource (currently only supporting zarr)
        async with self.context.session() as db:
            new_shape_and_chunks = await ensure_awaitable(
                (await self.get_adapter()).patch, data, offset, extend
            )
            node = await db.get(orm.Node, self.node.id)
            if len(node.data_sources) != 1:
                raise NotImplementedError("Only handles one data source")
            data_source = node.data_sources[0]
            structure_row = await db.get(orm.Structure, data_source.structure_id)
            # Get the current structure row, update the shape, and write it back
            structure_dict = copy.deepcopy(structure_row.structure)
            structure_dict["shape"], structure_dict["chunks"] = new_shape_and_chunks
            new_structure_id = compute_structure_id(structure_dict)
            statement = (
                self.insert(orm.Structure)
                .values(
                    id=new_structure_id,
                    structure=structure_dict,
                )
                .on_conflict_do_nothing(index_elements=["id"])
            )
            await db.execute(statement)
            new_structure = await db.get(orm.Structure, new_structure_id)
            data_source.structure = new_structure
            data_source.structure_id = new_structure_id
            db.add(data_source)
            await db.commit()
            return structure_dict


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
    async def get(self, *args, **kwargs):
        return (await self.get_adapter()).get(*args, **kwargs)

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

    async def append_partition(self, *args, **kwargs):
        return await ensure_awaitable(
            (await self.get_adapter()).append_partition, *args, **kwargs
        )


def delete_asset(data_uri, is_directory, parameters=None):
    url = urlparse(data_uri)
    if url.scheme == "file":
        path = path_from_uri(data_uri)
        if is_directory:
            shutil.rmtree(path)
        else:
            Path(path).unlink()
    elif url.scheme in {"duckdb", "sqlite", "postgresql"}:
        storage = cast(SQLStorage, get_storage(data_uri))
        with closing(storage.connect()) as conn:
            table_name = parameters.get("table_name") if parameters else None
            dataset_id = parameters.get("dataset_id") if parameters else None
            with conn.cursor() as cursor:
                cursor.execute(
                    f'DELETE FROM "{table_name}" WHERE _dataset_id = {dataset_id:d};',
                )
            conn.commit()

            # If the table is empty, we can drop it
            with conn.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
                if cursor.fetchone()[0] == 0:
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            conn.commit()
    else:
        raise NotImplementedError(
            f"Cannot delete asset at {data_uri!r} because the scheme {url.scheme!r} is not supported."
        )


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
    if isinstance(structure, dict):
        return structure
    return dataclasses.asdict(structure)


def binary_op(query, tree, operation):
    dialect_name = tree.context.engine.url.get_dialect().name
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


def like(query, tree):
    keys = query.key.split(".")
    attr = orm.Node.metadata_[keys]
    condition = _get_value(attr, str).like(query.pattern)
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
    attr = orm.Node.metadata_[query.key.split(".")]
    condition = _get_value(attr, type(query.value)).contains(query.value)
    return tree.new_variation(conditions=tree.conditions + [condition])


def full_text(query, tree):
    dialect_name = tree.context.engine.url.get_dialect().name
    if dialect_name == "sqlite":
        condition = orm.metadata_fts5.c.metadata.match(query.text)
    elif dialect_name == "postgresql":
        tsvector = func.jsonb_to_tsvector(
            sql_cast("simple", REGCONFIG),
            orm.Node.metadata_,
            sql_cast(["string"], JSONB),
        )
        condition = tsvector.op("@@")(func.to_tsquery("simple", query.text))
    else:
        raise UnsupportedQueryType("full_text")
    return tree.new_variation(conditions=tree.conditions + [condition])


def specs(query, tree):
    dialect_name = tree.context.engine.url.get_dialect().name
    conditions = []
    attr = orm.Node.specs
    if dialect_name == "sqlite":
        # Construct the conditions for includes
        for i, name in enumerate(query.include):
            conditions.append(attr.like(f'%{{"name":"{name}",%'))
        # Construct the conditions for excludes
        for i, name in enumerate(query.exclude):
            conditions.append(not_(attr.like(f'%{{"name":"{name}",%')))
    elif dialect_name == "postgresql":
        if query.include:
            conditions.append(attr.op("@>")(specs_array_to_json(query.include)))
        if query.exclude:
            conditions.append(not_(attr.op("@>")(specs_array_to_json(query.exclude))))
    else:
        raise UnsupportedQueryType("specs")
    return tree.new_variation(conditions=tree.conditions + conditions)


def access_blob_filter(query, tree):
    dialect_name = tree.context.engine.url.get_dialect().name
    access_blob = orm.Node.access_blob
    if not (query.user_id or query.tags):
        # Results cannot possibly match an empty value or list,
        # so put a False condition in the list ensuring that
        # there are no rows returned.
        condition = false()
    elif dialect_name == "sqlite":
        attr_id = access_blob["user"]
        attr_tags = access_blob["tags"]
        access_tags_json = func.json_each(attr_tags).table_valued("value")
        condition = (
            select(1)
            .select_from(access_tags_json)
            .where(access_tags_json.c.value.in_(query.tags))
            .exists()
        )
        if query.user_id is not None:
            user_match = (
                func.json_extract(func.json_quote(attr_id), "$") == query.user_id
            )
            condition = or_(condition, user_match)
    elif dialect_name == "postgresql":
        access_blob_jsonb = type_coerce(access_blob, JSONB)
        condition = access_blob_jsonb["tags"].has_any(sql_cast(query.tags, ARRAY(TEXT)))
        if query.user_id is not None:
            user_match = access_blob_jsonb["user"].astext == query.user_id
            condition = or_(condition, user_match)
    else:
        raise UnsupportedQueryType("access_blob_filter")

    return tree.new_variation(conditions=tree.conditions + [condition])


def in_or_not_in_sqlite(query, tree, method):
    keys = query.key.split(".")
    attr = orm.Node.metadata_[keys]
    if len(query.value) == 0:
        if method == "in_":
            # Results cannot possibly be "in" in an empty list,
            # so put a False condition in the list ensuring that
            # there are no rows return.
            condition = false()
        else:  # method == "not_in"
            # All results are always "not in" an empty list.
            condition = true()
    else:
        condition = getattr(_get_value(attr, type(query.value[0])), method)(query.value)
    return tree.new_variation(conditions=tree.conditions + [condition])


def in_or_not_in_postgresql(query, tree, method):
    keys = query.key.split(".")
    # Engage btree_gin index with @> operator
    if method == "in_":
        if len(query.value) == 0:
            condition = false()
        else:
            condition = or_(
                *(
                    orm.Node.metadata_.op("@>")(key_array_to_json(keys, item))
                    for item in query.value
                )
            )
    elif method == "not_in":
        if len(query.value) == 0:
            condition = true()
        else:
            condition = not_(
                or_(
                    *(
                        orm.Node.metadata_.op("@>")(key_array_to_json(keys, item))
                        for item in query.value
                    )
                )
            )
    return tree.new_variation(conditions=tree.conditions + [condition])


_IN_OR_NOT_IN_DIALECT_DISPATCH: Dict[str, Callable] = {
    "sqlite": in_or_not_in_sqlite,
    "postgresql": in_or_not_in_postgresql,
}


def in_or_not_in(query, tree, method):
    METHODS = {"in_", "not_in"}
    if method not in METHODS:
        raise ValueError(f"method must be one of {METHODS}")
    dialect_name = tree.context.engine.url.get_dialect().name
    return _IN_OR_NOT_IN_DIALECT_DISPATCH[dialect_name](query, tree, method)


def key_present(query, tree):
    # Functionally in SQLAlchemy 'is not None' does not work as expected
    if tree.context.engine.url.get_dialect().name == "sqlite":
        condition = orm.Node.metadata_.op("->")("$." + query.key) != None  # noqa: E711
    else:
        keys = query.key.split(".")
        condition = (
            orm.Node.metadata_.op("#>")(sql_cast(keys, ARRAY(TEXT)))
            != None  # noqa: E711
        )
    condition = condition if getattr(query, "exists", True) else not_(condition)
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
CatalogNodeAdapter.register_query(KeyPresent, key_present)
CatalogNodeAdapter.register_query(KeysFilter, keys_filter)
CatalogNodeAdapter.register_query(StructureFamilyQuery, structure_family)
CatalogNodeAdapter.register_query(SpecsQuery, specs)
CatalogNodeAdapter.register_query(AccessBlobFilter, access_blob_filter)
CatalogNodeAdapter.register_query(FullText, full_text)
CatalogNodeAdapter.register_query(Like, like)


def in_memory(
    *,
    named_memory=None,
    metadata=None,
    specs=None,
    writable_storage=None,
    readable_storage=None,
    echo=DEFAULT_ECHO,
    adapters_by_mimetype=None,
    top_level_access_blob=None,
    cache_settings=None,
):
    if not named_memory:
        uri = "sqlite:///:memory:"
    else:
        uri = f"sqlite:///file:{named_memory}?mode=memory&cache=shared&uri=true"
    # NOTE: catalog_pool_size and catalog_max_overflow are ignored when using an
    # in-memory catalog.
    return from_uri(
        uri=uri,
        metadata=metadata,
        specs=specs,
        writable_storage=writable_storage,
        readable_storage=readable_storage,
        init_if_not_exists=True,
        echo=echo,
        adapters_by_mimetype=adapters_by_mimetype,
        top_level_access_blob=top_level_access_blob,
        cache_settings=cache_settings,
    )


def from_uri(
    uri,
    *,
    metadata=None,
    specs=None,
    writable_storage=None,
    readable_storage=None,
    init_if_not_exists=False,
    echo=DEFAULT_ECHO,
    adapters_by_mimetype=None,
    top_level_access_blob=None,
    mount_node: Optional[Union[str, List[str]]] = None,
    cache_settings=None,
    catalog_pool_size=None,
    storage_pool_size=None,
    catalog_max_overflow=None,
    storage_max_overflow=None,
):
    uri = ensure_specified_sql_driver(uri)
    if init_if_not_exists:
        # The alembic stamping can only be does synchronously.
        # The cleanest option available is to start a subprocess
        # because SQLite is allergic to threads.
        import subprocess

        # TODO Check if catalog exists.
        process = subprocess.run(
            [sys.executable, "-m", "tiled", "catalog", "init", "--if-not-exists", uri],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        # Capture stdout and stderr from the subprocess and write to logging
        stdout = process.stdout.decode()
        stderr = process.stderr.decode()
        logger.info(f"Subprocess stdout: {stdout}")
        logger.info(f"Subprocess stderr: {stderr}")

    parsed_url = make_url(uri)
    if (
        (parsed_url.get_dialect().name == "sqlite")
        and (parsed_url.database != ":memory:")
        and (parsed_url.query.get("mode", None) != "memory")
    ):
        # For file-backed SQLite databases, connection pooling offers a
        # significant performance boost. For SQLite databases that exist
        # only in process memory, pooling is not applicable.
        poolclass = AsyncAdaptedQueuePool
    elif parsed_url.get_dialect().name.startswith("postgresql"):
        poolclass = AsyncAdaptedQueuePool  # Default for PostgreSQL
    else:
        poolclass = None  # defer to sqlalchemy default

    node = RootNode(metadata, specs, top_level_access_blob)
    mount_path = (
        [segment for segment in mount_node.split("/") if segment]
        if isinstance(mount_node, str)
        else mount_node
    )

    # Optionally set pool size and max overflow for the catalog engine;
    # if not specified, use the default values from sqlalchemy.
    pool_kwargs = (
        {"pool_size": catalog_pool_size, "max_overflow": catalog_max_overflow}
        if poolclass == AsyncAdaptedQueuePool
        else {}
    )
    pool_kwargs = {k: v for k, v in pool_kwargs.items() if v is not None}
    # Create the async engine with the specified parameters
    engine = create_async_engine(
        uri,
        echo=echo,
        json_serializer=json_serializer,
        poolclass=poolclass,
        **pool_kwargs,
    )
    if engine.dialect.name == "sqlite":
        event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragma)
    context = Context(
        engine,
        writable_storage,
        readable_storage,
        adapters_by_mimetype,
        cache_settings,
        storage_pool_size=storage_pool_size,
        storage_max_overflow=storage_max_overflow,
    )
    adapter = CatalogContainerAdapter(context, node, mount_path=mount_path)

    return adapter


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


def specs_array_to_json(specs):
    """Take array of Specs strings and convert them to a `penguin` @> friendly array
    Assume constructed array will feature keys called "name"

    Parameters
    ----------
    specs : iterable
        An array of specs strings to be searched for.

    Returns
    -------
    json
        JSON object for use in postgresql queries.

    Examples
    --------
    >>> specs_array_to_json(['foo','bar'])
    [{"name":"foo"},{"name":"bar"}]
    """
    return [{"name": spec} for spec in specs]


def node_from_segments(segments, root_id=0):
    """Create an SQLAlchemy select statement to find a node based on its path

    Queries the database recursively to find the node with the given ancestors
    and key.

    Parameters
    ----------
        segments : list of str
            The path segments leading to the node, e.g. ['A', 'x', 'i'].
        root_id : int
            The ID of the root node, typically 0 for the root of the catalog.

    Returns
    -------
        sqlalchemy.sql.selectable.Select
    """

    # Create an alias for each ancestor node in the path and build the join chain
    orm_NodeAliases = [aliased(orm.Node) for _ in range(len(segments))] + [orm.Node]
    statement = select(orm_NodeAliases[-1])  # Select the child node
    statement = statement.select_from(orm_NodeAliases[0])  # Start from the ancestor
    statement = statement.where(orm_NodeAliases[0].id == root_id)
    for i, segment in enumerate(segments):
        parent, child = orm_NodeAliases[i], orm_NodeAliases[i + 1]
        statement = statement.join(child, child.parent == parent.id).where(
            child.key == segment
        )

    return statement


STRUCTURES = {
    StructureFamily.array: CatalogArrayAdapter,
    StructureFamily.awkward: CatalogAwkwardAdapter,
    StructureFamily.container: CatalogContainerAdapter,
    StructureFamily.sparse: CatalogSparseAdapter,
    StructureFamily.table: CatalogTableAdapter,
}
