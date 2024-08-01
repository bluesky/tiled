import collections
import importlib
import itertools as it
import logging
import operator
import os
import re
import shutil
import sys
import uuid
from functools import partial, reduce
from pathlib import Path
from typing import Callable, Dict
from urllib.parse import quote_plus, urlparse

import anyio
from fastapi import HTTPException
from sqlalchemy import (
    delete,
    event,
    false,
    func,
    not_,
    or_,
    select,
    text,
    true,
    type_coerce,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, REGCONFIG
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import selectinload
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql.expression import cast
from sqlalchemy.sql.sqltypes import MatchType
from starlette.status import HTTP_404_NOT_FOUND, HTTP_415_UNSUPPORTED_MEDIA_TYPE

from tiled.queries import (
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    KeysFilter,
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
    ZARR_MIMETYPE,
)
from ..query_registration import QueryTranslationRegistry
from ..server.schemas import Asset, DataSource, Management, Revision, Spec
from ..structures.core import StructureFamily
from ..utils import (
    SCHEME_PATTERN,
    UNCHANGED,
    Conflicts,
    OneShotCachedMap,
    UnsupportedQueryType,
    ensure_awaitable,
    ensure_uri,
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
INIT_STORAGE = OneShotCachedMap(
    {
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "...adapters.zarr", __name__
        ).ZarrArrayAdapter.init_storage,
        AWKWARD_BUFFERS_MIMETYPE: lambda: importlib.import_module(
            "...adapters.awkward_buffers", __name__
        ).AwkwardBuffersAdapter.init_storage,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.parquet", __name__
        ).ParquetDatasetAdapter.init_storage,
        "text/csv": lambda: importlib.import_module(
            "...adapters.csv", __name__
        ).CSVAdapter.init_storage,
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter.init_storage,
        APACHE_ARROW_FILE_MIME_TYPE: lambda: importlib.import_module(
            "...adapters.arrow", __name__
        ).ArrowAdapter.init_storage,
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
        self.specs = [Spec.model_validate(spec) for spec in specs or []]
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
            if not urlparse(writable_storage).scheme == "file":
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
        self.specs = [Spec.model_validate(spec) for spec in node.specs]
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
        return f"<{type(self).__name__} /{'/'.join(self.segments)}>"

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

    async def async_len(self):
        statement = select(func.count(orm.Node.key)).filter(
            orm.Node.ancestors == self.segments
        )
        statement = self.apply_conditions(statement)
        async with self.context.session() as db:
            return (await db.execute(statement)).scalar_one()

    async def lookup_adapter(
        self, segments
    ):  # TODO: Accept filter for predicate-pushdown.
        if not segments:
            return self
        *ancestors, key = segments
        if self.conditions and len(segments) > 1:
            # There are some conditions (i.e. WHERE clauses) applied to
            # this node, either via user search queries or via access
            # control policy queries. Look up first the _direct_ child of this
            # node, if it exists within the filtered results.
            first_level = await self.lookup_adapter(segments[:1])
            if first_level is None:
                return None
            # Now proceed to traverse further down the tree, if needed.
            # Search queries and access controls apply only at the top level.
            assert not first_level.conditions
            return await first_level.lookup_adapter(segments[1:])
        statement = select(orm.Node)
        statement = self.apply_conditions(statement)
        statement = statement.filter(
            orm.Node.ancestors == self.segments + ancestors
        ).options(
            selectinload(orm.Node.data_sources).selectinload(orm.DataSource.structure)
        )
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
        (data_source,) = self.data_sources
        try:
            adapter_factory = self.context.adapters_by_mimetype[data_source.mimetype]
        except KeyError:
            raise RuntimeError(
                f"Server configuration has no adapter for mimetype {data_source.mimetype!r}"
            )
        parameters = collections.defaultdict(list)
        for asset in data_source.assets:
            if asset.parameter is None:
                continue
            scheme = urlparse(asset.data_uri).scheme
            if scheme != "file":
                raise NotImplementedError(
                    f"Only 'file://...' scheme URLs are currently supported, not {asset.data_uri}"
                )
            if scheme == "file":
                # Protect against misbehaving clients reading from unintended
                # parts of the filesystem.
                asset_path = path_from_uri(asset.data_uri)
                for readable_storage in self.context.readable_storage:
                    if Path(
                        os.path.commonpath(
                            [path_from_uri(readable_storage), asset_path]
                        )
                    ) == path_from_uri(readable_storage):
                        break
                else:
                    raise RuntimeError(
                        f"Refusing to serve {asset.data_uri} because it is outside "
                        "the readable storage area for this server."
                    )
            if asset.num is None:
                parameters[asset.parameter] = asset.data_uri
            else:
                parameters[asset.parameter].append(asset.data_uri)
        adapter_kwargs = dict(parameters)
        adapter_kwargs.update(data_source.parameters)
        adapter_kwargs["specs"] = self.node.specs
        adapter_kwargs["metadata"] = self.node.metadata_
        adapter_kwargs["structure"] = data_source.structure
        adapter_kwargs["access_policy"] = self.access_policy
        adapter = await anyio.to_thread.run_sync(
            partial(adapter_factory, **adapter_kwargs)
        )
        for query in self.queries:
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
    ):
        key = key or self.context.key_maker()
        data_sources = data_sources or []

        node = orm.Node(
            key=key,
            ancestors=self.segments,
            metadata_=metadata,
            structure_family=structure_family,
            specs=[s.model_dump() for s in specs or []],
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
                    if data_source.mimetype is None:
                        data_source.mimetype = DEFAULT_CREATION_MIMETYPE[
                            data_source.structure_family
                        ]
                    data_source.parameters = {}
                    data_uri = str(self.context.writable_storage) + "".join(
                        f"/{quote_plus(segment)}" for segment in (self.segments + [key])
                    )
                    if data_source.mimetype not in INIT_STORAGE:
                        raise HTTPException(
                            status_code=415,
                            detail=(
                                f"The given data source mimetype, {data_source.mimetype}, "
                                "is not one that the Tiled server knows how to write."
                            ),
                        )
                    init_storage = INIT_STORAGE[data_source.mimetype]
                    assets = await ensure_awaitable(
                        init_storage, data_uri, data_source.structure
                    )
                    data_source.assets.extend(assets)
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
            return key, type(self)(
                self.context, refreshed_node, access_policy=self.access_policy
            )

    async def _put_asset(self, db, asset):
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

    async def put_data_source(self, data_source):
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
                    status_code=HTTP_404_NOT_FOUND,
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
                    status_code=HTTP_404_NOT_FOUND,
                    detail=f"No revision {number} for node {self.node.id}",
                )
            assert (
                result.rowcount == 1
            ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def replace_metadata(self, metadata=None, specs=None):
        values = {}
        if metadata is not None:
            # Trailing underscore in 'metadata_' avoids collision with
            # SQLAlchemy reserved word 'metadata'.
            values["metadata_"] = metadata
        if specs is not None:
            values["specs"] = [s.model_dump() for s in specs]
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
            return it.islice(
                (await self.get_adapter()).keys(),
                offset,
                (offset + limit) if limit is not None else None,  # noqa: E203
            )
        statement = select(orm.Node.key).filter(orm.Node.ancestors == self.segments)
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
        statement = select(orm.Node).filter(orm.Node.ancestors == self.segments)
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


def delete_asset(data_uri, is_directory):
    url = urlparse(data_uri)
    if url.scheme == "file":
        path = path_from_uri(data_uri)
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
    return structure.model_dump()


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
    attr = orm.Node.metadata_[query.key.split(".")]
    condition = _get_value(attr, type(query.value)).contains(query.value)
    return tree.new_variation(conditions=tree.conditions + [condition])


def full_text(query, tree):
    dialect_name = tree.engine.url.get_dialect().name
    if dialect_name == "sqlite":
        condition = orm.metadata_fts5.c.metadata.match(query.text)
    elif dialect_name == "postgresql":
        tsvector = func.jsonb_to_tsvector(
            cast("simple", REGCONFIG), orm.Node.metadata_, cast(["string"], JSONB)
        )
        condition = tsvector.op("@@")(func.to_tsquery("simple", query.text))
    else:
        raise UnsupportedQueryType("full_text")
    return tree.new_variation(conditions=tree.conditions + [condition])


def specs(query, tree):
    dialect_name = tree.engine.url.get_dialect().name
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
    dialect_name = tree.engine.url.get_dialect().name
    return _IN_OR_NOT_IN_DIALECT_DISPATCH[dialect_name](query, tree, method)


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
CatalogNodeAdapter.register_query(SpecsQuery, specs)
CatalogNodeAdapter.register_query(FullText, full_text)
# TODO: Regex


def in_memory(
    *,
    metadata=None,
    specs=None,
    access_policy=None,
    writable_storage=None,
    readable_storage=None,
    echo=DEFAULT_ECHO,
    adapters_by_mimetype=None,
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
        adapters_by_mimetype=adapters_by_mimetype,
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
        process = subprocess.run(
            [sys.executable, "-m", "tiled", "catalog", "init", "--if-not-exists", uri],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        # Capture stdout and stderr from the subprocess and write to logging
        stdout = process.stdout.decode()
        stderr = process.stderr.decode()
        logging.info(f"Subprocess stdout: {stdout}")
        logging.error(f"Subprocess stderr: {stderr}")
    if not SCHEME_PATTERN.match(uri):
        # Interpret URI as filepath.
        uri = f"sqlite+aiosqlite:///{uri}"

    parsed_url = make_url(uri)
    if (parsed_url.get_dialect().name == "sqlite") and (
        parsed_url.database != ":memory:"
    ):
        # For file-backed SQLite databases, connection pooling offers a
        # significant performance boost. For SQLite databases that exist
        # only in process memory, pooling is not applicable.
        poolclass = AsyncAdaptedQueuePool
    else:
        poolclass = None  # defer to sqlalchemy default
    engine = create_async_engine(
        uri, echo=echo, json_serializer=json_serializer, poolclass=poolclass
    )
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


STRUCTURES = {
    StructureFamily.array: CatalogArrayAdapter,
    StructureFamily.awkward: CatalogAwkwardAdapter,
    StructureFamily.container: CatalogContainerAdapter,
    StructureFamily.sparse: CatalogSparseAdapter,
    StructureFamily.table: CatalogTableAdapter,
}
