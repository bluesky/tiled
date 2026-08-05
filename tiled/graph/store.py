"""
Storage layer for the graph links entity graph service.

``GraphSQLAlchemyStore`` attaches to the same process-global async
engine/connection pool used by Tiled's catalog (see
``tiled.server.connection_pool``), so the graph tables and the catalog
tables are always served from a single shared pool rather than opening a
second connection pool to the same database.

The graph tables themselves are defined in ``tiled.graph.orm`` (attached to
the catalog's ``Base.metadata``) and provisioned by the catalog's database
initialization / Alembic migrations. This store only reads and writes rows; it
does not create tables.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    and_,
    case,
    delete,
    event,
    false,
    func,
    insert,
    or_,
    select,
    text,
    type_coerce,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TEXT
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.expression import cast as sql_cast

from ..catalog.orm import Node
from ..queries import AccessBlobFilter
from ..server.connection_pool import get_database_engine
from ..server.settings import DatabaseSettings
from ..utils import UnsupportedQueryType
from .orm import entities as _entities
from .orm import links as _links
from .orm import namespaces as _namespaces

UNSET = object()

# The catalog ``nodes`` table, used to resolve entities.node_id by catalog path.
_nodes = Node.__table__

# ---------------------------------------------------------------------------
# Data records
# ---------------------------------------------------------------------------


class EntityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    node_id: Optional[int] = None
    entity_type: str
    name: str
    uri: Optional[str]
    properties: dict
    # None when node_id is set: access control is delegated to the node.
    access_blob: Optional[dict] = None
    created_at: datetime


class LinkRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    subject_id: str
    predicate: str
    object_id: str
    properties: dict
    access_blob: dict
    created_at: datetime


# ---------------------------------------------------------------------------
# SQLAlchemy schema
# ---------------------------------------------------------------------------

_metadata = MetaData()

# The real catalog nodes table (tiled.catalog.orm.Node), reused directly
# rather than re-declared here, so an entity's effective access control can
# be resolved from its node when the entity points to one (see
# get_node_access_blob below). It lives in Base.metadata, not this module's
# own _metadata -- _metadata.create_all() below never tries to create it.
_nodes = Node.__table__

_entities = Table(
    "entities",
    _metadata,
    Column("id", String, primary_key=True),
    Column(
        "node_id",
        Integer,
        # A Column object (rather than the string "nodes.id") is required
        # here: nodes lives in tiled.catalog.orm's Base.metadata, not this
        # module's own _metadata, so string-based FK resolution (which
        # looks within the owning Table's own MetaData) would not find it.
        ForeignKey(_nodes.c.id, ondelete="SET NULL"),
        nullable=True,
    ),
    Column("entity_type", String, nullable=False),
    Column("name", String, nullable=False),
    Column("uri", String, nullable=True),
    Column("properties", JSON, nullable=False),
    # Nullable: an entity with node_id set must not have its own
    # access_blob (enforced by the trigger below and in
    # tiled.graph.schema); access control for such an entity is delegated
    # to the node it points to. none_as_null=True: without it, SQLAlchemy's
    # JSON type stores Python None as the JSON literal 'null' (a non-NULL
    # string), which would defeat both the trigger's and our own
    # IS NULL / IS NOT NULL checks.
    Column("access_blob", JSON(none_as_null=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("entities_node_id_idx", "node_id"),
    Index("entities_type_created_idx", "entity_type", "created_at"),
    Index("entities_uri_idx", "uri"),
)

_ENTITY_NODE_ACCESS_BLOB_ERROR = (
    "An entity with node_id set must not have its own access_blob; "
    "access is controlled by the referenced node."
)


@event.listens_for(_entities, "after_create")
def _create_entities_node_access_blob_trigger(target, connection, **kw):
    """
    Enforce, at the database level, that an entity pointing to a catalog
    node (node_id set) does not also carry its own access_blob. This
    mirrors the trigger created by the
    c31f6a1d7e20_add_graph_entities_and_links_tables alembic migration,
    which applies the same DDL for databases provisioned via `alembic
    upgrade` rather than `create_all` (e.g. GraphSQLAlchemyStore's own
    schema initialization, which is what the test suite exercises).
    """
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            text(
                f"""
CREATE TRIGGER entities_node_access_blob_insert
BEFORE INSERT ON entities
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{_ENTITY_NODE_ACCESS_BLOB_ERROR}');
END"""
            )
        )
        connection.execute(
            text(
                f"""
CREATE TRIGGER entities_node_access_blob_update
BEFORE UPDATE ON entities
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{_ENTITY_NODE_ACCESS_BLOB_ERROR}');
END"""
            )
        )
    elif connection.engine.dialect.name == "postgresql":
        connection.execute(
            text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '{_ENTITY_NODE_ACCESS_BLOB_ERROR}';
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER entities_node_access_blob_check
BEFORE INSERT OR UPDATE ON entities
FOR EACH ROW
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
EXECUTE FUNCTION entities_reject_node_access_blob();"""
            )
        )


_links = Table(
    "links",
    _metadata,
    Column("id", String, primary_key=True),
    Column(
        "subject_id",
        String,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("predicate", String, nullable=False),
    Column(
        "object_id",
        String,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("properties", JSON, nullable=False),
    Column("access_blob", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("links_subject_predicate_idx", "subject_id", "predicate"),
    Index("links_predicate_object_idx", "predicate", "object_id"),
    Index("links_triple_idx", "subject_id", "predicate", "object_id"),
)

_namespaces = Table(
    "namespaces",
    _metadata,
    Column("prefix", String, primary_key=True),
    Column("uri", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


def _access_blob_condition(
    dialect_name: str, access_blob_column, query: AccessBlobFilter
):
    """
    Translate one AccessBlobFilter into a SQL condition on a JSON
    access_blob column, mirroring tiled.catalog.adapter.access_blob_filter
    so that pagination (LIMIT/OFFSET) is applied to already-filtered rows
    instead of filtering a page after the fact.
    """
    if not (query.user_id or query.tags):
        # Results cannot possibly match an empty value or list,
        # so put a False condition in the list ensuring that
        # there are no rows returned.
        return false()
    if dialect_name == "sqlite":
        access_tags_json = func.json_each(access_blob_column["tags"]).table_valued(
            "value"
        )
        condition = (
            select(1)
            .select_from(access_tags_json)
            .where(access_tags_json.c.value.in_(query.tags))
            .exists()
        )
        if query.user_id is not None:
            user_match = (
                func.json_extract(func.json_quote(access_blob_column["user"]), "$")
                == query.user_id
            )
            condition = or_(condition, user_match)
    elif dialect_name == "postgresql":
        access_blob_jsonb = type_coerce(access_blob_column, JSONB)
        condition = access_blob_jsonb["tags"].has_any(sql_cast(query.tags, ARRAY(TEXT)))
        if query.user_id is not None:
            user_match = access_blob_jsonb["user"].astext == query.user_id
            condition = or_(condition, user_match)
    else:
        raise UnsupportedQueryType("access_blob_filter")
    return condition


def _access_filters_condition(
    dialect_name: str, access_blob_column, queries: list[AccessBlobFilter]
):
    condition = _access_blob_condition(dialect_name, access_blob_column, queries[0])
    for query in queries[1:]:
        condition = and_(
            condition, _access_blob_condition(dialect_name, access_blob_column, query)
        )
    return condition


class GraphSQLAlchemyStore:
    """
    Async SQLAlchemy-backed store that can reuse Tiled's shared DB pool.

    Use ``from_database_settings`` to attach to the same async engine registry
    used by the rest of the server. The graph tables are provisioned by the
    catalog database (see ``tiled.graph.orm``); this store does not create
    them.
    """

    def __init__(self, engine: AsyncEngine, owns_engine: bool = False) -> None:
        self._engine = engine
        self._owns_engine = owns_engine

    @classmethod
    async def from_database_settings(
        cls,
        database_settings: DatabaseSettings,
    ) -> "GraphSQLAlchemyStore":
        engine = get_database_engine(database_settings)
        return cls(engine, owns_engine=False)

    @staticmethod
    def _to_entity(row) -> EntityRecord:
        return EntityRecord(
            id=row.id,
            node_id=row.node_id,
            entity_type=row.entity_type,
            name=row.name,
            uri=row.uri,
            properties=row.properties or {},
            # Preserve None as-is (rather than coercing to {}): None means
            # access control is delegated to the node (node_id is set).
            access_blob=row.access_blob,
            created_at=row.created_at,
        )

    @staticmethod
    def _to_link(row) -> LinkRecord:
        return LinkRecord(
            id=row.id,
            subject_id=row.subject_id,
            predicate=row.predicate,
            object_id=row.object_id,
            properties=row.properties or {},
            access_blob=row.access_blob or {},
            created_at=row.created_at,
        )

    async def create_entity(
        self,
        entity_type: str,
        name: str,
        node_id: Optional[int] = None,
        uri: Optional[str] = None,
        properties: Optional[dict] = None,
        access_blob: Optional[dict] = None,
    ) -> EntityRecord:
        id_ = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        async with self._engine.begin() as conn:
            await conn.execute(
                insert(_entities).values(
                    id=id_,
                    node_id=node_id,
                    entity_type=entity_type,
                    name=name,
                    uri=uri,
                    properties=properties or {},
                    # Not coerced to {}: passing None here (as the caller
                    # must when node_id is set) stores SQL NULL.
                    access_blob=access_blob,
                    created_at=now,
                )
            )
            row = (
                await conn.execute(select(_entities).where(_entities.c.id == id_))
            ).one()
        return self._to_entity(row)

    async def get_entity(self, id: str) -> Optional[EntityRecord]:
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(select(_entities).where(_entities.c.id == id))
            ).one_or_none()
        return self._to_entity(row) if row else None

    async def get_node_access_blob(self, node_id: int) -> Optional[dict]:
        """
        Look up a catalog node's access_blob, for resolving the effective
        access control of an entity that points to it (node_id is set).
        """
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    select(_nodes.c.access_blob).where(_nodes.c.id == node_id)
                )
            ).one_or_none()
        return row.access_blob if row else None

    async def list_entities(
        self,
        entity_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        access_filters: Optional[list[AccessBlobFilter]] = None,
    ) -> list[EntityRecord]:
        stmt = select(_entities).order_by(_entities.c.created_at)
        if entity_type is not None:
            stmt = stmt.where(_entities.c.entity_type == entity_type)
        if access_filters:
            dialect_name = self._engine.url.get_dialect().name
            # An entity's effective access_blob is its node's access_blob
            # when node_id is set, else its own access_blob.
            effective_access_blob = type_coerce(
                case(
                    (_entities.c.node_id.isnot(None), _nodes.c.access_blob),
                    else_=_entities.c.access_blob,
                ),
                JSON,
            )
            stmt = stmt.outerjoin(_nodes, _entities.c.node_id == _nodes.c.id).where(
                _access_filters_condition(
                    dialect_name, effective_access_blob, access_filters
                )
            )
        stmt = stmt.limit(limit).offset(offset)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(stmt)).all()
        return [self._to_entity(r) for r in rows]

    async def delete_entity(self, id: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(delete(_entities).where(_entities.c.id == id))
        return result.rowcount > 0

    async def update_entity(
        self,
        id: str,
        name: Optional[str] = None,
        node_id: object = UNSET,
        uri: object = UNSET,
        entity_type: Optional[str] = None,
        access_blob: object = UNSET,
    ) -> Optional[EntityRecord]:
        values: dict = {}
        if name is not None:
            values["name"] = name
        if node_id is not UNSET:
            values["node_id"] = node_id
        if uri is not UNSET:
            values["uri"] = uri
        if entity_type is not None:
            values["entity_type"] = entity_type
        if access_blob is not UNSET:
            values["access_blob"] = access_blob
        async with self._engine.begin() as conn:
            if values:
                await conn.execute(
                    update(_entities).where(_entities.c.id == id).values(**values)
                )
            row = (
                await conn.execute(select(_entities).where(_entities.c.id == id))
            ).one_or_none()
        return self._to_entity(row) if row else None

    async def create_link(
        self,
        subject_id: str,
        predicate: str,
        object_id: str,
        properties: Optional[dict] = None,
        access_blob: Optional[dict] = None,
    ) -> LinkRecord:
        id_ = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        # The subject_id/object_id foreign keys reference entities.id, so the
        # database rejects a link to a nonexistent entity (SQLite enforces this
        # too: the shared pool sets PRAGMA foreign_keys=ON). Insert directly and
        # let the constraint do the checking, rather than pre-querying both
        # endpoints on every create.
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    insert(_links).values(
                        id=id_,
                        subject_id=subject_id,
                        predicate=predicate,
                        object_id=object_id,
                        properties=properties or {},
                        access_blob=access_blob or {},
                        created_at=now,
                    )
                )
                row = (
                    await conn.execute(select(_links).where(_links.c.id == id_))
                ).one()
        except IntegrityError as exc:
            # A foreign-key violation means one of the endpoints is missing.
            # Resolve which one only on this failure path so the success path
            # stays a single INSERT.
            if not await self.get_entity(subject_id):
                raise ValueError(f"Subject entity '{subject_id}' not found") from exc
            if not await self.get_entity(object_id):
                raise ValueError(f"Object entity '{object_id}' not found") from exc
            raise
        return self._to_link(row)

    async def get_link(self, id: str) -> Optional[LinkRecord]:
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(select(_links).where(_links.c.id == id))
            ).one_or_none()
        return self._to_link(row) if row else None

    async def find_links(
        self,
        subject_id: Optional[str] = None,
        predicate: Optional[str] = None,
        object_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        access_filters: Optional[list[AccessBlobFilter]] = None,
    ) -> list[LinkRecord]:
        stmt = select(_links).order_by(_links.c.created_at)
        if subject_id is not None:
            stmt = stmt.where(_links.c.subject_id == subject_id)
        if predicate is not None:
            stmt = stmt.where(_links.c.predicate == predicate)
        if object_id is not None:
            stmt = stmt.where(_links.c.object_id == object_id)
        if access_filters:
            dialect_name = self._engine.url.get_dialect().name
            stmt = stmt.where(
                _access_filters_condition(
                    dialect_name, _links.c.access_blob, access_filters
                )
            )
        stmt = stmt.limit(limit).offset(offset)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(stmt)).all()
        return [self._to_link(r) for r in rows]

    async def delete_link(self, id: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(delete(_links).where(_links.c.id == id))
        return result.rowcount > 0

    async def update_link(
        self,
        id: str,
        predicate: object = UNSET,
        access_blob: object = UNSET,
    ) -> Optional[LinkRecord]:
        values: dict = {}
        if predicate is not UNSET:
            values["predicate"] = predicate
        if access_blob is not UNSET:
            values["access_blob"] = access_blob
        async with self._engine.begin() as conn:
            if values:
                await conn.execute(
                    update(_links).where(_links.c.id == id).values(**values)
                )
            row = (
                await conn.execute(select(_links).where(_links.c.id == id))
            ).one_or_none()
        return self._to_link(row) if row else None

    async def upsert_namespace(self, prefix: str, uri: str) -> None:
        if not prefix:
            raise ValueError("prefix must not be empty")
        if not uri:
            raise ValueError("uri must not be empty")

        async with self._engine.begin() as conn:
            existing = (
                await conn.execute(
                    select(_namespaces).where(_namespaces.c.prefix == prefix)
                )
            ).one_or_none()
            if existing is None:
                await conn.execute(
                    insert(_namespaces).values(
                        prefix=prefix,
                        uri=uri,
                        created_at=datetime.now(timezone.utc),
                    )
                )
            else:
                await conn.execute(
                    update(_namespaces)
                    .where(_namespaces.c.prefix == prefix)
                    .values(uri=uri)
                )

    async def list_namespaces(self) -> dict[str, str]:
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(select(_namespaces).order_by(_namespaces.c.prefix))
            ).all()
        return {row.prefix: row.uri for row in rows}

    async def delete_namespace(self, prefix: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                delete(_namespaces).where(_namespaces.c.prefix == prefix)
            )
        return result.rowcount > 0

    async def resolve_node_id(self, path: list[str]) -> Optional[int]:
        """
        Look up the internal catalog node id for a path of key segments,
        e.g. ``["raw_dataset"]`` for a top-level entry or ``["a", "b"]``
        for a nested one. Returns None if no such node exists.

        The catalog's root node always has id 0 (see
        tiled.catalog.adapter.node_from_segments, which this mirrors).
        """
        if not path:
            return 0
        aliases = [_nodes.alias() for _ in path] + [_nodes]
        statement = select(aliases[-1].c.id).select_from(aliases[0])
        statement = statement.where(aliases[0].c.id == 0)
        for i, segment in enumerate(path):
            parent, child = aliases[i], aliases[i + 1]
            statement = statement.join(child, child.c.parent == parent.c.id).where(
                child.c.key == segment
            )
        async with self._engine.connect() as conn:
            row = (await conn.execute(statement)).one_or_none()
        return row.id if row else None

    async def close(self) -> None:
        if self._owns_engine:
            await self._engine.dispose()
