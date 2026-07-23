"""
Storage layer for the splash-links entity graph service.

``GraphSQLAlchemyStore`` attaches to the same process-global async
engine/connection pool used by Tiled's catalog (see
``tiled.server.connection_pool``), so the graph tables and the catalog
tables are always served from a single shared pool rather than opening a
second connection pool to the same database.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncEngine

from ..server.connection_pool import get_database_engine
from ..server.settings import DatabaseSettings

UNSET = object()

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
    access_blob: dict
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

# Register the catalog nodes table key so entities.node_id can resolve
# ForeignKey("nodes.id") when SQLAlchemy sorts DDL dependencies.
Table(
    "nodes",
    _metadata,
    Column("id", Integer, primary_key=True),
    extend_existing=True,
)

_entities = Table(
    "entities",
    _metadata,
    Column("id", String, primary_key=True),
    Column(
        "node_id",
        Integer,
        ForeignKey("nodes.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("entity_type", String, nullable=False),
    Column("name", String, nullable=False),
    Column("uri", String, nullable=True),
    Column("properties", JSON, nullable=False),
    Column("access_blob", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("entities_node_id_idx", "node_id"),
    Index("entities_type_created_idx", "entity_type", "created_at"),
    Index("entities_uri_idx", "uri"),
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


class GraphSQLAlchemyStore:
    """
    Async SQLAlchemy-backed store that can reuse Tiled's shared DB pool.

    Use ``from_database_settings`` to attach to the same async engine registry
    used by the rest of the server.
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
        store = cls(engine, owns_engine=False)
        await store._initialize_schema()
        return store

    @staticmethod
    def _to_entity(row) -> EntityRecord:
        return EntityRecord(
            id=row.id,
            node_id=row.node_id,
            entity_type=row.entity_type,
            name=row.name,
            uri=row.uri,
            properties=row.properties or {},
            access_blob=row.access_blob or {},
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

    async def _initialize_schema(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(_metadata.create_all)

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
                    access_blob=access_blob or {},
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

    async def list_entities(
        self,
        entity_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EntityRecord]:
        stmt = (
            select(_entities)
            .order_by(_entities.c.created_at)
            .limit(limit)
            .offset(offset)
        )
        if entity_type is not None:
            stmt = stmt.where(_entities.c.entity_type == entity_type)
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
        if not await self.get_entity(subject_id):
            raise ValueError(f"Subject entity '{subject_id}' not found")
        if not await self.get_entity(object_id):
            raise ValueError(f"Object entity '{object_id}' not found")

        id_ = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
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
            row = (await conn.execute(select(_links).where(_links.c.id == id_))).one()
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
    ) -> list[LinkRecord]:
        stmt = select(_links).order_by(_links.c.created_at).limit(limit).offset(offset)
        if subject_id is not None:
            stmt = stmt.where(_links.c.subject_id == subject_id)
        if predicate is not None:
            stmt = stmt.where(_links.c.predicate == predicate)
        if object_id is not None:
            stmt = stmt.where(_links.c.object_id == object_id)
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

    async def close(self) -> None:
        if self._owns_engine:
            await self._engine.dispose()
