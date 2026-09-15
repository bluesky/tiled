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

Access control: entities and links carry access tags drawn from the catalog's
``access_tags`` table (the same tags nodes use), through the
``entity_access_tags`` and ``link_access_tags`` association tables. An entity
that points to a catalog node (``node_id`` set) carries no tags of its own;
it assumes the access tags of the referenced node.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Iterable, Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import and_, delete, false, insert, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from ..catalog.core import register_principal_tag_rows
from ..catalog.orm import AccessTag, Node, NodeAccessTag
from ..queries import AccessTagsFilter
from ..server.connection_pool import get_database_engine
from ..server.settings import DatabaseSettings
from .orm import entities as _entities
from .orm import entity_access_tags as _entity_access_tags
from .orm import link_access_tags as _link_access_tags
from .orm import links as _links
from .orm import namespaces as _namespaces

UNSET = object()

# Catalog tables, used to resolve entities.node_id by catalog path and to
# resolve/read access tags (shared with catalog nodes).
_nodes = Node.__table__
_access_tags = AccessTag.__table__
_node_access_tags = NodeAccessTag.__table__

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
    access_tags: Optional[frozenset[str]] = None
    created_at: datetime


class LinkRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    subject_id: str
    predicate: str
    object_id: str
    properties: dict
    access_tags: frozenset[str]
    created_at: datetime


def _access_tags_match_condition(
    assoc_table, assoc_id_column, owner_column, access_tag_names
):
    """
    Rows tagged with at least one of the given access tags.
    EXISTS is used (vs IN) as it is much more preformant in SQLite,
    though performance in Postgres appears similar for both.
    """
    return (
        select(assoc_id_column)
        .select_from(assoc_table)
        .join(_access_tags, _access_tags.c.id == assoc_table.c.tag_id)
        .where(assoc_id_column == owner_column)
        .where(_access_tags.c.name.in_(access_tag_names))
        .exists()
    )


def _link_access_condition(query: AccessTagsFilter):
    if not query.tags:
        # Nothing can match an empty tag list.
        return false()
    return _access_tags_match_condition(
        _link_access_tags, _link_access_tags.c.link_id, _links.c.id, query.tags
    )


def _entity_access_condition(query: AccessTagsFilter):
    """
    An entity matches if it carries one of the given tags itself, or, when it
    is node-backed (node_id set), if the referenced catalog node does.
    """
    if not query.tags:
        return false()
    return or_(
        and_(
            _entities.c.node_id.is_(None),
            _access_tags_match_condition(
                _entity_access_tags,
                _entity_access_tags.c.entity_id,
                _entities.c.id,
                query.tags,
            ),
        ),
        and_(
            _entities.c.node_id.isnot(None),
            _access_tags_match_condition(
                _node_access_tags,
                _node_access_tags.c.node_id,
                _entities.c.node_id,
                query.tags,
            ),
        ),
    )


def _access_filters_condition(condition_builder, queries: list[AccessTagsFilter]):
    condition = condition_builder(queries[0])
    for query in queries[1:]:
        condition = and_(condition, condition_builder(query))
    return condition


async def _resolve_access_tag_ids(conn, access_tag_names: Iterable[str]) -> list[int]:
    """
    Resolve access tag names to access_tags ids. An association cannot
    reference a tag that has no row, so unknown names raise. Normally the
    access policy has already validated the tags; this fires only for requests
    that bypassed the policy or raced a tag-definition resync.

    Principal tags are slightly different: these need to exist at write,
    possibly before the tags compiler has been able to create them.
    """
    names = set(access_tag_names)
    if not names:
        return []
    await register_principal_tag_rows(conn, names)
    rows = (
        await conn.execute(
            select(_access_tags.c.id, _access_tags.c.name).where(
                _access_tags.c.name.in_(names)
            )
        )
    ).all()
    missing = names - {row.name for row in rows}
    if missing:
        raise ValueError(
            f"Cannot apply access tags that are not defined: {sorted(missing)}"
        )
    return [row.id for row in rows]


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
    def _to_entity(row, access_tags: Optional[frozenset[str]]) -> EntityRecord:
        return EntityRecord(
            id=row.id,
            node_id=row.node_id,
            entity_type=row.entity_type,
            name=row.name,
            uri=row.uri,
            properties=row.properties or {},
            # None means access control is delegated to node_id.
            access_tags=(
                None if row.node_id is not None else (access_tags or frozenset())
            ),
            created_at=row.created_at,
        )

    @staticmethod
    def _to_link(row, access_tags: Optional[frozenset[str]]) -> LinkRecord:
        return LinkRecord(
            id=row.id,
            subject_id=row.subject_id,
            predicate=row.predicate,
            object_id=row.object_id,
            properties=row.properties or {},
            access_tags=access_tags or frozenset(),
            created_at=row.created_at,
        )

    @staticmethod
    async def _access_tags_by_id(conn, assoc_table, assoc_id_column, ids: list):
        """Map entity/link id -> frozenset of access tag names, one query per page."""
        if not ids:
            return {}
        rows = (
            await conn.execute(
                select(assoc_id_column, _access_tags.c.name)
                .select_from(assoc_table)
                .join(_access_tags, _access_tags.c.id == assoc_table.c.tag_id)
                .where(assoc_id_column.in_(ids))
            )
        ).all()
        access_tags_by_id: dict = {}
        for assoc_id, name in rows:
            access_tags_by_id.setdefault(assoc_id, set()).add(name)
        return {key: frozenset(value) for key, value in access_tags_by_id.items()}

    async def _entity_access_tags_by_id(self, conn, ids: list[str]):
        return await self._access_tags_by_id(
            conn, _entity_access_tags, _entity_access_tags.c.entity_id, ids
        )

    async def _link_access_tags_by_id(self, conn, ids: list[str]):
        return await self._access_tags_by_id(
            conn, _link_access_tags, _link_access_tags.c.link_id, ids
        )

    async def _entity_record(self, conn, id: str) -> Optional[EntityRecord]:
        row = (
            await conn.execute(select(_entities).where(_entities.c.id == id))
        ).one_or_none()
        if row is None:
            return None
        access_tags = (await self._entity_access_tags_by_id(conn, [id])).get(id)
        return self._to_entity(row, access_tags)

    async def _link_record(self, conn, id: str) -> Optional[LinkRecord]:
        row = (
            await conn.execute(select(_links).where(_links.c.id == id))
        ).one_or_none()
        if row is None:
            return None
        access_tags = (await self._link_access_tags_by_id(conn, [id])).get(id)
        return self._to_link(row, access_tags)

    async def _set_access_tags(
        self, conn, assoc_table, assoc_id_column_name: str, id: str, access_tag_names
    ) -> None:
        """Replace the access tag associations of an entity or link."""
        access_tag_ids = await _resolve_access_tag_ids(conn, access_tag_names)
        await conn.execute(
            delete(assoc_table).where(
                getattr(assoc_table.c, assoc_id_column_name) == id
            )
        )
        if access_tag_ids:
            await conn.execute(
                insert(assoc_table),
                [
                    {assoc_id_column_name: id, "tag_id": access_tag_id}
                    for access_tag_id in access_tag_ids
                ],
            )

    async def create_entity(
        self,
        entity_type: str,
        name: str,
        node_id: Optional[int] = None,
        uri: Optional[str] = None,
        properties: Optional[dict] = None,
        access_tags: Optional[Iterable[str]] = None,
    ) -> EntityRecord:
        id_ = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        if node_id is not None and access_tags:
            raise IntegrityError("entity node access tags", {}, None)
        async with self._engine.begin() as conn:
            await conn.execute(
                insert(_entities).values(
                    id=id_,
                    node_id=node_id,
                    entity_type=entity_type,
                    name=name,
                    uri=uri,
                    properties=properties or {},
                    created_at=now,
                )
            )
            if node_id is None and access_tags:
                access_tag_ids = await _resolve_access_tag_ids(conn, access_tags)
                await conn.execute(
                    insert(_entity_access_tags),
                    [
                        {"entity_id": id_, "tag_id": access_tag_id}
                        for access_tag_id in access_tag_ids
                    ],
                )
            record = await self._entity_record(conn, id_)
        return record

    async def get_entity(self, id: str) -> Optional[EntityRecord]:
        async with self._engine.connect() as conn:
            return await self._entity_record(conn, id)

    async def get_node_access_tags(self, node_id: int) -> Optional[frozenset[str]]:
        """
        Look up a catalog node's access tags, for resolving the effective
        access control of an entity that points to it (node_id is set).
        Returns None if the node does not exist. Returns raw stored tag
        names; the caller converts to the policy-facing AccessTags type.
        """
        async with self._engine.connect() as conn:
            exists = (
                await conn.execute(select(_nodes.c.id).where(_nodes.c.id == node_id))
            ).one_or_none()
            if exists is None:
                return None
            rows = (
                await conn.execute(
                    select(_access_tags.c.name)
                    .select_from(_node_access_tags)
                    .join(
                        _access_tags,
                        _access_tags.c.id == _node_access_tags.c.tag_id,
                    )
                    .where(_node_access_tags.c.node_id == node_id)
                )
            ).all()
        return frozenset(row.name for row in rows)

    async def list_entities(
        self,
        entity_type: Optional[str] = None,
        node_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        access_filters: Optional[list[AccessTagsFilter]] = None,
    ) -> list[EntityRecord]:
        stmt = select(_entities).order_by(_entities.c.created_at)
        if entity_type is not None:
            stmt = stmt.where(_entities.c.entity_type == entity_type)
        if node_id is not None:
            stmt = stmt.where(_entities.c.node_id == node_id)
        if access_filters:
            stmt = stmt.where(
                _access_filters_condition(_entity_access_condition, access_filters)
            )
        stmt = stmt.limit(limit).offset(offset)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(stmt)).all()
            access_tags_by_id = await self._entity_access_tags_by_id(
                conn, [row.id for row in rows]
            )
        return [self._to_entity(row, access_tags_by_id.get(row.id)) for row in rows]

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
        access_tags: object = UNSET,
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
        async with self._engine.begin() as conn:
            existing = await self._entity_record(conn, id)
            if existing is None:
                return None
            effective_node_id = node_id if node_id is not UNSET else existing.node_id
            if (
                access_tags is not UNSET
                and effective_node_id is not None
                and access_tags
            ):
                raise IntegrityError("entity node access tags", {}, None)
            if existing.node_id is None and effective_node_id is not None:
                # Becoming node-backed: shed own tags first, so the
                # entities.node_id trigger sees no remaining associations.
                await conn.execute(
                    delete(_entity_access_tags).where(
                        _entity_access_tags.c.entity_id == id
                    )
                )
            if values:
                await conn.execute(
                    update(_entities).where(_entities.c.id == id).values(**values)
                )
            if access_tags is not UNSET:
                if access_tags is None:
                    if effective_node_id is None:
                        raise IntegrityError(
                            "Refusing to clear access tags on a standalone entity "
                            "(no node_id): it would leave the entity without access "
                            "control. Provide a list of tags or set node_id.",
                            {},
                            None,
                        )
                    await conn.execute(
                        delete(_entity_access_tags).where(
                            _entity_access_tags.c.entity_id == id
                        )
                    )
                elif effective_node_id is None:
                    await self._set_access_tags(
                        conn, _entity_access_tags, "entity_id", id, access_tags
                    )
            record = await self._entity_record(conn, id)
        return record

    async def create_link(
        self,
        subject_id: str,
        predicate: str,
        object_id: str,
        properties: Optional[dict] = None,
        access_tags: Optional[Iterable[str]] = None,
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
                        created_at=now,
                    )
                )
                if access_tags:
                    access_tag_ids = await _resolve_access_tag_ids(conn, access_tags)
                    await conn.execute(
                        insert(_link_access_tags),
                        [
                            {"link_id": id_, "tag_id": access_tag_id}
                            for access_tag_id in access_tag_ids
                        ],
                    )
                record = await self._link_record(conn, id_)
        except IntegrityError as exc:
            # A foreign-key violation means one of the endpoints is missing.
            # Resolve which one only on this failure path so the success path
            # stays a single INSERT.
            if not await self.get_entity(subject_id):
                raise ValueError(f"Subject entity '{subject_id}' not found") from exc
            if not await self.get_entity(object_id):
                raise ValueError(f"Object entity '{object_id}' not found") from exc
            raise
        return record

    async def get_link(self, id: str) -> Optional[LinkRecord]:
        async with self._engine.connect() as conn:
            return await self._link_record(conn, id)

    async def find_links(
        self,
        subject_id: Optional[str] = None,
        predicate: Optional[str] = None,
        object_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        access_filters: Optional[list[AccessTagsFilter]] = None,
    ) -> list[LinkRecord]:
        stmt = select(_links).order_by(_links.c.created_at)
        if subject_id is not None:
            stmt = stmt.where(_links.c.subject_id == subject_id)
        if predicate is not None:
            stmt = stmt.where(_links.c.predicate == predicate)
        if object_id is not None:
            stmt = stmt.where(_links.c.object_id == object_id)
        if access_filters:
            stmt = stmt.where(
                _access_filters_condition(_link_access_condition, access_filters)
            )
        stmt = stmt.limit(limit).offset(offset)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(stmt)).all()
            access_tags_by_id = await self._link_access_tags_by_id(
                conn, [row.id for row in rows]
            )
        return [self._to_link(row, access_tags_by_id.get(row.id)) for row in rows]

    async def delete_link(self, id: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(delete(_links).where(_links.c.id == id))
        return result.rowcount > 0

    async def update_link(
        self,
        id: str,
        predicate: object = UNSET,
        access_tags: object = UNSET,
    ) -> Optional[LinkRecord]:
        values: dict = {}
        if predicate is not UNSET:
            values["predicate"] = predicate
        async with self._engine.begin() as conn:
            if values:
                await conn.execute(
                    update(_links).where(_links.c.id == id).values(**values)
                )
            if access_tags is not UNSET:
                await self._set_access_tags(
                    conn, _link_access_tags, "link_id", id, access_tags or []
                )
            record = await self._link_record(conn, id)
        return record

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
