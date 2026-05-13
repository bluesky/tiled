"""
Storage layer for the splash-links entity graph service.

The abstract ``Store`` interface decouples the application from the
underlying database.  The concrete ``SQLAlchemyStore`` targets any database
supported by SQLAlchemy 2.x — SQLite (default), PostgreSQL, and DuckDB (via
``duckdb-engine``) are the primary targets.

Connection URL examples
-----------------------
SQLite (file):     sqlite:///links.sqlite
SQLite (memory):   sqlite:///:memory:
PostgreSQL:        postgresql+psycopg2://user:pass@host/dbname
DuckDB (file):     duckdb:///links.duckdb
DuckDB (memory):   duckdb:///:memory:
"""

from __future__ import annotations

import abc
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
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    event,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Data records
# ---------------------------------------------------------------------------


class EntityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    entity_type: str
    name: str
    uri: Optional[str]
    properties: dict
    created_at: datetime


class LinkRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    subject_id: str
    predicate: str
    object_id: str
    properties: dict
    created_at: datetime


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------


class Store(abc.ABC):
    """Minimal interface for entity/link persistence."""

    @abc.abstractmethod
    def create_entity(
        self,
        entity_type: str,
        name: str,
        uri: Optional[str] = None,
        properties: Optional[dict] = None,
    ) -> EntityRecord: ...

    @abc.abstractmethod
    def get_entity(self, id: str) -> Optional[EntityRecord]: ...

    @abc.abstractmethod
    def list_entities(
        self,
        entity_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EntityRecord]: ...

    @abc.abstractmethod
    def delete_entity(self, id: str) -> bool: ...

    @abc.abstractmethod
    def update_entity(
        self,
        id: str,
        name: Optional[str] = None,
        uri: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> Optional[EntityRecord]: ...

    @abc.abstractmethod
    def create_link(
        self,
        subject_id: str,
        predicate: str,
        object_id: str,
        properties: Optional[dict] = None,
    ) -> LinkRecord: ...

    @abc.abstractmethod
    def get_link(self, id: str) -> Optional[LinkRecord]: ...

    @abc.abstractmethod
    def find_links(
        self,
        subject_id: Optional[str] = None,
        predicate: Optional[str] = None,
        object_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[LinkRecord]: ...

    @abc.abstractmethod
    def delete_link(self, id: str) -> bool: ...

    @abc.abstractmethod
    def update_link(self, id: str, predicate: str) -> Optional[LinkRecord]: ...

    @abc.abstractmethod
    def close(self) -> None: ...


# ---------------------------------------------------------------------------
# SQLAlchemy schema
# ---------------------------------------------------------------------------

_metadata = MetaData()

_entities = Table(
    "entities",
    _metadata,
    Column("id", String, primary_key=True),
    Column("entity_type", String, nullable=False),
    Column("name", String, nullable=False),
    Column("uri", String, nullable=True),
    Column("properties", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("entities_type_created_idx", "entity_type", "created_at"),
    Index("entities_uri_idx", "uri"),
)

_links = Table(
    "links",
    _metadata,
    Column("id", String, primary_key=True),
    Column("subject_id", String, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False),
    Column("predicate", String, nullable=False),
    Column("object_id", String, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False),
    Column("properties", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("links_subject_predicate_idx", "subject_id", "predicate"),
    Index("links_predicate_object_idx", "predicate", "object_id"),
    Index("links_triple_idx", "subject_id", "predicate", "object_id"),
)


def _make_engine(db_url: str) -> Engine:
    """Create a SQLAlchemy engine from a URL, applying dialect-specific tuning."""
    is_sqlite = db_url.startswith("sqlite")
    is_memory = ":memory:" in db_url

    kwargs: dict = {}
    if is_sqlite:
        kwargs["connect_args"] = {"check_same_thread": False}
    if is_memory:
        kwargs["poolclass"] = StaticPool

    engine = create_engine(db_url, **kwargs)

    if is_sqlite:
        # Enable foreign-key enforcement for every new SQLite connection.
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(conn, _record):
            conn.execute("PRAGMA foreign_keys=ON")

    return engine


def _url_from_path(db_path: str) -> str:
    """Convert a plain file path / ':memory:' to a sqlite:// URL."""
    if "://" in db_path:
        return db_path
    if db_path == ":memory:":
        return "sqlite:///:memory:"
    return f"sqlite:///{db_path}"


# ---------------------------------------------------------------------------
# SQLAlchemy implementation
# ---------------------------------------------------------------------------


class SQLAlchemyStore(Store):
    """
    Database-agnostic store backed by SQLAlchemy Core.

    ``db_url`` may be any SQLAlchemy connection URL.  For convenience,
    plain file paths and ``':memory:'`` are auto-converted to
    ``sqlite:///…`` / ``sqlite:///:memory:``.
    """

    def __init__(self, db_url: str = ":memory:") -> None:
        self._engine: Engine = _make_engine(_url_from_path(db_url))
        _metadata.create_all(self._engine)

    # ------------------------------------------------------------------
    # Row conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_entity(row) -> EntityRecord:
        return EntityRecord(
            id=row.id,
            entity_type=row.entity_type,
            name=row.name,
            uri=row.uri,
            properties=row.properties or {},
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
            created_at=row.created_at,
        )

    # ------------------------------------------------------------------
    # Entity operations
    # ------------------------------------------------------------------

    def create_entity(
        self,
        entity_type: str,
        name: str,
        uri: Optional[str] = None,
        properties: Optional[dict] = None,
    ) -> EntityRecord:
        id_ = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            conn.execute(
                insert(_entities).values(
                    id=id_,
                    entity_type=entity_type,
                    name=name,
                    uri=uri,
                    properties=properties or {},
                    created_at=now,
                )
            )
            row = conn.execute(select(_entities).where(_entities.c.id == id_)).one()
        return self._to_entity(row)

    def get_entity(self, id: str) -> Optional[EntityRecord]:
        with self._engine.connect() as conn:
            row = conn.execute(select(_entities).where(_entities.c.id == id)).one_or_none()
        return self._to_entity(row) if row else None

    def list_entities(
        self,
        entity_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EntityRecord]:
        stmt = select(_entities).order_by(_entities.c.created_at).limit(limit).offset(offset)
        if entity_type is not None:
            stmt = stmt.where(_entities.c.entity_type == entity_type)
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [self._to_entity(r) for r in rows]

    def delete_entity(self, id: str) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(delete(_entities).where(_entities.c.id == id))
        return result.rowcount > 0

    def update_entity(
        self,
        id: str,
        name: Optional[str] = None,
        uri: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> Optional[EntityRecord]:
        values: dict = {}
        if name is not None:
            values["name"] = name
        if uri is not None:
            values["uri"] = uri
        if entity_type is not None:
            values["entity_type"] = entity_type
        with self._engine.begin() as conn:
            if values:
                conn.execute(update(_entities).where(_entities.c.id == id).values(**values))
            row = conn.execute(select(_entities).where(_entities.c.id == id)).one_or_none()
        return self._to_entity(row) if row else None

    # ------------------------------------------------------------------
    # Link operations
    # ------------------------------------------------------------------

    def create_link(
        self,
        subject_id: str,
        predicate: str,
        object_id: str,
        properties: Optional[dict] = None,
    ) -> LinkRecord:
        if not self.get_entity(subject_id):
            raise ValueError(f"Subject entity '{subject_id}' not found")
        if not self.get_entity(object_id):
            raise ValueError(f"Object entity '{object_id}' not found")

        id_ = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            conn.execute(
                insert(_links).values(
                    id=id_,
                    subject_id=subject_id,
                    predicate=predicate,
                    object_id=object_id,
                    properties=properties or {},
                    created_at=now,
                )
            )
            row = conn.execute(select(_links).where(_links.c.id == id_)).one()
        return self._to_link(row)

    def get_link(self, id: str) -> Optional[LinkRecord]:
        with self._engine.connect() as conn:
            row = conn.execute(select(_links).where(_links.c.id == id)).one_or_none()
        return self._to_link(row) if row else None

    def find_links(
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
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [self._to_link(r) for r in rows]

    def delete_link(self, id: str) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(delete(_links).where(_links.c.id == id))
        return result.rowcount > 0

    def update_link(self, id: str, predicate: str) -> Optional[LinkRecord]:
        with self._engine.begin() as conn:
            conn.execute(update(_links).where(_links.c.id == id).values(predicate=predicate))
            row = conn.execute(select(_links).where(_links.c.id == id)).one_or_none()
        return self._to_link(row) if row else None

    def close(self) -> None:
        self._engine.dispose()


# Backward-compatible aliases
SQLiteStore = SQLAlchemyStore
DuckDBStore = SQLAlchemyStore
