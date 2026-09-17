"""
SQLAlchemy Core table definitions for the graph (splash-links) feature.

These tables live in the catalog database alongside the catalog's own tables
(``entities.node_id`` is a foreign key into the catalog ``nodes`` table). They
are attached to the catalog's ``Base.metadata`` so that the two supported ways
of provisioning a catalog database both include them:

* a fresh database created by ``tiled.catalog.core.initialize_database``
  (which runs ``Base.metadata.create_all``), and
* an existing database upgraded through the Alembic migration
  ``c31f6a1d7e20``.

The store (``tiled.graph.store``) uses these ``Table`` objects to read and
write rows; it does not create them itself. This mirrors how ``metadata_fts5``
is declared as a Core table on ``Base.metadata`` in ``tiled.catalog.orm``.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    event,
    text,
)

from ..catalog.base import Base

metadata = Base.metadata

entities = Table(
    "entities",
    metadata,
    Column("id", String, primary_key=True),
    Column(
        "node_id",
        Integer,
        ForeignKey("nodes.id", ondelete="CASCADE"),
        nullable=True,
    ),
    Column("entity_type", String, nullable=False),
    Column("name", String, nullable=False),
    Column("uri", String, nullable=True),
    Column("properties", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("entities_node_id_idx", "node_id"),
    Index("entities_type_created_idx", "entity_type", "created_at"),
    Index("entities_uri_idx", "uri"),
)

ENTITY_NODE_ACCESS_BLOB_ERROR = (
    "An entity with node_id set must not have its own access_blob; "
    "access is controlled by the referenced node."
)


links = Table(
    "links",
    metadata,
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
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("links_subject_predicate_idx", "subject_id", "predicate"),
    Index("links_predicate_object_idx", "predicate", "object_id"),
    Index("links_triple_idx", "subject_id", "predicate", "object_id"),
)

link_access_blobs = Table(
    "link_access_blobs",
    metadata,
    Column(
        "link_id",
        String,
        ForeignKey("links.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "access_blob_id",
        Integer,
        ForeignKey("access_blobs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    ),
)

entity_access_blobs = Table(
    "entity_access_blobs",
    metadata,
    Column(
        "entity_id",
        String,
        ForeignKey("entities.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "access_blob_id",
        Integer,
        ForeignKey("access_blobs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    ),
)


@event.listens_for(entity_access_blobs, "after_create")
def _create_entities_node_access_blob_trigger(target, connection, **kw):
    """
    Enforce, at the database level, that an entity pointing to a catalog
    node (node_id set) does not also carry its own access_blob.
    """
    if connection.engine.dialect.name == "sqlite":
        # Only an UPDATE that sets node_id can turn an existing blob-bearing
        # entity into an illegal node-backed-with-blob row. The INSERT case
        # cannot occur in a single statement: an entities INSERT sets only
        # node_id, while the blob lives in a separate entity_access_blobs row
        # written by a separate INSERT -- which is itself guarded by
        # entity_access_blobs_insert_reject_node_backed_entity. This mirrors
        # the PostgreSQL branch, which likewise only guards UPDATE OF node_id.
        connection.execute(
            text(
                f"""
CREATE TRIGGER IF NOT EXISTS entities_node_access_blob_update
BEFORE UPDATE OF node_id ON entities
WHEN (NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
))
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_BLOB_ERROR}');
END"""
            )
        )
    elif connection.engine.dialect.name == "postgresql":
        # PostgreSQL does not allow subqueries in a trigger WHEN clause
        # ("cannot use subquery in trigger WHEN condition"), so the EXISTS
        # check against entity_access_blobs lives in the function body; the
        # trigger WHEN clause keeps only the cheap scalar column test.
        connection.execute(
            text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
    ) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_BLOB_ERROR}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        # OR REPLACE keeps this belt-and-suspenders idempotent even though the
        # table-level listener already fires only once (PostgreSQL 14+).
        connection.execute(
            text(
                """
CREATE OR REPLACE TRIGGER entities_node_access_blob_check
BEFORE UPDATE OF node_id ON entities
FOR EACH ROW
WHEN (NEW.node_id IS NOT NULL)
EXECUTE FUNCTION entities_reject_node_access_blob();"""
            )
        )


@event.listens_for(link_access_blobs, "after_create")
def _create_link_access_blob_cleanup_trigger(target, connection, **kw):
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            text(
                """
CREATE TRIGGER link_access_blobs_delete_cleanup
AFTER DELETE ON link_access_blobs
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
END"""
            )
        )
    elif connection.engine.dialect.name == "postgresql":
        connection.execute(
            text(
                """
CREATE OR REPLACE FUNCTION delete_link_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER link_access_blobs_delete_cleanup
AFTER DELETE ON link_access_blobs
FOR EACH ROW EXECUTE FUNCTION delete_link_access_blob();
"""
            )
        )


@event.listens_for(link_access_blobs, "after_create")
def _create_access_blob_association_triggers(target, connection, **kw):
    tables = ("node_access_blobs", "entity_access_blobs", "link_access_blobs")
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            text(
                """
CREATE TRIGGER entity_access_blobs_delete_cleanup
AFTER DELETE ON entity_access_blobs
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
END"""
            )
        )
        for operation in ("INSERT", "UPDATE OF entity_id"):
            connection.execute(
                text(
                    f"""
CREATE TRIGGER entity_access_blobs_{operation.split()[0].lower()}_reject_node_backed_entity
BEFORE {operation} ON entity_access_blobs
WHEN EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_BLOB_ERROR}');
END"""
                )
            )
        for table in tables:
            others = " OR ".join(
                f"EXISTS (SELECT 1 FROM {other} WHERE access_blob_id = NEW.access_blob_id)"
                for other in tables
                if other != table
            )
            for operation in ("INSERT", "UPDATE OF access_blob_id"):
                connection.execute(
                    text(
                        f"""
CREATE TRIGGER {table}_{operation.split()[0].lower()}_reject_shared_access_blob
BEFORE {operation} ON {table}
WHEN {others}
BEGIN
    SELECT RAISE(ABORT, 'An access blob may belong to only one node, entity, or link');
END"""
                    )
                )
    elif connection.engine.dialect.name == "postgresql":
        # asyncpg cannot run multiple statements in one prepared execute, so
        # each CREATE FUNCTION / CREATE TRIGGER is issued individually.
        connection.execute(
            text(
                """
CREATE OR REPLACE FUNCTION delete_entity_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER entity_access_blobs_delete_cleanup
AFTER DELETE ON entity_access_blobs
FOR EACH ROW EXECUTE FUNCTION delete_entity_access_blob();"""
            )
        )
        connection.execute(
            text(
                f"""
CREATE OR REPLACE FUNCTION entity_access_blob_reject_node_backed_entity()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_BLOB_ERROR}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER entity_access_blobs_reject_node_backed_entity
BEFORE INSERT OR UPDATE OF entity_id ON entity_access_blobs
FOR EACH ROW EXECUTE FUNCTION entity_access_blob_reject_node_backed_entity();"""
            )
        )
        connection.execute(
            text(
                """
CREATE OR REPLACE FUNCTION reject_shared_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_TABLE_NAME = 'node_access_blobs' AND EXISTS (
        SELECT 1 FROM entity_access_blobs WHERE access_blob_id = NEW.access_blob_id
        UNION ALL SELECT 1 FROM link_access_blobs WHERE access_blob_id = NEW.access_blob_id
    )) OR (TG_TABLE_NAME = 'entity_access_blobs' AND EXISTS (
        SELECT 1 FROM node_access_blobs WHERE access_blob_id = NEW.access_blob_id
        UNION ALL SELECT 1 FROM link_access_blobs WHERE access_blob_id = NEW.access_blob_id
    )) OR (TG_TABLE_NAME = 'link_access_blobs' AND EXISTS (
        SELECT 1 FROM node_access_blobs WHERE access_blob_id = NEW.access_blob_id
        UNION ALL SELECT 1 FROM entity_access_blobs WHERE access_blob_id = NEW.access_blob_id
    )) THEN
        RAISE EXCEPTION 'An access blob may belong to only one node, entity, or link';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        for table in tables:
            connection.execute(
                text(
                    f"""
CREATE TRIGGER {table}_reject_shared_access_blob
BEFORE INSERT OR UPDATE OF access_blob_id ON {table}
FOR EACH ROW EXECUTE FUNCTION reject_shared_access_blob();"""
                )
            )


namespaces = Table(
    "namespaces",
    metadata,
    Column("prefix", String, primary_key=True),
    Column("uri", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
