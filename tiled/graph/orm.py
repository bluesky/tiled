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

ENTITY_NODE_ACCESS_BLOB_ERROR = (
    "An entity with node_id set must not have its own access_blob; "
    "access is controlled by the referenced node."
)


@event.listens_for(entities, "after_create")
def _create_entities_node_access_blob_trigger(target, connection, **kw):
    """
    Enforce, at the database level, that an entity pointing to a catalog
    node (node_id set) does not also carry its own access_blob. This
    mirrors the trigger created by the
    c31f6a1d7e20_add_graph_entities_and_links_tables alembic migration,
    which applies the same DDL for databases provisioned via `alembic
    upgrade` rather than `create_all` (e.g.
    tiled.catalog.core.initialize_database, which is what the test suite
    exercises).
    """
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            text(
                f"""
CREATE TRIGGER entities_node_access_blob_insert
BEFORE INSERT ON entities
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_BLOB_ERROR}');
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
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_BLOB_ERROR}');
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
    RAISE EXCEPTION '{ENTITY_NODE_ACCESS_BLOB_ERROR}';
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
    Column("access_blob", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("links_subject_predicate_idx", "subject_id", "predicate"),
    Index("links_predicate_object_idx", "predicate", "object_id"),
    Index("links_triple_idx", "subject_id", "predicate", "object_id"),
)

namespaces = Table(
    "namespaces",
    metadata,
    Column("prefix", String, primary_key=True),
    Column("uri", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
