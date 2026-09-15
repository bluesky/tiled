"""
SQLAlchemy Core table definitions for the graph (splash-links) feature.

These tables live in the catalog database alongside the catalog's own tables
(``entities.node_id`` is a foreign key into the catalog ``nodes`` table). They
are attached to the catalog's ``Base.metadata`` so that the two supported ways
of provisioning a catalog database both include them:

* a fresh database created by ``tiled.catalog.core.initialize_database``
  (which runs ``Base.metadata.create_all``), and
* an existing database upgraded through Alembic migrations.

The store (``tiled.graph.store``) uses these ``Table`` objects to read and
write rows; it does not create them itself. This mirrors how ``metadata_fts5``
is declared as a Core table on ``Base.metadata`` in ``tiled.catalog.orm``.

Access control: entities and links carry access tags drawn from the same
catalog ``access_tags`` table that nodes use. The ``entity_access_tags`` and
``link_access_tags`` association tables mirror the catalog's
``node_access_tags`` table. An entity that points to a catalog node
(``node_id`` set) must not carry its own access tags -- it assumes the tags of
the referenced node -- and this is enforced at the database level by triggers.
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

ENTITY_NODE_ACCESS_TAGS_ERROR = (
    "An entity with node_id set must not have its own access tags; "
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

# Association tables mapping entities/links to catalog access tags
# (many-to-many), mirroring the catalog's node_access_tags table. Both
# directions of lookup are served: "which tags are on this entity/link?"
# by the composite primary key, and "which entities/links have this tag?"
# by the covering reverse index. Deleting an entity, link, or tag cascades
# to its association rows via the foreign keys.

entity_access_tags = Table(
    "entity_access_tags",
    metadata,
    Column(
        "entity_id",
        String,
        ForeignKey(
            "entities.id", name="fk_entity_access_tags_entity", ondelete="CASCADE"
        ),
        primary_key=True,
    ),
    Column(
        "tag_id",
        Integer,
        ForeignKey(
            "access_tags.id", name="fk_entity_access_tags_tag", ondelete="CASCADE"
        ),
        primary_key=True,
    ),
    Index("idx_entity_access_tags_tag_id_entity_id", "tag_id", "entity_id"),
)

link_access_tags = Table(
    "link_access_tags",
    metadata,
    Column(
        "link_id",
        String,
        ForeignKey("links.id", name="fk_link_access_tags_link", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "tag_id",
        Integer,
        ForeignKey(
            "access_tags.id", name="fk_link_access_tags_tag", ondelete="CASCADE"
        ),
        primary_key=True,
    ),
    Index("idx_link_access_tags_tag_id_link_id", "tag_id", "link_id"),
)


@event.listens_for(entity_access_tags, "after_create")
def _create_entities_node_access_tags_triggers(target, connection, **kw):
    """
    Enforce, at the database level, that an entity pointing to a catalog
    node (node_id set) does not also carry its own access tags. Two paths
    could violate this: an UPDATE that sets node_id on an entity that has
    tag associations, and an INSERT/UPDATE on entity_access_tags that
    references a node-backed entity.
    """
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            text(
                f"""
CREATE TRIGGER IF NOT EXISTS entities_node_access_tags_update
BEFORE UPDATE OF node_id ON entities
WHEN (NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_tags WHERE entity_id = NEW.id
))
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_TAGS_ERROR}');
END"""
            )
        )
        for operation in ("INSERT", "UPDATE OF entity_id"):
            connection.execute(
                text(
                    f"""
CREATE TRIGGER IF NOT EXISTS entity_access_tags_{operation.split()[0].lower()}_reject_node_backed_entity
BEFORE {operation} ON entity_access_tags
WHEN EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_TAGS_ERROR}');
END"""
                )
            )
    elif connection.engine.dialect.name == "postgresql":
        # PostgreSQL does not allow subqueries in a trigger WHEN clause
        # ("cannot use subquery in trigger WHEN condition"), so the EXISTS
        # checks live in the function bodies; the trigger WHEN clause keeps
        # only the cheap scalar column test.
        connection.execute(
            text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_tags()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM entity_access_tags WHERE entity_id = NEW.id
    ) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_TAGS_ERROR}';
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
CREATE OR REPLACE TRIGGER entities_node_access_tags_check
BEFORE UPDATE OF node_id ON entities
FOR EACH ROW
WHEN (NEW.node_id IS NOT NULL)
EXECUTE FUNCTION entities_reject_node_access_tags();"""
            )
        )
        connection.execute(
            text(
                f"""
CREATE OR REPLACE FUNCTION entity_access_tags_reject_node_backed_entity()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_TAGS_ERROR}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            text(
                """
CREATE OR REPLACE TRIGGER entity_access_tags_reject_node_backed_entity
BEFORE INSERT OR UPDATE OF entity_id ON entity_access_tags
FOR EACH ROW EXECUTE FUNCTION entity_access_tags_reject_node_backed_entity();"""
            )
        )


namespaces = Table(
    "namespaces",
    metadata,
    Column("prefix", String, primary_key=True),
    Column("uri", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
