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

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Index, Integer, String, Table

from ..catalog.base import Base

metadata = Base.metadata

entities = Table(
    "entities",
    metadata,
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
