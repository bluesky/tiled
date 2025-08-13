from typing import List

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Table,
    Unicode,
    event,
    schema,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func

from ..server.schemas import Management
from ..structures.core import StructureFamily
from .base import Base

# Use JSON with SQLite and JSONB with PostgreSQL.
JSONVariant = JSON().with_variant(JSONB(), "postgresql")


class Timestamped:
    """
    Mixin for providing timestamps of creation and update time.

    These are not used by application code, but they may be useful for
    forensics.
    """

    time_created = Column(DateTime(timezone=False), server_default=func.now())
    time_updated = Column(
        DateTime(timezone=False), onupdate=func.now(), server_default=func.now()
    )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            + ", ".join(
                f"{key}={value!r}"
                for key, value in self.__dict__.items()
                if not key.startswith("_")
            )
            + ")"
        )


class Node(Timestamped, Base):
    """
    This describes a single Node and sometimes inlines descriptions of all its children.
    """

    __tablename__ = "nodes"
    __mapper_args__ = {"eager_defaults": True}

    # This id is internal, never exposed to the client.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    parent = Column(
        Integer,
        ForeignKey("nodes.id", name="fk_nodes_parent", ondelete="CASCADE"),
        nullable=True,
        index=True,  # An index to support parent lookups
    )

    key = Column(Unicode(1023), nullable=False)
    structure_family = Column(Enum(StructureFamily), nullable=False)
    metadata_ = Column("metadata", JSONVariant, nullable=False)
    specs = Column(JSONVariant, nullable=False)
    access_blob = Column("access_blob", JSONVariant, nullable=False)

    data_sources = relationship(
        "DataSource",
        backref="node",
        cascade="save-update",
        lazy="selectin",
        passive_deletes=True,
    )
    revisions = relationship(
        "Revision",
        backref="node",
        passive_deletes=True,
    )

    # This is a self-referencing relationship between parent and children
    prnt_rel = relationship("Node", remote_side=[id], back_populates="chld_rel")
    chld_rel = relationship("Node", back_populates="prnt_rel")

    __table_args__ = (
        UniqueConstraint("key", "parent", name="key_parent_unique_constraint"),
        # This index supports comparison operations (==, <, ...).
        # For key-existence operations we will need a GIN index additionally.
        Index(
            "top_level_metadata",
            "parent",
            # include the keys of the default sorting ('time_created', 'id'),
            # used to avoid creating a temp sort index
            "time_created",
            "id",
            "metadata",
            "access_blob",
            postgresql_using="gin",
        ),
    )


class NodesClosure(Base):
    """This describes the closure table for Node."""

    __tablename__ = "nodes_closure"

    ancestor = Column(
        Integer, ForeignKey("nodes.id", ondelete="CASCADE"), primary_key=True
    )
    descendant = Column(
        Integer, ForeignKey("nodes.id", ondelete="CASCADE"), primary_key=True
    )
    depth = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "ancestor", "descendant", name="ancestor_descendant_unique_constraint"
        ),
        Index("idx_nodes_closure_ancestor", "ancestor"),
        Index("idx_nodes_closure_descendant", "descendant"),
    )


class DataSourceAssetAssociation(Base):
    """
    This describes which Assets are used by which DataSources, and how.

    The 'parameter' describes which argument to pass the asset into when
    constructing the Adapter. If 'parameter' is NULL then the asset is an
    indirect dependency, such as a HDF5 data file backing an HDF5 'master'
    file.

    If 'num' is NULL, the asset is passed as a scalar value, and there must be
    only one for the given 'parameter'. If 'num' is not NULL, all the assets
    sharing the same 'parameter' (there may be one or more) will be passed in
    as a list, ordered in ascending order of 'num'.
    """

    __tablename__ = "data_source_asset_association"

    data_source_id: Mapped[int] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    asset_id: Mapped[int] = mapped_column(
        ForeignKey("assets.id", ondelete="CASCADE"),
        primary_key=True,
    )
    parameter = Column(Unicode(255), nullable=True)
    num = Column(Integer, nullable=True)

    data_source: Mapped["DataSource"] = relationship(
        back_populates="asset_associations"
    )
    asset: Mapped["Asset"] = relationship(
        back_populates="data_source_associations", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint(
            "data_source_id",
            "parameter",
            "num",
            name="parameter_num_unique_constraint",
        ),
        # Below, in unique_parameter_num_null_check, additional constraints
        # are applied, via triggers.
    )


@event.listens_for(DataSourceAssetAssociation.__table__, "after_create")
def unique_parameter_num_null_check(target, connection, **kw):
    # This creates a pair of triggers on the data_source_asset_association
    # table. (There are a total of four defined below, two for the SQLite
    # branch and two for the PostgreSQL branch.) Each pair include one trigger
    # that runs when NEW.num IS NULL and one trigger than runs when
    # NEW.num IS NOT NULL. Thus, for a given insert, only one of these
    # triggers is run.
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            text(
                """
CREATE TRIGGER cannot_insert_num_null_if_num_exists
BEFORE INSERT ON data_source_asset_association
WHEN NEW.num IS NULL
BEGIN
    SELECT RAISE(ABORT, 'Can only insert num=NULL if no other row exists for the same parameter')
    WHERE EXISTS
    (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND data_source_id = NEW.data_source_id
    );
END"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER cannot_insert_num_int_if_num_null_exists
BEFORE INSERT ON data_source_asset_association
WHEN NEW.num IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Can only insert INTEGER num if no NULL row exists for the same parameter')
    WHERE EXISTS
    (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND num IS NULL
        AND data_source_id = NEW.data_source_id
    );
END"""
            )
        )
    elif connection.engine.dialect.name == "postgresql":
        connection.execute(
            text(
                """
CREATE OR REPLACE FUNCTION raise_if_parameter_exists()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND data_source_id = NEW.data_source_id
    ) THEN
        RAISE EXCEPTION 'Can only insert num=NULL if no other row exists for the same parameter';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER cannot_insert_num_null_if_num_exists
BEFORE INSERT ON data_source_asset_association
FOR EACH ROW
WHEN (NEW.num IS NULL)
EXECUTE FUNCTION raise_if_parameter_exists();"""
            )
        )
        connection.execute(
            text(
                """
CREATE OR REPLACE FUNCTION raise_if_null_parameter_exists()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND data_source_id = NEW.data_source_id
        AND num IS NULL
    ) THEN
        RAISE EXCEPTION 'Can only insert INTEGER num if no NULL row exists for the same parameter';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            text(
                """
CREATE TRIGGER cannot_insert_num_int_if_num_null_exists
BEFORE INSERT ON data_source_asset_association
FOR EACH ROW
WHEN (NEW.num IS NOT NULL)
EXECUTE FUNCTION raise_if_null_parameter_exists();"""
            )
        )


@event.listens_for(Node.__table__, "after_create")
def create_index_metadata_tsvector_search(target, connection, **kw):
    # This creates a ts_vector based metadata search index for fulltext.
    # Postgres only feature
    if connection.engine.dialect.name == "postgresql":
        connection.execute(
            text(
                """
                CREATE INDEX metadata_tsvector_search
                ON nodes
                USING gin (jsonb_to_tsvector('simple', metadata, '["string"]'))
                """
            )
        )


@event.listens_for(NodesClosure.__table__, "after_create")
def update_closure_table(target, connection, **kw):
    if connection.engine.dialect.name == "sqlite":
        # Create a trigger to update the closure table when INSERTING a new node
        connection.execute(
            text(
                """
CREATE TRIGGER update_closure_table_when_inserting
AFTER INSERT ON nodes
BEGIN
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT NEW.id, NEW.id, 0;
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT p.ancestor, c.descendant, p.depth+c.depth+1
    FROM nodes_closure p, nodes_closure c
    WHERE p.descendant=NEW.parent and c.ancestor=NEW.id;
END"""
            )
        )

    elif connection.engine.dialect.name == "postgresql":
        # Create function and trigger to update the closure table when INSERTING a new node
        connection.execute(
            text(
                """
CREATE OR REPLACE FUNCTION update_closure_table_when_inserting()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    VALUES (NEW.id, NEW.id, 0);
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT p.ancestor, c.descendant, p.depth + c.depth + 1
    FROM nodes_closure p, nodes_closure c
    WHERE p.descendant = NEW.parent AND c.ancestor = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
            )
        )

        connection.execute(
            text(
                """
CREATE TRIGGER update_closure_table_when_inserting
AFTER INSERT ON nodes
FOR EACH ROW
EXECUTE FUNCTION update_closure_table_when_inserting();
"""
            )
        )

    # Create the root node (in nodes and, automatically, in nodes_closure) when initializing the database.
    connection.execute(
        text(
            """
INSERT INTO nodes(id, key, parent, structure_family, metadata, specs, access_blob)
SELECT 0, '', NULL, 'container', '{}', '[]', '{}';
"""
        )
    )


class FTS5Table(Table):
    pass


@compiles(schema.CreateTable, "sqlite")
def _compile_fts5_virtual_table_sqlite(element: schema.CreateTable, compiler, **kw):
    if not isinstance(element.target, FTS5Table):
        return compiler.visit_create_table(element, **kw)
    name = compiler.preparer.format_table(element.target)
    cols = ", ".join(
        # Skip last column (rowid).
        compiler.preparer.format_column(col)
        for col in element.target.columns[1:]
    )
    return f"CREATE VIRTUAL TABLE {name} USING fts5({cols}, content='nodes', content_rowid='id')"


@compiles(schema.CreateTable, "postgresql")
def _compile_no_op_fts5_postgresql(element: schema.CreateTable, compiler, **kw):
    # Preclude the creation of the FTS5 virtual table in posgres instances,
    # Where fulltext search is handled by a different indexing mechanism.
    if not isinstance(element.target, FTS5Table):
        return compiler.visit_create_table(element, **kw)
    return "SELECT 1"


metadata_fts5 = FTS5Table(
    "metadata_fts5", Base.metadata, Column("rowid", Integer), Column("metadata", JSON)
)


@event.listens_for(metadata_fts5, "after_create")
def create_virtual_table_fits5(target, connection, **kw):
    if connection.engine.dialect.name == "sqlite":
        statements = [
            # See https://www.sqlite.org/fts5.html Section 4.4.3.
            # """
            # CREATE VIRTUAL TABLE metadata_fts5 USING fts5(metadata, content='nodes', content_rowid='id');
            # """,
            # Triggers keep the index synchronized with the nodes table.
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_ai AFTER INSERT ON nodes BEGIN
              INSERT INTO metadata_fts5(rowid, metadata)
              VALUES (new.id, new.metadata);
            END;
            """,
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_ad AFTER DELETE ON nodes BEGIN
              INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata)
              VALUES('delete', old.id, old.metadata);
            END;
            """,
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_au AFTER UPDATE ON nodes BEGIN
              INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata)
              VALUES('delete', old.id, old.metadata);
              INSERT INTO metadata_fts5(rowid, metadata)
              VALUES (new.id, new.metadata);
            END;
            """,
        ]
        for statement in statements:
            connection.execute(text(statement))


class DataSource(Timestamped, Base):
    """
    The describes how to open one or more file/blobs to extract data for a Node.

    The mimetype can be used to look up an appropriate Adapter.
    The Adapter will accept the data_uri (which may be a directory in this case)
    and optional parameters.

    The parameters are used to select the data of interest for this DataSource.
    Then, within that, Tiled may use the standard Adapter API to subselect the data
    of interest for a given request.
    """

    __tablename__ = "data_sources"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    node_id = Column(
        Integer, ForeignKey("nodes.id", ondelete="CASCADE"), nullable=False, index=True
    )
    structure_id = Column(
        Unicode(32), ForeignKey("structures.id", ondelete="CASCADE"), nullable=True
    )
    mimetype = Column(Unicode(255), nullable=False)  # max length given by RFC 4288
    # These are additional parameters passed to the Adapter to guide
    # it to access and arrange the data in the file correctly.
    parameters = Column(JSONVariant, nullable=True)
    # This relates to the mutability of the data.
    management = Column(Enum(Management), nullable=False)
    structure_family = Column(Enum(StructureFamily), nullable=False)

    # many-to-one relationship to Structure
    structure: Mapped["Structure"] = relationship(
        "Structure",
        lazy="selectin",
        passive_deletes=True,
    )

    # many-to-many relationship to Asset, bypassing the `Association` class
    assets: Mapped[List["Asset"]] = relationship(
        secondary="data_source_asset_association",
        back_populates="data_sources",
        cascade="all, delete",
        lazy="selectin",
        viewonly=True,
    )
    # association between Asset -> Association -> DataSource
    asset_associations: Mapped[List["DataSourceAssetAssociation"]] = relationship(
        back_populates="data_source",
        lazy="selectin",
        order_by=[DataSourceAssetAssociation.parameter, DataSourceAssetAssociation.num],
    )


class Structure(Base):
    """
    The describes the structure of a DataSource.

    The id is the HEX digest of the MD5 hash of the canonical representation
    of the JSON structure, as specified by RFC 8785.

    https://datatracker.ietf.org/doc/html/rfc8785

    """

    __tablename__ = "structures"

    id: str = Column(Unicode(32), primary_key=True, unique=True)
    structure = Column(JSONVariant, nullable=False)


class Asset(Timestamped, Base):
    """
    This tracks individual files/blobs.
    """

    __tablename__ = "assets"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)

    data_uri = Column(Unicode(1023), index=True, unique=True)
    is_directory = Column(Boolean, nullable=False)
    hash_type = Column(Unicode(63), nullable=True)
    hash_content = Column(Unicode(1023), nullable=True)
    size = Column(Integer, nullable=True)

    # # many-to-many relationship to Asset, bypassing the `Association` class
    data_sources: Mapped[List["DataSource"]] = relationship(
        secondary="data_source_asset_association",
        back_populates="assets",
        viewonly=True,
    )
    # association between DataSource -> Association -> Asset
    data_source_associations: Mapped[List["DataSourceAssetAssociation"]] = relationship(
        back_populates="asset",
    )


class Revision(Timestamped, Base):
    """
    This tracks history of metadata, specs, and access_blob supporting 'undo' functionality.
    """

    __tablename__ = "revisions"
    __mapper_args__ = {"eager_defaults": True}

    # This id is internal, never exposed to the client.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    node_id = Column(
        Integer, ForeignKey("nodes.id", ondelete="CASCADE"), nullable=False
    )
    revision_number = Column(Integer, nullable=False)

    metadata_ = Column("metadata", JSONVariant, nullable=False)
    specs = Column(JSONVariant, nullable=False)
    access_blob = Column("access_blob", JSONVariant, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "node_id",
            "revision_number",
            name="node_id_revision_number_unique_constraint",
        ),
    )
