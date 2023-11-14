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
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
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

    key = Column(Unicode(1023), nullable=False)
    ancestors = Column(JSONVariant, nullable=True)
    structure_family = Column(Enum(StructureFamily), nullable=False)
    metadata_ = Column("metadata", JSONVariant, nullable=False)
    specs = Column(JSONVariant, nullable=False)

    data_sources = relationship(
        "DataSource",
        backref="node",
        cascade="save-update",
        lazy="selectin",
        passive_deletes=True,
    )
    revisions = relationship(
        "Revision",
        backref="revisions",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint("key", "ancestors", name="key_ancestors_unique_constraint"),
        # This index supports comparison operations (==, <, ...).
        # For key-existence operations we will need a GIN index additionally.
        Index(
            "top_level_metadata",
            "ancestors",
            # include the keys of the default sorting ('time_created', 'id'),
            # used to avoid creating a temp sort index
            "time_created",
            "id",
            "metadata",
            postgresql_using="gin",
        ),
        # This is used by ORDER BY with the default sorting.
        # Index("ancestors_time_created", "ancestors", "time_created"),
    )


data_source_asset_association_table = Table(
    "data_source_asset_association",
    Base.metadata,
    Column(
        "data_source_id",
        Integer,
        ForeignKey("data_sources.id", ondelete="CASCADE"),
    ),
    Column(
        "asset_id",
        Integer,
        ForeignKey("assets.id", ondelete="CASCADE"),
    ),
)


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

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    node_id = Column(
        Integer, ForeignKey("nodes.id", ondelete="CASCADE"), nullable=False
    )

    structure = Column(JSONVariant, nullable=True)
    mimetype = Column(Unicode(255), nullable=False)  # max length given by RFC 4288
    # These are additional parameters passed to the Adapter to guide
    # it to access and arrange the data in the file correctly.
    parameters = Column(JSONVariant, nullable=True)
    # This relates to the mutability of the data.
    management = Column(Enum(Management), nullable=False)

    assets = relationship(
        "Asset",
        secondary=data_source_asset_association_table,
        back_populates="data_sources",
        cascade="all, delete",
        lazy="selectin",
    )


class Asset(Timestamped, Base):
    """
    This tracks individual files/blobs.
    """

    __tablename__ = "assets"
    __mapper_args__ = {"eager_defaults": True}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    data_uri = Column(Unicode(1023), index=True, unique=True)
    is_directory = Column(Boolean, nullable=False)
    hash_type = Column(Unicode(63), nullable=True)
    hash_content = Column(Unicode(1023), nullable=True)
    size = Column(Integer, nullable=True)

    data_sources = relationship(
        "DataSource",
        secondary=data_source_asset_association_table,
        back_populates="assets",
        passive_deletes=True,
    )


class Revision(Timestamped, Base):
    """
    This tracks history of metadata and specs, supporting 'undo' functionality.
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

    __table_args__ = (
        UniqueConstraint(
            "node_id",
            "revision_number",
            name="node_id_revision_number_unique_constraint",
        ),
    )
