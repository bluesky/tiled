import uuid as uuid_module

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Table,
    Unicode,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator

from ..structures.core import StructureFamily
from .base import Base

# Use JSON with SQLite and JSONB with PostgreSQL.
JSONVariant = JSON().with_variant(JSONB(), "postgresql")


class UUID(TypeDecorator):
    """Represents a UUID in a dialect-agnostic way

    Postgres has built-in support but SQLite does not, so we
    just use a 36-character Unicode column.

    We could use 16-byte LargeBinary, which would be more compact
    but we decided it was worth the cost to make the content easily
    inspectable by external database management and development tools.
    """

    impl = Unicode(36)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not isinstance(value, uuid_module.UUID):
                raise ValueError(f"Expected uuid.UUID, got {type(value)}")
            return str(value)

    def process_result_value(self, value, dialect):
        if value is not None:
            return uuid_module.UUID(hex=value)


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
    references = Column(JSONVariant, nullable=False)

    data_sources = relationship("DataSource", back_populates="node", lazy="selectin")

    __table_args__ = (
        UniqueConstraint("key", "ancestors", name="key_ancestors_unique_constraint"),
        # This index supports comparison operations (==, <, ...).
        # For key-existence operations we will need a GIN index additionally.
        Index(
            "top_level_metadata",
            "ancestors",
            "time_created",  # the default sorting, used to avoid creating a temp sort index
            "metadata",
            postgresql_using="btree",
        ),
        # This is used by ORDER BY with the default sorting.
        # Index("ancestors_time_created", "ancestors", "time_created"),
    )


data_source_asset_association_table = Table(
    "data_source_asset_association",
    Base.metadata,
    Column("data_source_id", Integer, ForeignKey("data_sources.id"), primary_key=True),
    Column("asset_id", Integer, ForeignKey("assets.id"), primary_key=True),
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

    # This id is internal, never exposed to the client.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    # This uuid is exposed to the client.
    uuid = Column(UUID, index=True, nullable=False, default=uuid_module.uuid4)
    node_id = Column(Integer, ForeignKey("nodes.id"), nullable=False)

    structure = Column(JSONVariant, nullable=True)
    mimetype = Column(Unicode(255), nullable=False)  # max length given by RFC 4288
    # These are additional parameters passed to the Adapter to guide
    # it to access and arrange the data in the file correctly.
    parameters = Column(JSONVariant, nullable=True)
    externally_managed = Column(Boolean, default=False, nullable=False)

    node = relationship("Node", back_populates="data_sources")
    assets = relationship(
        "Asset",
        secondary=data_source_asset_association_table,
        back_populates="data_sources",
        lazy="selectin",
    )


class Asset(Timestamped, Base):
    """
    This tracks individual files/blobs.
    """

    __tablename__ = "assets"
    __mapper_args__ = {"eager_defaults": True}

    # This id is internal, never exposed to the client.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False)

    # data_uri can refer to an external file or network resource,
    # or to a row in the AssetBlob table "assetblob://"
    data_uri = Column(Unicode(1023), index=True, unique=True)
    is_directory = Column(Boolean, nullable=False)
    hash_type = Column(Unicode(63), nullable=True)
    hash_content = Column(Unicode(1023), nullable=True)

    data_sources = relationship(
        "DataSource",
        secondary=data_source_asset_association_table,
        back_populates="assets",
    )


class AssetBlob(Base):
    """
    This stores blob data in the table.

    This is can be optimal for small data payloads, where the overhead
    of opening a separate file or accessing a network resource is
    significant.
    """

    __tablename__ = "asset_blobs"
    __mapper_args__ = {"eager_defaults": True}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    blob = Column(LargeBinary, nullable=False)


class Revisions(Timestamped, Base):
    """
    This tracks history of metadata and specs, supporting 'undo' functionaltiy.
    """

    __tablename__ = "revisions"
    __mapper_args__ = {"eager_defaults": True}

    # This id is internal, never exposed to the client.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    key = Column(Unicode(1023), nullable=False)
    ancestors = Column(JSONVariant, nullable=True)
    revision = Column(Integer, nullable=False)

    metadata_ = Column("metadata", JSONVariant, nullable=False)
    specs = Column(JSONVariant, nullable=False)

    time_updated = Column(
        DateTime(timezone=False), onupdate=func.now()
    )  # null until first update

    __table_args__ = (
        UniqueConstraint(
            "key",
            "ancestors",
            "revision",
            name="key_ancestors_revision_unique_constraint",
        ),
    )
