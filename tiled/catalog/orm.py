import json

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    LargeBinary,
    Unicode,
)
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator

from ..structures.core import StructureFamily
from .base import Base


class Timestamped:
    """
    Mixin for providing timestamps of creation and update time.

    These are not used by application code, but they may be useful for
    forensics.
    """

    time_created = Column(DateTime(timezone=False), server_default=func.now())
    time_updated = Column(
        DateTime(timezone=False), onupdate=func.now()
    )  # null until first update

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


class JSONList(TypeDecorator):
    """Represents an immutable structure as a JSON-encoded list.

    Usage::

        JSONList(255)

    """

    impl = Unicode
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            # Make sure we don't get passed some iterable like a dict.
            if not isinstance(value, (list, tuple)):
                raise ValueError(
                    "JSONList must be given a literal `list` or `tuple` type."
                )
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class JSONDict(TypeDecorator):
    """Represents an immutable structure as a JSON-encoded dict (i.e. object).

    Usage::

        JSONDict(255)

    """

    impl = Unicode
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            # Make sure we don't get passed some iterable like a dict.
            if not isinstance(value, dict):
                raise ValueError("JSONDict must be given a literal `dict` type.")
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class Node(Timestamped, Base):
    """
    This describes a single Node and sometimes inlines descriptions of all its children.
    """

    __tablename__ = "nodes"

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    key = Column(Unicode(1023), index=True, nullable=False)
    ancestors = Column(JSONList(2**16 - 1), index=True, nullable=True)
    structure_family = Column(Enum(StructureFamily), nullable=False)
    metadata_ = Column("metadata", JSONDict, nullable=False)
    specs = Column(JSONList, nullable=False)
    references = Column(JSONList, nullable=False)

    data_sources = relationship("DataSource", back_populates="node")

    __table_args__ = (
        UniqueConstraint("key", "ancestors", name="_key_ancestors_unique_constraint"),
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

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    node_id = Column(Integer, ForeignKey("nodes.id"), nullable=False)

    structure = Column(JSONDict, nullable=True)
    mimetype = Column(Unicode(1023), nullable=False)
    # These are additional parameters passed to the Adapter to guide
    # it to access and arrange the data in the file correctly.
    parameters = Column(JSONDict(1023), nullable=True)
    externally_managed = Column(Boolean, default=False, nullable=False)

    node = relationship("Node", back_populates="data_sources")
    assets = relationship("Asset", back_populates="data_source")


class Asset(Timestamped, Base):
    """
    This tracks individual files/blobs.
    """

    __tablename__ = "assets"

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False)

    # data_uri can refer to an external file or network resource,
    # or to a row in the AssetBlob table "assetblob://"
    data_uri = Column(Unicode(1023))
    hash_type = Column(Unicode(63), nullable=True)
    hash_content = Column(Unicode(1023), nullable=True)

    data_source = relationship("DataSource", back_populates="assets")


class AssetBlob(Base):
    """
    This stores blob data in the table.

    This is can be optimal for small data payloads, where the overhead
    of opening a separate file or accessing a network resource is
    significant.
    """

    __tablename__ = "asset_blobs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    blob = Column(LargeBinary, nullable=False)


class AssetState(Base):
    """
    This tracks information used to check whether a filesystem asset has changed.
    """

    __tablename__ = "asset_state"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    mtime = Column(Integer, nullable=False)
    size = Column(Integer, nullable=False)
    inode = Column(Integer, nullable=False)
    mode = Column(Unicode(4), nullable=False)  # e.g. "0664"
    uid = Column(Integer, nullable=False)
    gid = Column(Integer, nullable=False)


class Revisions(Timestamped, Base):
    """
    This tracks history of metadata and specs, supporting 'undo' functionaltiy.
    """

    __tablename__ = "revisions"

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    key = Column(Unicode(1023), index=True, nullable=False)
    ancestors = Column(JSONList(2**16), index=True, nullable=True)
    revision = Column(Integer, index=True, nullable=False)

    metadata_ = Column("metadata", JSONDict, nullable=False)
    specs = Column(JSONList, nullable=False)

    time_updated = Column(
        DateTime(timezone=False), onupdate=func.now()
    )  # null until first update

    __table_args__ = (
        UniqueConstraint(
            "key",
            "ancestors",
            "revision",
            name="_key_ancestors_revision_unique_constraint",
        ),
    )
