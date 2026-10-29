import json
import uuid as uuid_module
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    LargeBinary,
    Table,
    Unicode,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator

from ..server.schemas import PrincipalType
from .base import Base

# Use JSON with SQLite and JSONB with PostgreSQL.
JSONVariant = JSON().with_variant(JSONB(), "postgresql")


class JSONList(TypeDecorator):
    """Represents an immutable structure as a JSON-encoded list.

    Usage::

        JSONList(255)

    """

    impl = Unicode
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            # Allow None for columns that are nullable
            return None
        else:
            # Make sure we don't get passed some iterable like a dict.
            if not isinstance(value, list):
                raise ValueError("JSONList must be given a literal `list` type.")
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


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

    __mapper_args__ = {"eager_defaults": True}

    time_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    time_updated: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        nullable=True,
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


principal_role_association_table = Table(
    "principal_role_association",
    Base.metadata,
    Column("principal_id", Integer, ForeignKey("principals.id"), primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
)


class Principal(Timestamped, Base):
    __tablename__ = "principals"

    # This id is internal, never exposed to the client.
    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, index=True, autoincrement=True
    )
    # This uuid is public.
    uuid: Mapped[uuid_module.UUID] = mapped_column(
        UUID,
        index=True,
        nullable=False,
        default=lambda: uuid_module.uuid4(),
    )
    type: Mapped[PrincipalType] = mapped_column(Enum(PrincipalType), nullable=False)
    # In the future we may add other information.

    identities: Mapped[list["Identity"]] = relationship(back_populates="principal")
    api_keys: Mapped[list["APIKey"]] = relationship(back_populates="principal")
    roles: Mapped[list["Role"]] = relationship(
        secondary=principal_role_association_table,
        back_populates="principals",
        lazy="joined",
    )
    sessions: Mapped[list["Session"]] = relationship(
        "Session", back_populates="principal"
    )


class Identity(Timestamped, Base):
    __tablename__ = "identities"

    # An (id, provider) pair must be unique.
    id: Mapped[str] = mapped_column(Unicode(255), primary_key=True, nullable=False)
    provider: Mapped[str] = mapped_column(
        Unicode(255), primary_key=True, nullable=False
    )
    principal_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("principals.id"), nullable=False
    )
    latest_login: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # In the future we may add a notion of "primary" identity.

    principal: Mapped[Principal] = relationship(back_populates="identities")


class Role(Timestamped, Base):
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, index=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(
        Unicode(255), index=True, unique=True, nullable=False
    )
    description: Mapped[str | None] = mapped_column(Unicode(1023), nullable=True)
    scopes: Mapped[list[str]] = mapped_column(JSONList(511), nullable=False)
    principals: Mapped[list[Principal]] = relationship(
        secondary=principal_role_association_table, back_populates="roles"
    )


class APIKey(Timestamped, Base):
    __tablename__ = "api_keys"

    # Store the first_eight characters of the hex-encoded secret.
    # The key holder can use this to identity the key.
    # We do not store the full secret, only its sha256-hashed value.
    # A primary key on (first_eight, hashed_secret) enables
    # fast lookups.
    first_eight: Mapped[str] = mapped_column(
        Unicode(8), primary_key=True, index=True, nullable=False
    )
    hashed_secret: Mapped[bytes] = mapped_column(
        LargeBinary(32), primary_key=True, index=True, nullable=False
    )
    expiration_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    latest_activity: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    note: Mapped[str | None] = mapped_column(Unicode(1023), nullable=True)
    principal_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("principals.id"), nullable=False
    )
    scopes: Mapped[list[str]] = mapped_column(JSONList(511), nullable=False)
    access_tags: Mapped[list[str] | None] = mapped_column(JSONList(511), nullable=True)
    # In the future we could make it possible to disable API keys
    # without deleting them from the database, for forensics and
    # record-keeping.

    principal: Mapped[Principal] = relationship(
        back_populates="api_keys", lazy="joined"
    )


class Session(Timestamped, Base):
    """
    This related to refresh tokens, which have a session uuid ("sid") claim.

    When the client attempts to use a refresh token, we first check
    here to ensure that the "session", which is associated with a chain
    of refresh tokens that came from a single authentication, are still valid.
    """

    __tablename__ = "sessions"

    # This id is internal, never exposed to the client.
    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, index=True, autoincrement=True
    )
    # This uuid is exposed to the client.
    uuid: Mapped[uuid_module.UUID] = mapped_column(
        UUID, index=True, nullable=False, default=uuid_module.uuid4
    )
    time_last_refreshed: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    refresh_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expiration_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    principal_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("principals.id"), nullable=False
    )
    revoked: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    # State allows for custom  authenticator information to be stored in the session.
    state: Mapped[Any] = mapped_column(JSONVariant, nullable=False)
    principal: Mapped[Principal] = relationship(
        back_populates="sessions", lazy="joined"
    )


class PendingSession(Base):
    """
    This is used only in Device Code Flow.
    """

    __tablename__ = "pending_sessions"

    hashed_device_code: Mapped[bytes] = mapped_column(
        LargeBinary(32), primary_key=True, index=True, nullable=False
    )
    user_code: Mapped[str] = mapped_column(Unicode(8), index=True, nullable=False)
    expiration_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    # SET NULL so that deleting a Session (e.g. purging an expired one) is
    # never blocked by a pending session that still references it.
    session_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("sessions.id", ondelete="SET NULL"), nullable=True
    )
    session: Mapped[Session] = relationship(lazy="joined")
