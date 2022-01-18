"""Initialize

Revision ID: 481830dd6c11
Revises:
Create Date: 2022-01-13 11:26:35.432786

"""
import uuid

from alembic import op
from sqlalchemy import (
    Binary,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Unicode,
)
from sqlalchemy.sql import func

from tiled.server.orm import JSONList, PrincipalType

# revision identifiers, used by Alembic.
revision = "481830dd6c11"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "principals",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        # SQLite does not support UUID4 type, so we use generic binary.
        Column(
            "uuid",
            Binary(16),
            index=True,
            nullable=False,
            default=lambda: uuid.uuid4().bytes,
        ),
        Column("type", Enum(PrincipalType), nullable=False),
        Column("display_name", Unicode(255), nullable=False),
    )
    op.create_table(
        "identities",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("external_id", Unicode(255), primary_key=True, nullable=False),
        Column("provider", Unicode(255), primary_key=True, nullable=False),
        Column("principal_id", Integer, ForeignKey("principals.id")),
    )
    op.create_table(
        "roles",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column("name", Unicode(255), index=True, unique=True),
        Column("scopes", JSONList, nullable=False),
    )
    op.create_table(
        "principal_role_association",
        Column("principal_id", Integer, ForeignKey("principals.id"), primary_key=True),
        Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
    )
    op.create_table(
        "api_keys",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column(
            "hashed_api_key", Unicode(255), primary_key=True, index=True, nullable=False
        ),
        Column("expiration_time", DateTime(timezone=True), nullable=True),
        Column("note", Unicode(1023), nullable=True),
        Column("principal_id", Integer, ForeignKey("principals.id"), nullable=False),
        Column("scopes", JSONList, nullable=False),
    )
    op.create_table(
        "sessions",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        # SQLite does not support UUID4 type, so we use generic binary.
        Column(
            "uuid",
            Binary(16),
            index=True,
            nullable=False,
            default=lambda: uuid.uuid4().bytes,
        ),
        Column("expiration_time", DateTime(timezone=True), nullable=False),
        Column("principal_id", Integer, ForeignKey("principals.id"), nullable=False),
        Column("revoked", Boolean, default=False, nullable=False),
    )


def downgrade():
    "Nothing to do because this is the initial schema."
    pass
