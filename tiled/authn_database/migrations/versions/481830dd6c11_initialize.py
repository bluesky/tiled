"""Initialize

Revision ID: 481830dd6c11
Revises:
Create Date: 2022-01-13 11:26:35.432786

"""
import uuid

from alembic import op
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
from sqlalchemy.sql import func

from tiled.authn_database.orm import UUID, JSONList, PrincipalType

# revision identifiers, used by Alembic.
revision = "481830dd6c11"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "principals",
        Column("time_created", DateTime(timezone=False), server_default=func.now()),
        Column("time_updated", DateTime(timezone=False), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column(
            "uuid",
            UUID,
            index=True,
            nullable=False,
            default=lambda: uuid.uuid4(),
        ),
        Column("type", Enum(PrincipalType), nullable=False),
    )
    op.create_table(
        "identities",
        Column("time_created", DateTime(timezone=False), server_default=func.now()),
        Column("time_updated", DateTime(timezone=False), onupdate=func.now()),
        Column("id", Unicode(255), primary_key=True, nullable=False),
        Column("provider", Unicode(255), primary_key=True, nullable=False),
        Column("principal_id", Integer, ForeignKey("principals.id")),
        Column("latest_login", DateTime(timezone=False), nullable=True),
    )
    op.create_table(
        "roles",
        Column("time_created", DateTime(timezone=False), server_default=func.now()),
        Column("time_updated", DateTime(timezone=False), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column("name", Unicode(255), index=True, unique=True),
        Column("description", Unicode(1023), nullable=True),
        Column("scopes", JSONList(511), nullable=False),
    )
    op.create_table(
        "principal_role_association",
        Column("principal_id", Integer, ForeignKey("principals.id"), primary_key=True),
        Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
    )
    op.create_table(
        "api_keys",
        Column("time_created", DateTime(timezone=False), server_default=func.now()),
        Column("time_updated", DateTime(timezone=False), onupdate=func.now()),
        Column(
            "hashed_secret",
            LargeBinary(32),
            primary_key=True,
            index=True,
            nullable=False,
        ),
        Column(
            "first_eight",
            Unicode(8),
            primary_eky=True,
            index=True,
            nullable=False,
        ),
        Column("latest_activity", DateTime(timezone=False), nullable=True),
        Column("expiration_time", DateTime(timezone=False), nullable=True),
        Column("note", Unicode(1023), nullable=True),
        Column("principal_id", Integer, ForeignKey("principals.id"), nullable=False),
        Column("scopes", JSONList(511), nullable=False),
    )
    op.create_table(
        "sessions",
        Column("time_created", DateTime(timezone=False), server_default=func.now()),
        Column("time_updated", DateTime(timezone=False), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column(
            "uuid",
            UUID,
            index=True,
            nullable=False,
            default=lambda: uuid.uuid4(),
        ),
        Column("time_last_refreshed", DateTime(timezone=False), nullable=True),
        Column("refresh_count", Integer, nullable=False, default=0),
        Column("expiration_time", DateTime(timezone=False), nullable=False),
        Column("principal_id", Integer, ForeignKey("principals.id"), nullable=False),
        Column("revoked", Boolean, default=False, nullable=False),
    )


def downgrade():
    "Nothing to do because this is the initial schema."
    pass
