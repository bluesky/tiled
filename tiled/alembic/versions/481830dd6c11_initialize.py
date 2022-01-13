"""Initialize

Revision ID: 481830dd6c11
Revises:
Create Date: 2022-01-13 11:26:35.432786

"""
from alembic import op
from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, Unicode
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
        Column("type", Enum(PrincipalType)),
        Column("display_name", Unicode(255)),
    )
    op.create_table(
        "identities",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("external_id", Unicode(255), primary_key=True),
        Column("provider", Unicode(255), primary_key=True),
        Column("provider_id", Integer, ForeignKey("principals.id")),
    )
    op.create_table(
        "roles",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column("scopes", JSONList(255)),
    )

    op.create_table(
        "api_keys",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("hashed_api_key", Unicode(255), primary_key=True, index=True),
        Column("expiration_time", DateTime(timezone=True)),
        Column("principal_id", Integer, ForeignKey("principals.id")),
        Column("scopes", JSONList(255)),
    )
    op.create_table(
        "revoked_sessions",
        Column("time_created", DateTime(timezone=True), server_default=func.now()),
        Column("time_updated", DateTime(timezone=True), onupdate=func.now()),
        Column("session_id", Unicode(255), primary_key=True, index=True),
        Column("expiration_time", DateTime(timezone=True)),
    )


def downgrade():
    "Nothing to do because this is the initial schema."
    pass
