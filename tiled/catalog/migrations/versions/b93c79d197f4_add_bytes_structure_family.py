"""Add bytes structure family

Revision ID: b93c79d197f4
Revises: e8956581ecd5
Create Date: 2026-06-12 14:08:28.462809

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b93c79d197f4"
down_revision = "e8956581ecd5"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'bytes' AFTER 'awkward'"
                )
            )


def downgrade():
    # PostgreSQL does not support dropping values from enums.
    # (The enum would need to be destroyed and recreated, migrating
    # the data in the process.) Fortunately, an extra unused value does
    # not interfere with operation of older versions of Tiled.
    raise NotImplementedError
