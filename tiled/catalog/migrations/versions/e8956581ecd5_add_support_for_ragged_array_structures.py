"""Add support for ragged array structures

Revision ID: e8956581ecd5
Revises: 85a47342e78e
Create Date: 2026-04-28 11:24:54.806141

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e8956581ecd5"
down_revision = "85a47342e78e"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'ragged' AFTER 'container'"
                )
            )


def downgrade():
    # PostgreSQL does not support dropping values from enums.
    # (The enum would need to be destroyed and recreated, migrating
    # the data in the process.) Fortunately, an extra unused value does
    # not interfere with operation of older versions of Tiled.
    raise NotImplementedError
