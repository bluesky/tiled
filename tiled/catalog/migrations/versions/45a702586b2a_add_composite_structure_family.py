"""Add 'composite' structure family to the structurefamily enum.

Revision ID: 45a702586b2a
Revises: ed3a4223a600
Create Date: 2025-03-12 15:05:25.335010

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "45a702586b2a"
down_revision = "ed3a4223a600"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'composite' AFTER 'awkward'"
                )
            )


def downgrade():
    # PostgreSQL does not support dropping values from enums.
    # (The enum would need to be destroyed and recreated, migrating
    # the data in the process.) Fortunately, an extra unused values does
    # not interfere with operation of older versions of Tiled.
    pass
