"""Add 'consolidated' to structure_family enum.

Revision ID: 0dc110294112
Revises: 7c8130c40b8f
Create Date: 2024-02-23 09:13:23.658921

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0dc110294112"
down_revision = "7c8130c40b8f"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'consolidated' AFTER 'table'"
                )
            )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
