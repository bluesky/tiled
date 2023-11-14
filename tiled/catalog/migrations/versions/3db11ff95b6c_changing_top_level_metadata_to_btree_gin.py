"""Changing top_level_metadata to btree_gin

Revision ID: 3db11ff95b6c
Revises: 0b033e7fbe30
Create Date: 2023-11-01 15:16:48.554420

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3db11ff95b6c"
down_revision = "0b033e7fbe30"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(sa.text("create extension IF NOT EXISTS btree_gin;"))
            op.drop_index("top_level_metadata", table_name="nodes")
            op.create_index(
                "top_level_metadata",
                "nodes",
                ["ancestors", "time_created", "id", "metadata"],
                postgresql_using="gin",
            )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
