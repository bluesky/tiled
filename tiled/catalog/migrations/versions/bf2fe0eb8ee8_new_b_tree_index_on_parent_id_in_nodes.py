"""New B-tree index on (parent, id) in Nodes

Revision ID: bf2fe0eb8ee8
Revises: 85a47342e78e
Create Date: 2026-05-14 09:24:08.541769

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bf2fe0eb8ee8'
down_revision = '85a47342e78e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_nodes_parent_id",
        table_name="nodes",
        columns=["parent", "id"],
    )


def downgrade():
    op.drop_index("ix_nodes_parent_id", table_name="nodes")
