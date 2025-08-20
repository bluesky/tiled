"""covering-index

Revision ID: 9b4bbdcdaf80
Revises: a963a6c32a0c
Create Date: 2025-08-20 08:54:39.273733

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "9b4bbdcdaf80"
down_revision = "a963a6c32a0c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_nodes_parent_time_id_key",
        "nodes",
        ["parent", "time_created", "id"],
        postgresql_include=["key"],
    )


def downgrade():
    # Drop the index concurrently
    op.drop_index(
        "ix_nodes_parent_time_id_key",
        table_name="nodes",
    )
