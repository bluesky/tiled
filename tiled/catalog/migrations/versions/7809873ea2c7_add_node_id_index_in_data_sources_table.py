"""Add node_id index in data_sources table

Revision ID: 7809873ea2c7
Revises: 9331ed94d6ac
Create Date: 2025-08-01 11:26:54.658601

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "7809873ea2c7"
down_revision = "9331ed94d6ac"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "idx_data_sources_node_id", table_name="data_sources", columns=["node_id"]
    )


def downgrade():
    op.drop_index("idx_data_sources_node_id", table_name="data_sources")
