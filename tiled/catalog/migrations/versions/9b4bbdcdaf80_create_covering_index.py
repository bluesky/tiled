"""create-covering-index

Revision ID: 9b4bbdcdaf80
Revises: a963a6c32a0c
Create Date: 2025-08-20 08:54:39.273733

"""
import logging

from alembic import op

# revision identifiers, used by Alembic.
revision = "9b4bbdcdaf80"
down_revision = "a963a6c32a0c"
branch_labels = None
depends_on = None


logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        op.create_index(
            "ix_nodes_parent_time_id_key",
            "nodes",
            ["parent", "time_created", "id"],
            postgresql_include=["key"],
        )
        logger.info("Created covering index on nodes table.")


def downgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        op.drop_index(
            "ix_nodes_parent_time_id_key",
            table_name="nodes",
        )
        logger.info("Dropped covering index on nodes table.")
