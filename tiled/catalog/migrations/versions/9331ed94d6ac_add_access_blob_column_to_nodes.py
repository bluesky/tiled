"""Add access_blob column to nodes

Revision ID: 9331ed94d6ac
Revises: 45a702586b2a
Create Date: 2025-05-07 12:44:45.886279

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = "9331ed94d6ac"
down_revision = "45a702586b2a"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "nodes",
        sa.Column("access_blob", JSONVariant, nullable=False, server_default="{}"),
    )


def downgrade():
    op.drop_column("nodes", "access_blob")
