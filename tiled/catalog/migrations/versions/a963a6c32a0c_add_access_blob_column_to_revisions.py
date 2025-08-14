"""Add access_blob column to revisions

Revision ID: a963a6c32a0c
Revises: e05e918092c3
Create Date: 2025-08-12 16:22:07.744963

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = "a963a6c32a0c"
down_revision = "e05e918092c3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "revisions",
        sa.Column("access_blob", JSONVariant, nullable=False, server_default="{}"),
    )


def downgrade():
    op.drop_column("revisions", "access_blob")
