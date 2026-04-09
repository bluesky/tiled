"""Add 'properties' column to the DataSources table

Revision ID: 4cf6011a8db5
Revises: a86a48befff6
Create Date: 2026-01-09 13:59:48.448079

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = "4cf6011a8db5"
down_revision = "a86a48befff6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "data_sources",
        sa.Column("properties", JSONVariant, nullable=False, server_default="{}"),
    )


def downgrade():
    op.drop_column("data_sources", "properties")
