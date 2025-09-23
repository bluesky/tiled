"""Add access_tags to API keys

Revision ID: a806cc635ab2
Revises: 0c705a02954c
Create Date: 2025-08-26 17:10:47.717942

"""
import sqlalchemy as sa
from alembic import op

from tiled.authn_database.orm import APIKey, JSONList

# revision identifiers, used by Alembic.
revision = "a806cc635ab2"
down_revision = "0c705a02954c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        APIKey.__tablename__,
        sa.Column("access_tags", JSONList(511), nullable=True),
    )


def downgrade():
    op.drop_column(
        APIKey.__tablename__,
        "access_tags",
    )
