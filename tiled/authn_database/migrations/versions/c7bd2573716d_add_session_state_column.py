"""add session state column

Revision ID: c7bd2573716d
Revises: 4a9dfaba4a98
Create Date: 2023-07-10 13:57:12.476131

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from tiled.authn_database.orm import Session, JSONVariant

# revision identifiers, used by Alembic.
revision = "c7bd2573716d"
down_revision = "4a9dfaba4a98"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        Session.__tablename__, sa.Column("state", JSONVariant, nullable=False)
    )


def downgrade():
    op.drop_column(Session.__tablename__, "state")
