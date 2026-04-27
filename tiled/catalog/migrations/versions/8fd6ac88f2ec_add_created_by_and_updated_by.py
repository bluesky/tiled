"""Add created_by and updated_by

Revision ID: 8fd6ac88f2ec
Revises: dfbb7478c6bd
Create Date: 2026-03-11 14:36:52.488573

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import String
from sqlalchemy.sql import func

# revision identifiers, used by Alembic.
revision = "8fd6ac88f2ec"
down_revision = "4cf6011a8db5"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "nodes",
        sa.Column("created_by", String, nullable=True, server_default="NULL"),
    )
    op.add_column(
        "nodes",
        sa.Column("updated_by", String, nullable=True, server_default="NULL"),
    )
    op.add_column(
        "revisions",
        sa.Column("updated_by", String, nullable=True, server_default="NULL"),
    )
    op.drop_column("revisions", "time_created")


def downgrade():
    op.drop_column("nodes", "created_by")
    op.drop_column("nodes", "updated_by")
    op.drop_column("revisions", "updated_by")
    op.add_column(
        "revisions",
        sa.Column("time_created", String, nullable=False, server_default=func.now()),
    )
