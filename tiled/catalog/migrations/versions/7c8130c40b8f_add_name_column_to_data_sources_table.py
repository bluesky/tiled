"""Add 'name' column to data_sources table.

Revision ID: 7c8130c40b8f
Revises: e756b9381c14
Create Date: 2024-02-23 08:53:24.008576

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7c8130c40b8f"
down_revision = "ed3a4223a600"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("data_sources", sa.Column("name", sa.Unicode(1023), nullable=True))


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
