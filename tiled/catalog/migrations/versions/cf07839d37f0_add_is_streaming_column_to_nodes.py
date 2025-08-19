"""add is_streaming column to nodes

Revision ID: cf07839d37f0
Revises: a963a6c32a0c
Create Date: 2025-08-19 13:38:53.741491

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cf07839d37f0"
down_revision = "a963a6c32a0c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "nodes",
        sa.Column(
            "is_streaming", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )


def downgrade():
    op.drop_column("nodes", "is_streaming")
