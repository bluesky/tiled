"""Widen assets.size to BigInteger

Revision ID: 9bc9b57294b9
Revises: b93c79d197f4
Create Date: 2026-07-09 14:32:49.251181

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9bc9b57294b9"
down_revision = "b93c79d197f4"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "postgresql":
        op.alter_column(
            "assets",
            "size",
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )
    # SQLite: INTEGER affinity already stores 64-bit values, no schema change needed.


def downgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "postgresql":
        op.alter_column(
            "assets",
            "size",
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=True,
        )
