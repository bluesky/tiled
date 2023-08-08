"""Add 'awkward' to structurefamily enum.

Revision ID: 0b033e7fbe30
Revises: 83889e049ddc
Create Date: 2023-08-08 21:10:20.181470

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0b033e7fbe30"
down_revision = "83889e049ddc"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'awkward' AFTER 'array'"
                )
            )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
