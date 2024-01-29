"""Create index for fulltext search

Revision ID: 1cd99c02d0c7
Revises: a66028395cab
Create Date: 2024-01-24 15:53:12.348880

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "1cd99c02d0c7"
down_revision = "a66028395cab"
branch_labels = None
depends_on = None

# Make JSONB available in column
JSONVariant = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            # There is no sane way to perform this using op.create_index()
            op.execute(
                """
                CREATE INDEX metadata_tsvector_search
                ON nodes
                USING gin (jsonb_to_tsvector('simple', metadata, '["string"]'))
                """
            )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
