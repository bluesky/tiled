"""Restore server_default for time_created

Revision ID: 0c705a02954c
Revises: d88e91ea03f9
Create Date: 2025-02-19 13:03:35.067755

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0c705a02954c"
down_revision = "d88e91ea03f9"
branch_labels = None
depends_on = None

tables = ["principals", "identities", "roles", "api_keys", "sessions"]


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        return
    for table in tables:
        connection.execute(
            sa.text(
                f"""
ALTER TABLE {table}
ALTER COLUMN time_created
SET DEFAULT CURRENT_TIMESTAMP;
            """
            )
        )


def downgrade():
    # No action required
    pass
