"""Make time_created NOT NULL in principals and roles

Revision ID: 967cde08e8a1
Revises: 0a017b76d0d0
Create Date: 2026-10-28 10:00:00.000000

The ORM declares `time_created` NOT NULL on every timestamped table, but
migration d88e91ea03f9 left it nullable on `principals` and `roles` in
PostgreSQL. The column has had a server default since its creation, so no row
is expected to be NULL; any that is gets the time of this migration.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "967cde08e8a1"
down_revision = "0a017b76d0d0"
branch_labels = None
depends_on = None

# The server does not need this migration in order to work, so it may be
# applied after the new server version is deployed.
required_at_startup = False

tables = ["principals", "roles"]


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        # SQLite databases are created by create_all and already match the ORM.
        return
    for table in tables:
        connection.execute(
            sa.text(
                f"UPDATE {table} SET time_created = CURRENT_TIMESTAMP "
                "WHERE time_created IS NULL"
            )
        )
        op.alter_column(
            table,
            "time_created",
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
        )


def downgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        return
    for table in tables:
        op.alter_column(
            table,
            "time_created",
            existing_type=sa.DateTime(timezone=True),
            nullable=True,
        )
