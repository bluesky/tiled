"""Add index on (parent, key) for key prefix lookup

Revision ID: 8f2400f6ac20
Revises: bfbcd5d137d0
Create Date: 2026-11-24 22:00:00.000000

See https://github.com/bluesky/tiled/issues/1676
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8f2400f6ac20"
down_revision = "bfbcd5d137d0"
branch_labels = None
depends_on = None

# The server does not need this index in order to work, so this migration may
# be applied after the new server version is deployed.
required_at_startup = False

INDEX = "ix_nodes_parent_key_pattern"


def upgrade():
    # PostgreSQL only: SQLite evaluates key_like with substr/instr, which no
    # index serves.
    connection = op.get_bind()
    if connection.dialect.name == "postgresql":
        # Building the index blocks writes to `nodes`. On a large database,
        # create it beforehand with CREATE INDEX CONCURRENTLY (see the
        # changelog); this is then a no-op. A concurrent build that failed or
        # was cancelled leaves an invalid index, which IF NOT EXISTS would
        # accept and no query can use.
        valid = connection.execute(
            sa.text(
                f"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('{INDEX}')"
            )
        ).scalar()
        if valid is False:
            raise RuntimeError(
                f"The index {INDEX} exists but is not valid, as a failed or "
                "unfinished CREATE INDEX CONCURRENTLY leaves it. Once no build "
                f"is running, drop it (DROP INDEX CONCURRENTLY {INDEX}), then "
                "build it again or rerun this upgrade."
            )
        op.create_index(
            INDEX,
            "nodes",
            ["parent", "key"],
            postgresql_ops={"key": "varchar_pattern_ops"},
            if_not_exists=True,
        )


def downgrade():
    if op.get_bind().dialect.name == "postgresql":
        op.drop_index(INDEX, table_name="nodes", if_exists=True)
