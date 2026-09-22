"""Replace the combined top_level_metadata GIN with a focused metadata GIN

The former ``top_level_metadata`` index was a GIN over
``(parent, time_created, id, metadata, access_blob)``. Mixing b-tree columns
into a GIN (via the ``btree_gin`` extension) produced a large index that the
planner could not use effectively: GIN provides neither ordering nor
index-only scans, so it never helped ``ORDER BY id`` pagination, and the
scalar/JSON columns only bloated it (on production it grew to ~50 GB while
serving no query).

This replaces it with a focused ``GIN (metadata jsonb_path_ops)`` index that
supports the metadata containment queries the server actually emits
(``metadata @> {...}`` for equality filters). Pagination is already served by
the ``ix_nodes_parent_id`` (parent, id) b-tree and parent filtering by the
``ix_nodes_parent`` index, both unaffected here.

On SQLite there is no GIN and metadata search uses ``json_each``/FTS5, so no
replacement index is created there; the wide b-tree that ``top_level_metadata``
became on SQLite is simply dropped.

See https://github.com/bluesky/tiled/issues/1320

Revision ID: 3e87cbeb195b
Revises: 0d4e1f2a3b4c
Create Date: 2026-09-16 00:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3e87cbeb195b"
down_revision = "0d4e1f2a3b4c"
branch_labels = None
depends_on = None

# The combined GIN as it existed before this revision (created by
# e05e918092c3_add_closure_table). Used to restore prior state on downgrade.
_OLD_INDEX_COLUMNS = ["parent", "time_created", "id", "metadata", "access_blob"]


def upgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name
    if dialect_name == "postgresql":
        # CONCURRENTLY avoids an exclusive lock on a large, actively-written
        # nodes table, so it must run outside the migration's transaction.
        with op.get_context().autocommit_block():
            op.drop_index(
                "top_level_metadata",
                table_name="nodes",
                postgresql_concurrently=True,
                if_exists=True,
            )
            op.create_index(
                "ix_nodes_metadata",
                "nodes",
                ["metadata"],
                unique=False,
                postgresql_using="gin",
                postgresql_ops={"metadata": "jsonb_path_ops"},
                postgresql_concurrently=True,
                if_not_exists=True,
            )
    else:
        op.drop_index("top_level_metadata", table_name="nodes", if_exists=True)


def downgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name
    if dialect_name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
            op.drop_index(
                "ix_nodes_metadata",
                table_name="nodes",
                postgresql_concurrently=True,
                if_exists=True,
            )
            op.create_index(
                "top_level_metadata",
                "nodes",
                _OLD_INDEX_COLUMNS,
                unique=False,
                postgresql_using="gin",
                postgresql_concurrently=True,
                if_not_exists=True,
            )
    else:
        op.create_index(
            "top_level_metadata",
            "nodes",
            _OLD_INDEX_COLUMNS,
            unique=False,
        )
