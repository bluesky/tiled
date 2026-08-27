"""Add unique constraint on entities (node_id, kind, name)

Revision ID: 7f2a1c9e4b3d
Revises: b7d2f1a9c3e4
Create Date: 2026-08-27 00:00:01.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "7f2a1c9e4b3d"
down_revision = "b7d2f1a9c3e4"
branch_labels = None
depends_on = None


def upgrade():
    # Enforce at most one entity per (node, kind, name). Implemented as a
    # unique index (rather than a table-level UNIQUE constraint) so it applies
    # cleanly on SQLite without a table rebuild. node_id is nullable, and both
    # SQLite and PostgreSQL treat NULLs as distinct in a unique index, so
    # free-standing (external) entities -- which have node_id NULL -- are left
    # unconstrained; only node-bound entities are deduplicated.
    op.create_index(
        "entities_node_kind_name_uq",
        "entities",
        ["node_id", "kind", "name"],
        unique=True,
    )


def downgrade():
    op.drop_index("entities_node_kind_name_uq", table_name="entities")
