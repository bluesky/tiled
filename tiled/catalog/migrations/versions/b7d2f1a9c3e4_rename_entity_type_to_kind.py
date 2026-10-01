"""Rename entities.entity_type to entities.kind

Revision ID: b7d2f1a9c3e4
Revises: 3e87cbeb195b
Create Date: 2026-10-01 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b7d2f1a9c3e4"
down_revision = "3e87cbeb195b"
branch_labels = None
depends_on = None


def upgrade():
    # RENAME COLUMN works on both PostgreSQL and SQLite (>= 3.25, 2018) and
    # updates references in indexes automatically.
    op.execute("ALTER TABLE entities RENAME COLUMN entity_type TO kind")
    # Rename the supporting index to match. There is no portable ALTER INDEX
    # RENAME (SQLite has none), so drop and recreate under the new name.
    op.drop_index("entities_type_created_idx", table_name="entities")
    op.create_index(
        "entities_kind_created_idx",
        "entities",
        ["kind", "created_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("entities_kind_created_idx", table_name="entities")
    op.execute("ALTER TABLE entities RENAME COLUMN kind TO entity_type")
    op.create_index(
        "entities_type_created_idx",
        "entities",
        ["entity_type", "created_at"],
        unique=False,
    )
