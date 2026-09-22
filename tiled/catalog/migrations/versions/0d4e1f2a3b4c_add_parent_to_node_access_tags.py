"""Add parent id to node access-tag assignments.

Revision ID: 0d4e1f2a3b4c
Revises: 3bc110ce44e9
Create Date: 2026-09-19

The parent id is denormalized from ``nodes.parent`` so authorization queries
can restrict the node/tag association scan to one container. Triggers maintain
the value for relationship-generated inserts and node moves.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0d4e1f2a3b4c"
down_revision = "3bc110ce44e9"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_node_access_tags_association_parent_id_tag_id_node_id"


def _create_triggers(connection):
    if connection.dialect.name == "sqlite":
        op.execute(
            """
CREATE TRIGGER node_access_tags_set_parent_after_insert
AFTER INSERT ON node_access_tags_association
BEGIN
    UPDATE node_access_tags_association
    SET parent_id = (SELECT parent FROM nodes WHERE id = NEW.node_id)
    WHERE node_id = NEW.node_id AND tag_id = NEW.tag_id;
END
"""
        )
        op.execute(
            """
CREATE TRIGGER node_access_tags_set_parent_after_update
AFTER UPDATE OF node_id ON node_access_tags_association
BEGIN
    UPDATE node_access_tags_association
    SET parent_id = (SELECT parent FROM nodes WHERE id = NEW.node_id)
    WHERE node_id = NEW.node_id AND tag_id = NEW.tag_id;
END
"""
        )
        op.execute(
            """
CREATE TRIGGER node_access_tags_sync_parent_after_node_update
AFTER UPDATE OF parent ON nodes
WHEN NEW.parent IS NOT OLD.parent
BEGIN
    UPDATE node_access_tags_association
    SET parent_id = NEW.parent
    WHERE node_id = NEW.id;
END
"""
        )
    elif connection.dialect.name == "postgresql":
        op.execute(
            """
CREATE FUNCTION node_access_tags_set_parent()
RETURNS TRIGGER AS $$
BEGIN
    SELECT parent INTO NEW.parent_id FROM nodes WHERE id = NEW.node_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""
        )
        op.execute(
            """
CREATE TRIGGER node_access_tags_set_parent
BEFORE INSERT OR UPDATE OF node_id ON node_access_tags_association
FOR EACH ROW
EXECUTE FUNCTION node_access_tags_set_parent()
"""
        )
        op.execute(
            """
CREATE FUNCTION node_access_tags_sync_parent_after_node_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE node_access_tags_association
    SET parent_id = NEW.parent
    WHERE node_id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""
        )
        op.execute(
            """
CREATE TRIGGER node_access_tags_sync_parent_after_node_update
AFTER UPDATE OF parent ON nodes
FOR EACH ROW
WHEN (NEW.parent IS DISTINCT FROM OLD.parent)
EXECUTE FUNCTION node_access_tags_sync_parent_after_node_update()
"""
        )


def _drop_triggers(connection):
    if connection.dialect.name == "sqlite":
        op.execute("DROP TRIGGER IF EXISTS node_access_tags_set_parent_after_insert")
        op.execute("DROP TRIGGER IF EXISTS node_access_tags_set_parent_after_update")
        op.execute(
            "DROP TRIGGER IF EXISTS node_access_tags_sync_parent_after_node_update"
        )
    elif connection.dialect.name == "postgresql":
        op.execute(
            "DROP TRIGGER IF EXISTS node_access_tags_set_parent "
            "ON node_access_tags_association"
        )
        op.execute(
            "DROP TRIGGER IF EXISTS node_access_tags_sync_parent_after_node_update "
            "ON nodes"
        )
        op.execute("DROP FUNCTION IF EXISTS node_access_tags_set_parent()")
        op.execute(
            "DROP FUNCTION IF EXISTS node_access_tags_sync_parent_after_node_update()"
        )


def upgrade():
    connection = op.get_bind()
    dialect_name = connection.dialect.name

    op.add_column(
        "node_access_tags_association",
        sa.Column("parent_id", sa.Integer(), nullable=True),
    )
    _create_triggers(connection)

    if dialect_name == "postgresql":
        op.execute(
            """
UPDATE node_access_tags_association AS assignment
SET parent_id = node.parent
FROM nodes AS node
WHERE node.id = assignment.node_id
  AND assignment.parent_id IS DISTINCT FROM node.parent
"""
        )
        # Avoid blocking reads and writes for the duration of an index build
        # over a potentially very large association table.
        with op.get_context().autocommit_block():
            op.create_index(
                INDEX_NAME,
                "node_access_tags_association",
                ["parent_id", "tag_id", "node_id"],
                postgresql_concurrently=True,
            )
        op.execute("ANALYZE node_access_tags_association")
    else:
        op.execute(
            """
UPDATE node_access_tags_association
SET parent_id = (
    SELECT parent FROM nodes
    WHERE nodes.id = node_access_tags_association.node_id
)
"""
        )
        op.create_index(
            INDEX_NAME,
            "node_access_tags_association",
            ["parent_id", "tag_id", "node_id"],
        )


def downgrade():
    connection = op.get_bind()
    dialect_name = connection.dialect.name

    _drop_triggers(connection)
    if dialect_name == "postgresql":
        with op.get_context().autocommit_block():
            op.drop_index(
                INDEX_NAME,
                table_name="node_access_tags_association",
                postgresql_concurrently=True,
            )
        op.drop_column("node_access_tags_association", "parent_id")
    else:
        op.drop_index(INDEX_NAME, table_name="node_access_tags_association")
        with op.batch_alter_table("node_access_tags_association") as batch_op:
            batch_op.drop_column("parent_id")
