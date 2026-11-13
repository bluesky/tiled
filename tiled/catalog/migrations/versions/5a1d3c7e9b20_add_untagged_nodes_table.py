"""Add untagged_nodes table for access-tag inheritance.

Revision ID: 5a1d3c7e9b20
Revises: 917dbbcfc4b1
Create Date: 2026-11-13

A node with no access tags of its own inherits those of its nearest tagged
ancestor. Access filters need a container's untagged children; finding them in
'node_access_tags_association' means scanning every tag assignment under the
container. This table lists them directly, indexed by parent, and triggers keep
it up to date.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5a1d3c7e9b20"
down_revision = "917dbbcfc4b1"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_untagged_nodes_parent_id_node_id"

# A frozen copy of tiled.catalog.orm.untagged_nodes_trigger_statements.
TRIGGERS = {
    "sqlite": [
        """
CREATE TRIGGER untagged_nodes_after_node_insert
AFTER INSERT ON nodes
BEGIN
    INSERT OR IGNORE INTO untagged_nodes (node_id, parent_id)
    VALUES (NEW.id, NEW.parent);
END""",
        """
CREATE TRIGGER untagged_nodes_after_node_move
AFTER UPDATE OF parent ON nodes
WHEN NEW.parent IS NOT OLD.parent
BEGIN
    UPDATE untagged_nodes SET parent_id = NEW.parent WHERE node_id = NEW.id;
END""",
        """
CREATE TRIGGER untagged_nodes_after_tag_insert
AFTER INSERT ON node_access_tags_association
BEGIN
    DELETE FROM untagged_nodes WHERE node_id = NEW.node_id;
END""",
        """
CREATE TRIGGER untagged_nodes_after_tag_delete
AFTER DELETE ON node_access_tags_association
BEGIN
    INSERT OR IGNORE INTO untagged_nodes (node_id, parent_id)
    SELECT id, parent FROM nodes
    WHERE id = OLD.node_id
      AND NOT EXISTS (
        SELECT 1 FROM node_access_tags_association WHERE node_id = OLD.node_id
      );
END""",
        """
CREATE TRIGGER untagged_nodes_after_tag_update
AFTER UPDATE OF node_id ON node_access_tags_association
BEGIN
    DELETE FROM untagged_nodes WHERE node_id = NEW.node_id;
    INSERT OR IGNORE INTO untagged_nodes (node_id, parent_id)
    SELECT id, parent FROM nodes
    WHERE id = OLD.node_id
      AND NOT EXISTS (
        SELECT 1 FROM node_access_tags_association WHERE node_id = OLD.node_id
      );
END""",
    ],
    "postgresql": [
        """
CREATE OR REPLACE FUNCTION untagged_nodes_after_node_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO untagged_nodes (node_id, parent_id)
    VALUES (NEW.id, NEW.parent)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql""",
        """
CREATE TRIGGER untagged_nodes_after_node_insert
AFTER INSERT ON nodes
FOR EACH ROW
EXECUTE FUNCTION untagged_nodes_after_node_insert()""",
        """
CREATE OR REPLACE FUNCTION untagged_nodes_after_node_move()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE untagged_nodes SET parent_id = NEW.parent WHERE node_id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql""",
        """
CREATE TRIGGER untagged_nodes_after_node_move
AFTER UPDATE OF parent ON nodes
FOR EACH ROW
WHEN (NEW.parent IS DISTINCT FROM OLD.parent)
EXECUTE FUNCTION untagged_nodes_after_node_move()""",
        """
CREATE OR REPLACE FUNCTION untagged_nodes_after_tag_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        DELETE FROM untagged_nodes WHERE node_id = NEW.node_id;
    END IF;
    IF TG_OP IN ('DELETE', 'UPDATE') THEN
        INSERT INTO untagged_nodes (node_id, parent_id)
        SELECT id, parent FROM nodes
        WHERE id = OLD.node_id
          AND NOT EXISTS (
            SELECT 1 FROM node_access_tags_association
            WHERE node_id = OLD.node_id
          )
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql""",
        """
CREATE TRIGGER untagged_nodes_after_tag_change
AFTER INSERT OR DELETE OR UPDATE OF node_id ON node_access_tags_association
FOR EACH ROW
EXECUTE FUNCTION untagged_nodes_after_tag_change()""",
    ],
}

DROP_TRIGGERS = {
    "sqlite": [
        "DROP TRIGGER IF EXISTS untagged_nodes_after_node_insert",
        "DROP TRIGGER IF EXISTS untagged_nodes_after_node_move",
        "DROP TRIGGER IF EXISTS untagged_nodes_after_tag_insert",
        "DROP TRIGGER IF EXISTS untagged_nodes_after_tag_delete",
        "DROP TRIGGER IF EXISTS untagged_nodes_after_tag_update",
    ],
    "postgresql": [
        "DROP TRIGGER IF EXISTS untagged_nodes_after_node_insert ON nodes",
        "DROP TRIGGER IF EXISTS untagged_nodes_after_node_move ON nodes",
        "DROP TRIGGER IF EXISTS untagged_nodes_after_tag_change "
        "ON node_access_tags_association",
        "DROP FUNCTION IF EXISTS untagged_nodes_after_node_insert()",
        "DROP FUNCTION IF EXISTS untagged_nodes_after_node_move()",
        "DROP FUNCTION IF EXISTS untagged_nodes_after_tag_change()",
    ],
}


def upgrade():
    connection = op.get_bind()
    dialect_name = connection.dialect.name

    op.create_table(
        "untagged_nodes",
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["nodes.id"],
            name="fk_untagged_nodes_node",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("node_id"),
    )
    op.execute(
        """
INSERT INTO untagged_nodes (node_id, parent_id)
SELECT id, parent FROM nodes
WHERE NOT EXISTS (
    SELECT 1 FROM node_access_tags_association
    WHERE node_access_tags_association.node_id = nodes.id
)
"""
    )
    op.create_index(INDEX_NAME, "untagged_nodes", ["parent_id", "node_id"])
    for statement in TRIGGERS.get(dialect_name, []):
        op.execute(statement)
    if dialect_name == "postgresql":
        with op.get_context().autocommit_block():
            connection.execute(sa.text("VACUUM ANALYZE untagged_nodes"))


def downgrade():
    connection = op.get_bind()
    for statement in DROP_TRIGGERS.get(connection.dialect.name, []):
        op.execute(statement)
    op.drop_index(INDEX_NAME, table_name="untagged_nodes")
    op.drop_table("untagged_nodes")
