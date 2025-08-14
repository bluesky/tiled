"""Redefine the catalog tree using a closure table

Revision ID: e05e918092c3
Revises: 7809873ea2c7
Create Date: 2025-06-16 10:38:46.797381

This revision introduces a closure table to represent the hierarchical structure of nodes in the catalog.

Tree structure:
---------------

root
├── A
│   ├── C
│   │   ├── E
│   │   ├── F
│   │   └── G
│   └── D
└── B

Before:
-------

> SELECT * FROM nodes;

id  key  ancestors  structure_family  metadata   ...
--  ---  ---------  ----------------  --------
1   A    []         container         {}
2   B    []         container         {}
3   C    ["A"]      container         {}
4   D    ["A"]      container         {}
5   E    ["A","C"]  container         {}
6   F    ["A","C"]  container         {}
7   G    ["A","C"]  container         {}

After:
------

> SELECT * FROM nodes;

id  parent  key  structure_family  metadata   ...
--  ------  ---  ----------------  --------
0                container         {}         <-- root node (always present)
1   0       A    container         {}
2   0       B    container         {}
3   1       C    container         {}
4   1       D    container         {}
5   3       E    container         {}
6   3       F    container         {}
7   3       G    container         {}

> SELECT * FROM nodes_closure;

ancestor  descendant  depth
--------  ----------  -----
0         0           0
1         1           0
0         1           1
2         2           0
0         2           1
3         3           0
1         3           1
0         3           2
4         4           0
1         4           1
0         4           2
5         5           0
3         5           1
1         5           2
0         5           3
6         6           0
3         6           1
1         6           2
0         6           3
7         7           0
3         7           1
1         7           2
0         7           3

A new node with `id = 0` and no parent representing the root of the tree is added to the nodes table.
The `parent` column is introduced to represent the parent-child relationships between nodes.
The `ancestors` column is removed, as it is no longer needed with the closure table structure.

"""
import logging

import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = "e05e918092c3"
down_revision = "7809873ea2c7"
branch_labels = None
depends_on = None

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


def upgrade():
    connection = op.get_bind()

    logger.info("Starting migration to add closure table...")

    # 1. Add the 'parent' column and the foreign key to the 'nodes' table. Use batch mode, so it works for SQLite.
    with op.batch_alter_table("nodes", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("parent", sa.Integer(), nullable=True, index=True)
        )
        batch_op.create_foreign_key("fk_nodes_parent", "nodes", ["parent"], ["id"])
        batch_op.drop_constraint("key_ancestors_unique_constraint", type_="unique")
    logger.info("Added 'parent' column and foreign key to 'nodes' table.")

    # 2. Create the 'nodes_closure' table and create the uniqueness constraint
    op.create_table(
        "nodes_closure",
        sa.Column(
            "ancestor", sa.Integer(), sa.ForeignKey("nodes.id"), primary_key=True
        ),
        sa.Column(
            "descendant", sa.Integer(), sa.ForeignKey("nodes.id"), primary_key=True
        ),
        sa.Column("depth", sa.Integer(), nullable=False),
    )
    with op.batch_alter_table("nodes_closure", schema=None) as batch_op:
        batch_op.create_unique_constraint(
            "ancestor_descendant_unique_constraint", ["ancestor", "descendant"]
        )
    op.create_index("idx_nodes_closure_ancestor", "nodes_closure", ["ancestor"])
    op.create_index("idx_nodes_closure_descendant", "nodes_closure", ["descendant"])
    logger.info("Created 'nodes_closure' table with uniqueness constraint.")

    # 3. Insert the explicit root node (id=0, key='') with no parent
    connection.execute(
        sa.text(
            """
        INSERT INTO nodes(id, key, parent, ancestors, structure_family, metadata, specs, access_blob)
        VALUES (0, '', NULL, NULL, 'container', '{}', '[]', '{}');
        """
        )
    )
    logger.info("Inserted root node with id=0.")

    # 4. Insert self-referential records into nodes_closure for each node, including the "root" node
    connection.execute(
        sa.text(
            """
        INSERT INTO nodes_closure(ancestor, descendant, depth)
        SELECT id, id, 0 FROM nodes;
        """
        )
    )
    logger.info("Inserted self-referential records into 'nodes_closure' for each node.")

    # 5. Populate the 'parent' column of the 'nodes' table based on the 'ancestors' column
    json_len_func = (
        "jsonb_array_length"
        if connection.engine.dialect.name == "postgresql"
        else "json_array_length"
    )
    max_depth = (
        connection.execute(
            sa.text(
                f"SELECT MAX({json_len_func}(ancestors)) FROM nodes WHERE ancestors IS NOT NULL;"
            )
        ).scalar()
        or 0
    )
    logger.info(f"Maximum depth of ancestors found: {max_depth}")

    # 6. Initialize the parent of each node as 0 (the 'root' node) and set 'depth' in the closure table
    connection.execute(
        sa.text("UPDATE nodes SET parent = 0 WHERE ancestors IS NOT NULL;")
    )
    connection.execute(
        sa.text(
            f"""
        INSERT INTO nodes_closure(ancestor, descendant, depth)
        SELECT 0, id, {json_len_func}(ancestors) + 1 FROM nodes WHERE ancestors IS NOT NULL;
        """
        )
    )
    logger.info(
        "Initialized parent of each node as 0 and set depth in 'nodes_closure'."
    )

    # 7. Update the 'parent' column recursively
    for depth in range(max_depth):
        condition_statement = (
            f"parent.key = child.ancestors::json->>{depth}"
            if connection.engine.dialect.name == "postgresql"
            else f"parent.key = json_extract(child.ancestors, '$[{depth}]')"
        )
        connection.execute(
            sa.text(
                f"""
            UPDATE nodes AS child
            SET parent = parent.id
            FROM nodes AS parent
            WHERE {json_len_func}(child.ancestors) >= {depth + 1}
            AND {condition_statement}
            AND parent.parent = child.parent
        """
            )
        )

        # Populate the 'nodes_closure' table   (possibly use ON CONFLICT DO NOTHING ?)
        connection.execute(
            sa.text(
                f"""
            INSERT INTO nodes_closure (ancestor, descendant, depth)
            SELECT parent.id, child.id, {json_len_func}(child.ancestors) - {depth}
            FROM nodes AS child
            JOIN nodes AS parent ON parent.id = child.parent
            WHERE {json_len_func}(child.ancestors) >= {depth + 1};
        """
            )
        )
        logger.info(
            f"Updated 'parent' column and 'nodes_closure' for depth {depth + 1}."
        )
    logger.info("Completed updating 'parent' column recursively.")

    # 8. Update index in the 'nodes' table: drop old, add new
    op.drop_index("top_level_metadata", table_name="nodes")
    op.create_index(
        "top_level_metadata",
        "nodes",
        ["parent", "time_created", "id", "metadata", "access_blob"],
        postgresql_using="gin",
    )
    logger.info("Updated index in the 'nodes' table.")

    # 9. Create constraint to ensure uniqueness of (key, parent) pairs
    with op.batch_alter_table("nodes", schema=None) as batch_op:
        batch_op.create_unique_constraint(
            "key_parent_unique_constraint", ["key", "parent"]
        )
    logger.info("Created unique constraint on (key, parent) pairs in 'nodes' table.")

    # 10. Drop the 'ancestors' column from the 'nodes' table
    op.drop_column("nodes", "ancestors")
    logger.info("Dropped 'ancestors' column from the 'nodes' table.")

    # 11. Add triggers to maintain the closure table
    if connection.engine.dialect.name == "sqlite":
        # Create a trigger to update the closure table when INSERTING a new node
        connection.execute(
            sa.text(
                """
CREATE TRIGGER update_closure_table_when_inserting
AFTER INSERT ON nodes
BEGIN
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT NEW.id, NEW.id, 0;
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT p.ancestor, c.descendant, p.depth+c.depth+1
    FROM nodes_closure p, nodes_closure c
    WHERE p.descendant=NEW.parent and c.ancestor=NEW.id;
END"""
            )
        )

        # Create a trigger to update the closure table when DELETING a node
        connection.execute(
            sa.text(
                """
CREATE TRIGGER update_closure_table_when_deleting
BEFORE DELETE ON nodes
BEGIN
    DELETE FROM nodes_closure
    WHERE (ancestor, descendant) IN (
    SELECT p.ancestor, c.descendant
    FROM nodes_closure p, nodes_closure c
    WHERE (p.descendant=OLD.parent OR p.descendant=OLD.id) AND c.ancestor=OLD.id);
END"""
            )
        )

    elif connection.engine.dialect.name == "postgresql":
        # Create function and trigger to update the closure table when INSERTING a new node
        connection.execute(
            sa.text(
                """
CREATE OR REPLACE FUNCTION update_closure_table_when_inserting()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    VALUES (NEW.id, NEW.id, 0);
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT p.ancestor, c.descendant, p.depth + c.depth + 1
    FROM nodes_closure p, nodes_closure c
    WHERE p.descendant = NEW.parent AND c.ancestor = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
            )
        )

        connection.execute(
            sa.text(
                """
CREATE TRIGGER update_closure_table_when_inserting
AFTER INSERT ON nodes
FOR EACH ROW
EXECUTE FUNCTION update_closure_table_when_inserting();
"""
            )
        )

        # Create function and trigger to update the closure table when DELETING a node
        connection.execute(
            sa.text(
                """
CREATE OR REPLACE FUNCTION update_closure_table_when_deleting()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM nodes_closure
    WHERE (ancestor, descendant) IN (
        SELECT p.ancestor, c.descendant
        FROM nodes_closure p, nodes_closure c
        WHERE (p.descendant = OLD.parent OR p.descendant = OLD.id)
        AND c.ancestor = OLD.id
    );
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
"""
            )
        )

        connection.execute(
            sa.text(
                """
CREATE TRIGGER update_closure_table_when_deleting
BEFORE DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION update_closure_table_when_deleting();
"""
            )
        )
    logger.info("Added triggers to maintain the closure table.")
    logger.info("Migration to add closure table completed successfully.")


def downgrade():
    connection = op.get_bind()

    logger.info("Starting downgrade to remove closure table...")

    # 1. Drop triggers and functions for maintaining the closure table
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            sa.text("DROP TRIGGER IF EXISTS update_closure_table_when_inserting")
        )
        connection.execute(
            sa.text("DROP TRIGGER IF EXISTS update_closure_table_when_deleting")
        )
    elif connection.engine.dialect.name == "postgresql":
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS update_closure_table_when_inserting ON nodes"
            )
        )
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS update_closure_table_when_deleting ON nodes"
            )
        )
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS update_closure_table_when_inserting")
        )
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS update_closure_table_when_deleting")
        )
    logger.info("Dropped triggers and functions for maintaining the closure table.")

    # 2. Re-add the 'ancestors' column to the 'nodes' table
    op.add_column("nodes", sa.Column("ancestors", JSONVariant, nullable=True))

    # 3. Reconstruct 'ancestors' for each node from 'parent'. Skip the root node (id=0)
    # In PostgreSQL, jsonb_agg does not support ORDER BY directly in SELECT.
    # SQLite’s json_group_array only allows ORDER BY outside the function.
    select_stmt = (
        "SELECT jsonb_agg(n.key ORDER BY nc.depth DESC)"
        if connection.engine.dialect.name == "postgresql"
        else "SELECT json_group_array(n.key)"
    )
    connection.execute(
        sa.text(
            f"""
            UPDATE nodes
            SET ancestors = COALESCE((
                {select_stmt}
                FROM nodes_closure nc
                JOIN nodes n ON nc.ancestor = n.id
                WHERE nc.descendant = nodes.id AND nc.depth > 0 AND nc.ancestor != 0
                {"" if connection.engine.dialect.name == "postgresql" else "ORDER BY nc.depth DESC"}
            ), '[]'{"::jsonb" if connection.engine.dialect.name == "postgresql" else ""})
            WHERE id != 0;
            """
        )
    )
    logger.info("Reconstructed 'ancestors' for each node from 'parent'.")

    # 4. Drop the closure table
    op.drop_table("nodes_closure")
    logger.info("Dropped 'nodes_closure' table.")

    # 5. Restore the old index, drop the new one
    op.drop_index("top_level_metadata", table_name="nodes")
    op.create_index(
        "top_level_metadata",
        "nodes",
        ["time_created", "id", "ancestors", "metadata", "access_blob"],
        postgresql_using="gin",
    )
    logger.info("Restored old index in the 'nodes' table.")

    # 6. Drop the 'parent' column and related foreign key/unique constraint
    with op.batch_alter_table("nodes", schema=None) as batch_op:
        batch_op.drop_index("idx_nodes_parent")
        batch_op.drop_constraint("fk_nodes_parent", type_="foreignkey")
        batch_op.drop_constraint("key_parent_unique_constraint", type_="unique")
        batch_op.create_unique_constraint(
            "key_ancestors_unique_constraint", ["key", "ancestors"]
        )
        batch_op.drop_column("parent")
    logger.info(
        "Dropped 'parent' column and restored 'key_ancestors_unique_constraint'."
    )

    # 7. Remove the explicit root node from the 'nodes' table
    connection.execute(sa.text("DELETE FROM nodes WHERE id = 0"))
    logger.info("Removed the root node from the 'nodes' table.")
    logger.info("Downgrade to remove closure table completed successfully.")
