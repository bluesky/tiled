"""Optimize node deletion

Revision ID: a86a48befff6
Revises: dfbb7478c6bd
Create Date: 2026-01-23 14:31:23.869799

"""
import logging

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a86a48befff6"
down_revision = "dfbb7478c6bd"
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

    logger.info("Starting upgrade to optimize node deletion...")

    # 1. Drop on-delete triggers and functions if they exist (added in prior migration)
    if connection.engine.dialect.name == "sqlite":
        connection.execute(
            sa.text("DROP TRIGGER IF EXISTS update_closure_table_when_deleting")
        )
    elif connection.engine.dialect.name == "postgresql":
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS update_closure_table_when_deleting ON nodes"
            )
        )
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS update_closure_table_when_deleting")
        )
    logger.info("Dropped triggers and functions for maintaining the closure table.")

    # 2. Recreate constraints with ON DELETE CASCADE
    # Even though ondelete="CASCADE" have been declared in the ORM, Postgres (and SQLite)
    # only enforces it if the FK constraint was actually created with ON DELETE CASCADE.
    # If the constraint already existed, nothing changes in the database.
    # https://stackoverflow.com/a/10356720

    if connection.dialect.name == "postgresql":
        # PostgreSQL: direct ALTER TABLE, zero/low downtime
        # ---- nodes.parent -> nodes.id (ON DELETE CASCADE) ----
        op.execute(
            """
            ALTER TABLE nodes
            DROP CONSTRAINT IF EXISTS fk_nodes_parent
        """
        )

        op.execute(
            """
            ALTER TABLE nodes
            ADD CONSTRAINT fk_nodes_parent
            FOREIGN KEY (parent)
            REFERENCES nodes(id)
            ON DELETE CASCADE
            NOT VALID
        """
        )

        # ---- nodes_closure FKs ----
        op.execute(
            """
            ALTER TABLE nodes_closure
            DROP CONSTRAINT IF EXISTS nodes_closure_ancestor_fkey,
            DROP CONSTRAINT IF EXISTS nodes_closure_descendant_fkey
        """
        )

        op.execute(
            """
            ALTER TABLE nodes_closure
            ADD CONSTRAINT nodes_closure_ancestor_fkey
            FOREIGN KEY (ancestor)
            REFERENCES nodes(id)
            ON DELETE CASCADE
            NOT VALID
        """
        )

        op.execute(
            """
            ALTER TABLE nodes_closure
            ADD CONSTRAINT nodes_closure_descendant_fkey
            FOREIGN KEY (descendant)
            REFERENCES nodes(id)
            ON DELETE CASCADE
            NOT VALID
        """
        )

        # VALIDATE (non-blocking)
        op.execute(
            """
            ALTER TABLE nodes
            VALIDATE CONSTRAINT fk_nodes_parent
        """
        )

        op.execute(
            """
            ALTER TABLE nodes_closure
            VALIDATE CONSTRAINT nodes_closure_ancestor_fkey
        """
        )

        op.execute(
            """
            ALTER TABLE nodes_closure
            VALIDATE CONSTRAINT nodes_closure_descendant_fkey
        """
        )

    else:
        # SQLite / others: batch mode
        # ---- nodes.parent -> nodes.id (ON DELETE CASCADE) ----
        with op.batch_alter_table("nodes") as batch_op:
            batch_op.drop_constraint("fk_nodes_parent", type_="foreignkey")
            batch_op.create_foreign_key(
                "fk_nodes_parent",
                referent_table="nodes",
                local_cols=["parent"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )
        logger.info("Recreated fk_nodes_parent with ON DELETE CASCADE.")

        # ---- nodes_closure FKs ----
        with op.batch_alter_table("nodes_closure") as batch_op:
            batch_op.drop_constraint("nodes_closure_ancestor_fkey", type_="foreignkey")
            batch_op.drop_constraint(
                "nodes_closure_descendant_fkey", type_="foreignkey"
            )
            batch_op.create_foreign_key(
                "nodes_closure_ancestor_fkey",
                referent_table="nodes",
                local_cols=["ancestor"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )
            batch_op.create_foreign_key(
                "nodes_closure_descendant_fkey",
                referent_table="nodes",
                local_cols=["descendant"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )
        logger.info("Recreated nodes_closure FKs with ON DELETE CASCADE.")

    # 3. Drop the unique constraint on (ancestor, descendant) in nodes_closure (satisfied by PK)
    with op.batch_alter_table("nodes_closure") as batch_op:
        batch_op.drop_constraint(
            "ancestor_descendant_unique_constraint", type_="unique"
        )
    logger.info("Dropped unique constraint on (ancestor, descendant) in nodes_closure.")

    # 4. Drop redundant indices if they exist (satisfied by PKs)
    op.drop_index("ix_data_sources_id")
    op.drop_index("ix_assets_id")
    op.drop_index("ix_nodes_id")
    op.drop_index("ix_revisions_id")
    logger.info(
        "Dropped redundant indices on data_sources, assets, nodes, revisions tables."
    )

    # 5. Create indices on the data_source_asset_associations table
    op.create_index(
        "ix_data_source_asset_association_asset_id",
        "data_source_asset_association",
        ["asset_id"],
    )
    op.create_index(
        "ix_data_source_asset_association_data_source_id",
        "data_source_asset_association",
        ["data_source_id"],
    )
    logger.info("Created indices on data_source_asset_association table.")

    # 6. Create composite index on data_sources (id, node_id)
    op.create_index(
        "idx_data_sources_id_node",
        "data_sources",
        ["id", "node_id"],
    )
    logger.info("Created composite index on data_sources (id, node_id).")


def downgrade():
    # 1. Drop the indices on data_source_asset_association table
    op.drop_index(
        "ix_data_source_asset_association_data_source_id",
        table_name="data_source_asset_association",
    )
    op.drop_index(
        "ix_data_source_asset_association_asset_id",
        table_name="data_source_asset_association",
    )
    logger.info("Dropped indices on data_source_asset_association table.")

    # 2. Drop composite index on data_sources (id, node_id)
    op.drop_index("idx_data_sources_id_node", table_name="data_sources")
    logger.info("Dropped composite index on data_sources (id, node_id).")
