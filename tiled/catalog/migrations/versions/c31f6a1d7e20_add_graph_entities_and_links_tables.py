"""Add graph entities, links, and namespaces tables

Revision ID: c31f6a1d7e20
Revises: 9bc9b57294b9
Create Date: 2026-07-21 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c31f6a1d7e20"
down_revision = "9bc9b57294b9"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entities",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=True),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("uri", sa.String(), nullable=True),
        sa.Column("properties", sa.JSON(), nullable=False),
        # Nullable: an entity with node_id set must not have its own
        # access_blob (see the trigger below), because access control for
        # such an entity is delegated to the node it points to.
        sa.Column("access_blob", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["node_id"], ["nodes.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("entities_node_id_idx", "entities", ["node_id"], unique=False)
    op.create_index(
        "entities_type_created_idx",
        "entities",
        ["entity_type", "created_at"],
        unique=False,
    )
    op.create_index("entities_uri_idx", "entities", ["uri"], unique=False)

    op.create_table(
        "links",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("subject_id", sa.String(), nullable=False),
        sa.Column("predicate", sa.String(), nullable=False),
        sa.Column("object_id", sa.String(), nullable=False),
        sa.Column("properties", sa.JSON(), nullable=False),
        sa.Column("access_blob", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["object_id"], ["entities.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["entities.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "links_subject_predicate_idx",
        "links",
        ["subject_id", "predicate"],
        unique=False,
    )
    op.create_index(
        "links_predicate_object_idx",
        "links",
        ["predicate", "object_id"],
        unique=False,
    )
    op.create_index(
        "links_triple_idx",
        "links",
        ["subject_id", "predicate", "object_id"],
        unique=False,
    )

    op.create_table(
        "namespaces",
        sa.Column("prefix", sa.String(), nullable=False),
        sa.Column("uri", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("prefix"),
    )

    # An entity that points to a catalog node delegates access control to
    # that node, so its own access_blob must be NULL. The application layer
    # (tiled.graph.schema) validates this up front for a friendly error
    # message; this trigger is the data-integrity backstop.
    connection = op.get_bind()
    error_message = (
        "An entity with node_id set must not have its own access_blob; "
        "access is controlled by the referenced node."
    )
    if connection.engine.dialect.name == "sqlite":
        with op.get_context().autocommit_block():
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER entities_node_access_blob_insert
BEFORE INSERT ON entities
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{error_message}');
END"""
                )
            )
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER entities_node_access_blob_update
BEFORE UPDATE ON entities
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{error_message}');
END"""
                )
            )
    else:
        # PostgreSQL
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '{error_message}';
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER entities_node_access_blob_check
BEFORE INSERT OR UPDATE ON entities
FOR EACH ROW
WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
EXECUTE FUNCTION entities_reject_node_access_blob();"""
            )
        )


def downgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        with op.get_context().autocommit_block():
            connection.execute(sa.text("DROP TRIGGER entities_node_access_blob_update"))
            connection.execute(sa.text("DROP TRIGGER entities_node_access_blob_insert"))
    else:
        # PostgreSQL
        connection.execute(
            sa.text("DROP TRIGGER entities_node_access_blob_check ON entities")
        )
        connection.execute(sa.text("DROP FUNCTION entities_reject_node_access_blob"))

    op.drop_table("namespaces")

    op.drop_index("links_triple_idx", table_name="links")
    op.drop_index("links_predicate_object_idx", table_name="links")
    op.drop_index("links_subject_predicate_idx", table_name="links")
    op.drop_table("links")

    op.drop_index("entities_node_id_idx", table_name="entities")
    op.drop_index("entities_uri_idx", table_name="entities")
    op.drop_index("entities_type_created_idx", table_name="entities")
    op.drop_table("entities")
