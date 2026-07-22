"""Add graph entities and links tables

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
        sa.Column("access_blob", sa.JSON(), nullable=False),
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


def downgrade():
    op.drop_index("links_triple_idx", table_name="links")
    op.drop_index("links_predicate_object_idx", table_name="links")
    op.drop_index("links_subject_predicate_idx", table_name="links")
    op.drop_table("links")

    op.drop_index("entities_node_id_idx", table_name="entities")
    op.drop_index("entities_uri_idx", table_name="entities")
    op.drop_index("entities_type_created_idx", table_name="entities")
    op.drop_table("entities")
