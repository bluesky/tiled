"""Initial schema: entities and links tables

Revision ID: 7f3a9d1c0b25
Revises:
Create Date: 2026-05-12

"""

import sqlalchemy as sa
from alembic import op

revision = "7f3a9d1c0b25"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entities",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("entity_type", sa.String, nullable=False),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("uri", sa.String, nullable=True),
        sa.Column("properties", sa.JSON, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("entities_type_created_idx", "entities", ["entity_type", "created_at"])
    op.create_index("entities_uri_idx", "entities", ["uri"])

    op.create_table(
        "links",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column(
            "subject_id",
            sa.String,
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("predicate", sa.String, nullable=False),
        sa.Column(
            "object_id",
            sa.String,
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("properties", sa.JSON, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("links_subject_predicate_idx", "links", ["subject_id", "predicate"])
    op.create_index("links_predicate_object_idx", "links", ["predicate", "object_id"])
    op.create_index("links_triple_idx", "links", ["subject_id", "predicate", "object_id"])


def downgrade():
    op.drop_table("links")
    op.drop_table("entities")
