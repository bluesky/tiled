"""Convert access_blob column to assoc. table

Revision ID: de302a096358
Revises: b93c79d197f4
Create Date: 2026-07-03 14:27:39.197261

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = "de302a096358"
down_revision = "b93c79d197f4"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name
    access_tags_variant = sa.JSON().with_variant(sa.ARRAY(sa.String()), "postgresql")

    op.create_table(
        "access_blobs",
        sa.Column(
            "node_id",
            sa.Integer(),
            sa.ForeignKey("nodes.id", ondelete="CASCADE"),
            nullable=False,
            primary_key=True,
            unique=True,
        ),
        sa.Column("kind", sa.Enum("user", "tags", name="access_kind"), nullable=False),
        sa.Column("username", sa.String(), nullable=True),
        sa.Column("tags", access_tags_variant, nullable=True),
        sa.CheckConstraint(
            "(username IS NOT NULL AND tags IS NULL) OR "
            "(username IS NULL AND tags IS NOT NULL)",
            name="ck_access_blob_user_xor_tags",
        ),
    )
    op.create_index(
        "ix_access_blobs_username_user",
        "access_blobs",
        ["username"],
        sqlite_where=sa.text("kind = 'user' AND username IS NOT NULL"),
        postgresql_where=sa.text("kind = 'user' AND username IS NOT NULL"),
    )
    op.create_index(
        "ix_access_blobs_kind_node_id",
        "access_blobs",
        ["kind", "node_id"],
    )
    if dialect_name == "postgresql":
        op.create_index(
            "ix_access_blobs_tags_gin",
            "access_blobs",
            ["tags"],
            postgresql_using="gin",
            postgresql_where=sa.text("kind = 'tags' AND tags IS NOT NULL"),
        )

    if dialect_name == "postgresql":
        op.execute(
            """
            INSERT INTO access_blobs (node_id, kind, username, tags)
            SELECT
                id AS node_id,
                CASE
                    WHEN access_blob ? 'user' THEN 'user'::access_kind
                    ELSE 'tags'::access_kind
                END AS kind,
                access_blob ->> 'user' AS username,
                CASE
                    WHEN access_blob ? 'user' THEN NULL
                    WHEN access_blob ? 'tags' THEN ARRAY(
                        SELECT jsonb_array_elements_text(access_blob -> 'tags')
                    )::varchar[]
                    ELSE ARRAY[]::varchar[]
                END AS tags
            FROM nodes
            """
        )
    elif dialect_name == "sqlite":
        op.execute(
            """
            INSERT INTO access_blobs (node_id, kind, username, tags)
            SELECT
                id AS node_id,
                CASE
                    WHEN json_type(access_blob, '$.user') IS NOT NULL THEN 'user'
                    ELSE 'tags'
                END AS kind,
                json_extract(access_blob, '$.user') AS username,
                CASE
                    WHEN json_type(access_blob, '$.user') IS NOT NULL THEN NULL
                    WHEN json_type(access_blob, '$.tags') IS NOT NULL THEN json_extract(access_blob, '$.tags')
                    ELSE json('[]')
                END AS tags
            FROM nodes
            """
        )
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect_name}")

    op.drop_index("top_level_metadata", table_name="nodes")
    with op.batch_alter_table("nodes") as batch_op:
        batch_op.drop_column("access_blob")
    op.create_index(
        "top_level_metadata",
        "nodes",
        ["parent", "time_created", "id", "metadata"],
        postgresql_using="gin",
    )


def downgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name

    op.drop_index("top_level_metadata", table_name="nodes")
    with op.batch_alter_table("nodes") as batch_op:
        batch_op.add_column(
            sa.Column("access_blob", JSONVariant, nullable=False, server_default="{}")
        )
    op.create_index(
        "top_level_metadata",
        "nodes",
        ["parent", "time_created", "id", "metadata", "access_blob"],
        postgresql_using="gin",
    )

    if dialect_name == "postgresql":
        op.execute(
            """
            UPDATE nodes
            SET access_blob = COALESCE(
                (
                    SELECT CASE
                        WHEN access_blobs.kind = 'user' THEN
                            jsonb_build_object('user', access_blobs.username)
                        WHEN access_blobs.kind = 'tags' THEN
                            jsonb_build_object(
                                'tags',
                                to_jsonb(COALESCE(access_blobs.tags, ARRAY[]::varchar[]))
                            )
                        ELSE '{}'::jsonb
                    END
                    FROM access_blobs
                    WHERE access_blobs.node_id = nodes.id
                ),
                '{}'::jsonb
            )
            """
        )
    elif dialect_name == "sqlite":
        op.execute(
            """
            UPDATE nodes
            SET access_blob = COALESCE(
                (
                    SELECT CASE
                        WHEN access_blobs.kind = 'user' THEN
                            json_object('user', access_blobs.username)
                        WHEN access_blobs.kind = 'tags' THEN
                            json_object('tags', COALESCE(access_blobs.tags, json('[]')))
                        ELSE json('{}')
                    END
                    FROM access_blobs
                    WHERE access_blobs.node_id = nodes.id
                ),
                json('{}')
            )
            """
        )
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect_name}")

    op.drop_table("access_blobs")
    if dialect_name == "postgresql":
        op.execute("DROP TYPE IF EXISTS access_kind")
