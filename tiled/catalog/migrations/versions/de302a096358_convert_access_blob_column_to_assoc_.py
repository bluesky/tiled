"""Convert node, entity, and link access_blob columns to assoc. tables

Revision ID: de302a096358
Revises: c31f6a1d7e20
Create Date: 2026-07-03 14:27:39.197261

"""
import json

import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = "de302a096358"
down_revision = "c31f6a1d7e20"
branch_labels = None
depends_on = None


def _copy_access_blobs(
    connection, source_table, association_table, owner_column, where=""
):
    access_tags_variant = sa.JSON(none_as_null=True).with_variant(
        sa.ARRAY(sa.String()), "postgresql"
    )
    access_blobs = sa.table(
        "access_blobs",
        sa.column("id", sa.Integer),
        sa.column("kind", sa.Enum("user", "tags", name="access_kind")),
        sa.column("username", sa.String),
        sa.column("tags", access_tags_variant),
    )
    associations = sa.table(
        association_table,
        sa.column(owner_column),
        sa.column("access_blob_id", sa.Integer),
    )
    rows = connection.execute(
        sa.text(f"SELECT id, access_blob FROM {source_table} {where}")
    )
    for row in rows:
        access_blob = row.access_blob
        if isinstance(access_blob, str):
            access_blob = json.loads(access_blob)
        access_blob = access_blob or {}
        if (
            source_table == "nodes"
            and row.id == 0
            and (not access_blob or access_blob.get("tags") == [])
        ):
            access_blob = {"tags": ["public"]}
        values = (
            {"kind": "user", "username": access_blob["user"], "tags": None}
            if "user" in access_blob
            else {"kind": "tags", "username": None, "tags": access_blob.get("tags", [])}
        )
        access_blob_id = connection.execute(
            access_blobs.insert().values(**values).returning(access_blobs.c.id)
        ).scalar_one()
        connection.execute(
            associations.insert().values(
                **{owner_column: row.id, "access_blob_id": access_blob_id}
            )
        )


def _restore_access_blobs(
    dialect_name, source_table, id_column, destination_table, default_empty=True
):
    if dialect_name == "postgresql":
        fallback = "'{}'::jsonb" if default_empty else "NULL"
        op.execute(
            f"""
            UPDATE {destination_table}
            SET access_blob = COALESCE(
                (
                    SELECT CASE
                        WHEN access_blobs.kind = 'user' THEN
                            jsonb_build_object('user', access_blobs.username)
                        ELSE jsonb_build_object(
                            'tags', to_jsonb(COALESCE(access_blobs.tags, ARRAY[]::varchar[]))
                        )
                    END FROM {source_table}
                    JOIN access_blobs ON access_blobs.id = {source_table}.access_blob_id
                    WHERE {source_table}.{id_column} = {destination_table}.id
                ),
                {fallback}
            )
            """
        )
    elif dialect_name == "sqlite":
        fallback = "json('{{}}')" if default_empty else "NULL"
        op.execute(
            f"""
            UPDATE {destination_table}
            SET access_blob = COALESCE(
                (
                    SELECT CASE
                        WHEN access_blobs.kind = 'user' THEN
                            json_object('user', access_blobs.username)
                        ELSE json_object('tags', COALESCE(access_blobs.tags, json('[]')))
                    END FROM {source_table}
                    JOIN access_blobs ON access_blobs.id = {source_table}.access_blob_id
                    WHERE {source_table}.{id_column} = {destination_table}.id
                ),
                {fallback}
            )
            """
        )
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect_name}")


def _create_association_triggers(connection):
    dialect_name = connection.engine.dialect.name
    tables = ("node_access_blobs", "entity_access_blobs", "link_access_blobs")
    error_message = (
        "An entity with node_id set must not have its own access blob; "
        "access is controlled by the referenced node."
    )
    if dialect_name == "sqlite":
        for table in tables:
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER {table}_delete_cleanup
AFTER DELETE ON {table}
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
END"""
                )
            )
            others = " OR ".join(
                f"EXISTS (SELECT 1 FROM {other} WHERE access_blob_id = NEW.access_blob_id)"
                for other in tables
                if other != table
            )
            for operation in ("INSERT", "UPDATE OF access_blob_id"):
                connection.execute(
                    sa.text(
                        f"""
CREATE TRIGGER {table}_{operation.split()[0].lower()}_reject_shared_access_blob
BEFORE {operation} ON {table}
WHEN {others}
BEGIN
    SELECT RAISE(ABORT, 'An access blob may belong to only one node, entity, or link');
END"""
                    )
                )
        for operation in ("INSERT", "UPDATE OF entity_id"):
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER entity_access_blobs_{operation.split()[0].lower()}_reject_node_backed_entity
BEFORE {operation} ON entity_access_blobs
WHEN EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{error_message}');
END"""
                )
            )
        for operation in ("INSERT", "UPDATE OF node_id"):
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER entities_node_access_blob_{operation.split()[0].lower()}
BEFORE {operation} ON entities
WHEN NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
)
BEGIN
    SELECT RAISE(ABORT, '{error_message}');
END"""
                )
            )
    elif dialect_name == "postgresql":
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION delete_orphaned_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION reject_shared_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_TABLE_NAME = 'node_access_blobs' AND EXISTS (
        SELECT 1 FROM entity_access_blobs WHERE access_blob_id = NEW.access_blob_id
        UNION ALL SELECT 1 FROM link_access_blobs WHERE access_blob_id = NEW.access_blob_id
    )) OR (TG_TABLE_NAME = 'entity_access_blobs' AND EXISTS (
        SELECT 1 FROM node_access_blobs WHERE access_blob_id = NEW.access_blob_id
        UNION ALL SELECT 1 FROM link_access_blobs WHERE access_blob_id = NEW.access_blob_id
    )) OR (TG_TABLE_NAME = 'link_access_blobs' AND EXISTS (
        SELECT 1 FROM node_access_blobs WHERE access_blob_id = NEW.access_blob_id
        UNION ALL SELECT 1 FROM entity_access_blobs WHERE access_blob_id = NEW.access_blob_id
    )) THEN
        RAISE EXCEPTION 'An access blob may belong to only one node, entity, or link';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '{error_message}';
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION entity_access_blob_reject_node_backed_entity()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL) THEN
        RAISE EXCEPTION '{error_message}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        for table in tables:
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER {table}_delete_cleanup
AFTER DELETE ON {table}
FOR EACH ROW EXECUTE FUNCTION delete_orphaned_access_blob();
CREATE TRIGGER {table}_reject_shared_access_blob
BEFORE INSERT OR UPDATE OF access_blob_id ON {table}
FOR EACH ROW EXECUTE FUNCTION reject_shared_access_blob();"""
                )
            )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER entity_access_blobs_reject_node_backed_entity
BEFORE INSERT OR UPDATE OF entity_id ON entity_access_blobs
FOR EACH ROW EXECUTE FUNCTION entity_access_blob_reject_node_backed_entity();
CREATE TRIGGER entities_node_access_blob_check
BEFORE UPDATE OF node_id ON entities
FOR EACH ROW WHEN (NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
))
EXECUTE FUNCTION entities_reject_node_access_blob();"""
            )
        )


def _drop_association_triggers(connection):
    dialect_name = connection.engine.dialect.name
    tables = ("node_access_blobs", "entity_access_blobs", "link_access_blobs")
    if dialect_name == "sqlite":
        for table in tables:
            connection.execute(
                sa.text(f"DROP TRIGGER IF EXISTS {table}_delete_cleanup")
            )
            for operation in ("insert", "update"):
                connection.execute(
                    sa.text(
                        f"DROP TRIGGER IF EXISTS {table}_{operation}_reject_shared_access_blob"
                    )
                )
        for operation in ("insert", "update"):
            connection.execute(
                sa.text(
                    f"DROP TRIGGER IF EXISTS entity_access_blobs_{operation}_reject_node_backed_entity"
                )
            )
        for operation in ("insert", "update"):
            connection.execute(
                sa.text(f"DROP TRIGGER IF EXISTS entities_node_access_blob_{operation}")
            )
    elif dialect_name == "postgresql":
        for table in tables:
            connection.execute(
                sa.text(f"DROP TRIGGER IF EXISTS {table}_delete_cleanup ON {table}")
            )
            connection.execute(
                sa.text(
                    f"DROP TRIGGER IF EXISTS {table}_reject_shared_access_blob ON {table}"
                )
            )
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS entity_access_blobs_reject_node_backed_entity ON entity_access_blobs"
            )
        )
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS entities_node_access_blob_check ON entities"
            )
        )
        for function in (
            "delete_orphaned_access_blob",
            "reject_shared_access_blob",
            "entities_reject_node_access_blob",
            "entity_access_blob_reject_node_backed_entity",
        ):
            connection.execute(sa.text(f"DROP FUNCTION IF EXISTS {function}"))


def upgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name
    access_tags_variant = sa.JSON(none_as_null=True).with_variant(
        sa.ARRAY(sa.String()), "postgresql"
    )

    op.create_table(
        "access_blobs",
        sa.Column(
            "id",
            sa.Integer(),
            nullable=False,
            primary_key=True,
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
        "ix_access_blobs_kind_id",
        "access_blobs",
        ["kind", "id"],
    )

    op.create_table(
        "node_access_blobs",
        sa.Column(
            "node_id",
            sa.Integer(),
            sa.ForeignKey("nodes.id", ondelete="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            "access_blob_id",
            sa.Integer(),
            sa.ForeignKey("access_blobs.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
    )
    op.create_table(
        "link_access_blobs",
        sa.Column(
            "link_id",
            sa.String(),
            sa.ForeignKey("links.id", ondelete="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            "access_blob_id",
            sa.Integer(),
            sa.ForeignKey("access_blobs.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
    )
    op.create_table(
        "entity_access_blobs",
        sa.Column(
            "entity_id",
            sa.String(),
            sa.ForeignKey("entities.id", ondelete="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            "access_blob_id",
            sa.Integer(),
            sa.ForeignKey("access_blobs.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
    )
    if dialect_name == "postgresql":
        op.create_index(
            "ix_access_blobs_tags_gin",
            "access_blobs",
            ["tags"],
            postgresql_using="gin",
            postgresql_where=sa.text("kind = 'tags' AND tags IS NOT NULL"),
        )
    _copy_access_blobs(connection, "nodes", "node_access_blobs", "node_id")
    _copy_access_blobs(connection, "links", "link_access_blobs", "link_id")
    _copy_access_blobs(
        connection,
        "entities",
        "entity_access_blobs",
        "entity_id",
        "WHERE access_blob IS NOT NULL",
    )

    # Replace c31's JSON-column delegation checks before dropping that column.
    if dialect_name == "sqlite":
        connection.execute(sa.text("DROP TRIGGER entities_node_access_blob_insert"))
        connection.execute(sa.text("DROP TRIGGER entities_node_access_blob_update"))
    elif dialect_name == "postgresql":
        connection.execute(
            sa.text("DROP TRIGGER entities_node_access_blob_check ON entities")
        )
        connection.execute(sa.text("DROP FUNCTION entities_reject_node_access_blob"))

    with op.batch_alter_table("links") as batch_op:
        batch_op.drop_column("access_blob")

    op.drop_index("top_level_metadata", table_name="nodes")
    with op.batch_alter_table("nodes") as batch_op:
        batch_op.drop_column("access_blob")
    with op.batch_alter_table("entities") as batch_op:
        batch_op.drop_column("access_blob")
    op.create_index(
        "top_level_metadata",
        "nodes",
        ["parent", "time_created", "id", "metadata"],
        postgresql_using="gin",
    )
    _create_association_triggers(connection)


def downgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name

    _drop_association_triggers(connection)

    with op.batch_alter_table("entities") as batch_op:
        batch_op.add_column(sa.Column("access_blob", JSONVariant, nullable=True))
    _restore_access_blobs(
        dialect_name,
        "entity_access_blobs",
        "entity_id",
        "entities",
        default_empty=False,
    )
    op.drop_table("entity_access_blobs")

    # Restore c31's JSON-column delegation checks.
    error_message = (
        "An entity with node_id set must not have its own access_blob; "
        "access is controlled by the referenced node."
    )
    if dialect_name == "sqlite":
        for operation in ("INSERT", "UPDATE"):
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER entities_node_access_blob_{operation.lower()}
BEFORE {operation} ON entities
WHEN NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, '{error_message}');
END"""
                )
            )
    elif dialect_name == "postgresql":
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '{error_message}';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER entities_node_access_blob_check
BEFORE INSERT OR UPDATE ON entities
FOR EACH ROW WHEN (NEW.node_id IS NOT NULL AND NEW.access_blob IS NOT NULL)
EXECUTE FUNCTION entities_reject_node_access_blob();"""
            )
        )

    with op.batch_alter_table("links") as batch_op:
        batch_op.add_column(
            sa.Column("access_blob", JSONVariant, nullable=False, server_default="{}")
        )

    _restore_access_blobs(dialect_name, "link_access_blobs", "link_id", "links")

    op.drop_table("link_access_blobs")

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

    _restore_access_blobs(dialect_name, "node_access_blobs", "node_id", "nodes")

    op.drop_table("node_access_blobs")
    op.drop_table("access_blobs")
    if dialect_name == "postgresql":
        op.execute("DROP TYPE IF EXISTS access_kind")
