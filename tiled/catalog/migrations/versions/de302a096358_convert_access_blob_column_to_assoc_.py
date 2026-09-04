"""Convert node, entity, and link access_blob columns to assoc. tables

Revision ID: de302a096358
Revises: c31f6a1d7e20
Create Date: 2026-07-03 14:27:39.197261

"""
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
    """Convert a JSON ``access_blob`` column into ``access_blobs`` rows plus an
    association row per owner, using set-based SQL so the migration scales to
    very large catalogs.

    Correlation between a newly inserted ``access_blobs`` row and its owner is
    done through a temporary ``_migrate_owner`` column (dropped at the end),
    which avoids needing per-row ``RETURNING``.

    * a blob containing a ``"user"`` key becomes ``kind='user'``;
    * otherwise ``kind='tags'`` with ``tags`` taken from the ``"tags"`` key,
      defaulting to an empty list;
    * the root node (``nodes.id = 0``) with an empty blob or ``tags == []`` is
      promoted to ``{"tags": ["public"]}``.
    """
    dialect_name = connection.engine.dialect.name

    # Add a temporary column to correlate each access_blobs row to its owner.
    owner_type = "INTEGER" if owner_column == "node_id" else "VARCHAR"
    connection.execute(
        sa.text(f"ALTER TABLE access_blobs ADD COLUMN _migrate_owner {owner_type}")
    )

    # Per-dialect JSON extraction. Both branches must yield, per source row:
    #   kind      -> 'user' when a "user" key is present, else 'tags'
    #   username  -> the "user" value when kind='user', else NULL
    #   tags      -> the "tags" array when kind='tags', else NULL
    # honoring the root-node "public" promotion and (for entities) the
    # caller-supplied WHERE clause that skips node-delegated rows.
    root_public = source_table == "nodes"
    if dialect_name == "sqlite":
        # In SQLite the column is TEXT; treat SQL NULL / JSON 'null' as '{}'.
        blob = "COALESCE(NULLIF(access_blob, 'null'), '{}')"
        has_user = f"json_extract({blob}, '$.user') IS NOT NULL"
        user_val = f"json_extract({blob}, '$.user')"
        # json_extract of a missing/absent "tags" yields NULL; default to [].
        tags_val = f"COALESCE(json_extract({blob}, '$.tags'), json('[]'))"
        if root_public:
            # Root node with empty tags or empty blob -> ["public"].
            is_empty_root = (
                f"id = 0 AND ("
                f"json_extract({blob}, '$.user') IS NULL AND ("
                f"json_extract({blob}, '$.tags') IS NULL OR "
                f"json_array_length(json_extract({blob}, '$.tags')) = 0))"
            )
            tags_val = (
                f"CASE WHEN {is_empty_root} THEN json('[\"public\"]') "
                f"ELSE {tags_val} END"
            )
        kind_expr = f"CASE WHEN {has_user} THEN 'user' ELSE 'tags' END"
        username_expr = f"CASE WHEN {has_user} THEN {user_val} ELSE NULL END"
        tags_expr = f"CASE WHEN {has_user} THEN NULL ELSE {tags_val} END"
    elif dialect_name == "postgresql":
        # nodes.access_blob is jsonb but the c31-era entities/links access_blob
        # columns are plain json; cast so COALESCE unifies on jsonb either way.
        blob = "COALESCE(access_blob::jsonb, '{}'::jsonb)"
        has_user = f"({blob} ? 'user')"
        user_val = f"{blob} ->> 'user'"
        # Build a text[] from the JSON "tags" array; default to empty array.
        tags_val = (
            f"COALESCE("
            f"ARRAY(SELECT jsonb_array_elements_text({blob} -> 'tags')), "
            f"ARRAY[]::varchar[])"
        )
        if root_public:
            is_empty_root = (
                f"id = 0 AND NOT ({blob} ? 'user') AND ("
                f"NOT ({blob} ? 'tags') OR "
                f"jsonb_array_length(COALESCE({blob} -> 'tags', '[]'::jsonb)) = 0)"
            )
            tags_val = (
                f"CASE WHEN {is_empty_root} THEN ARRAY['public']::varchar[] "
                f"ELSE {tags_val} END"
            )
        # The CASE yields text; the target column is the access_kind enum and
        # INSERT ... SELECT does not implicitly cast, so cast explicitly.
        kind_expr = f"(CASE WHEN {has_user} THEN 'user' ELSE 'tags' END)::access_kind"
        username_expr = f"CASE WHEN {has_user} THEN {user_val} ELSE NULL END"
        # Likewise cast to the column's varchar[] type: jsonb_array_elements_text
        # produces text[], and mixed CASE/COALESCE branches resolve to text[].
        tags_expr = f"(CASE WHEN {has_user} THEN NULL ELSE {tags_val} END)::varchar[]"
    else:
        raise RuntimeError(f"Unsupported dialect for migration: {dialect_name}")

    connection.execute(
        sa.text(
            f"""
INSERT INTO access_blobs (kind, username, tags, _migrate_owner)
SELECT {kind_expr}, {username_expr}, {tags_expr}, id
FROM {source_table}
{where}
"""
        )
    )
    connection.execute(
        sa.text(
            f"""
INSERT INTO {association_table} ({owner_column}, access_blob_id)
SELECT _migrate_owner, id
FROM access_blobs
WHERE _migrate_owner IS NOT NULL
"""
        )
    )
    # Drop the temporary correlation column now that associations are populated.
    # Use a native ALTER rather than Alembic's batch mode, which would rebuild
    # the access_blobs table and disturb the foreign keys the association tables
    # hold against it.
    # ALTER TABLE ... DROP COLUMN requires SQLite >= 3.35 (2021).
    connection.execute(sa.text("ALTER TABLE access_blobs DROP COLUMN _migrate_owner"))


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
        fallback = "json('{}')" if default_empty else "NULL"
        op.execute(
            f"""
            UPDATE {destination_table}
            SET access_blob = COALESCE(
                (
                    SELECT CASE
                        WHEN access_blobs.kind = 'user' THEN
                            json_object('user', access_blobs.username)
                        ELSE json_object(
                            'tags', json(COALESCE(access_blobs.tags, '[]'))
                        )
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
        "An entity with node_id set must not have its own access_blob; "
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
        # Only guard UPDATE OF node_id: the INSERT case cannot occur in a
        # single statement (an entities INSERT sets only node_id, while the
        # blob lives in a separate entity_access_blobs row guarded by
        # entity_access_blobs_insert_reject_node_backed_entity). This mirrors
        # the PostgreSQL branch and tiled.graph.orm's create_all path.
        connection.execute(
            sa.text(
                f"""
CREATE TRIGGER entities_node_access_blob_update
BEFORE UPDATE OF node_id ON entities
WHEN NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
)
BEGIN
    SELECT RAISE(ABORT, '{error_message}');
END"""
            )
        )
    elif dialect_name == "postgresql":
        # asyncpg cannot run multiple statements in one prepared execute, so
        # each CREATE FUNCTION / CREATE TRIGGER is issued individually.
        # One cleanup function per association table, matching the objects
        # created by the create_all path (node's lives in tiled.catalog.orm,
        # entity's and link's in tiled.graph.orm). Keeping them per-table -
        # rather than a single shared function - keeps each feature's DDL
        # self-contained and avoids graph objects depending on a catalog-owned
        # function (or vice versa).
        for table in tables:
            singular = table.replace("_access_blobs", "")
            connection.execute(
                sa.text(
                    f"""
CREATE OR REPLACE FUNCTION delete_{singular}_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM access_blobs WHERE id = OLD.access_blob_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;"""
                )
            )
        connection.execute(
            sa.text(
                """
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
$$ LANGUAGE plpgsql;"""
            )
        )
        # PostgreSQL does not allow subqueries in a trigger WHEN clause
        # ("cannot use subquery in trigger WHEN condition"), so the EXISTS
        # check against entity_access_blobs lives in the function body; the
        # trigger WHEN clause keeps only the cheap scalar column test.
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
    ) THEN
        RAISE EXCEPTION '{error_message}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            sa.text(
                f"""
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
            singular = table.replace("_access_blobs", "")
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER {table}_delete_cleanup
AFTER DELETE ON {table}
FOR EACH ROW EXECUTE FUNCTION delete_{singular}_access_blob();"""
                )
            )
            connection.execute(
                sa.text(
                    f"""
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
FOR EACH ROW EXECUTE FUNCTION entity_access_blob_reject_node_backed_entity();"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER entities_node_access_blob_check
BEFORE UPDATE OF node_id ON entities
FOR EACH ROW WHEN (NEW.node_id IS NOT NULL)
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
        connection.execute(
            sa.text("DROP TRIGGER IF EXISTS entities_node_access_blob_update")
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
            "delete_node_access_blob",
            "delete_entity_access_blob",
            "delete_link_access_blob",
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
    # The Revision model no longer tracks access_blob; drop the column so that
    # inserting a revision (on metadata update) does not violate its former
    # NOT NULL constraint now that the ORM stops populating it.
    with op.batch_alter_table("revisions") as batch_op:
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

    # Restore the revisions.access_blob column dropped on upgrade. The former
    # data is not recoverable (revisions no longer carried it), so recreate it
    # with the same NOT NULL + server_default('{}') shape as migration
    # a963a6c32a0c, which is what a database at that revision would have.
    with op.batch_alter_table("revisions") as batch_op:
        batch_op.add_column(
            sa.Column("access_blob", JSONVariant, nullable=False, server_default="{}")
        )

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
        # asyncpg cannot run multiple statements in one prepared execute.
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
