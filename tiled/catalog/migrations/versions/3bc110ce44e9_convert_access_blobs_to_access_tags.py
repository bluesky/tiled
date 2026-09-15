"""Convert access blobs to access tags

Revision ID: 3bc110ce44e9
Revises: de302a096358
Create Date: 2026-09-04

Replaces the ``access_blobs`` table (and its per-owner association tables
``node_access_blobs``, ``entity_access_blobs``, ``link_access_blobs``) with
named, deduplicated access tags:

* ``access_tags`` holds one row per distinct tag name. Every owner carrying a
  given tag points at the same deduplicated row. The ``public`` tag is marked
  ``is_public``.
* ``node_access_tags``, ``entity_access_tags``, and ``link_access_tags`` are
  many-to-many association tables (an owner may carry several tags; a tag may
  be carried by many owners).
* A blob of kind ``user`` (a principal-owned node/entity/link) becomes the tag
  ``user:<username>``. The blob world stored service principals the same way
  (``username`` held the service's uuid), and the catalog database has no
  local way to tell the two apart, so they too become ``user:<uuid>`` here.
  The access tags compiler later adds a ``service:<uuid>`` tag definition for
  each service principal (with stable IDs, matching either form) so that data
  migrated as ``user:<uuid>`` and data tagged ``service:<uuid>`` going forward
  both resolve; each owner still carries exactly one principal tag.
* Entities that reference a catalog node (``node_id`` set) had no blob and get
  no tags: they assume the access tags of the referenced node. Database
  triggers enforcing that invariant are recreated in tag form.
* The supporting tables for tag definitions -- ``access_tags_principals``,
  ``scopes``, ``access_tag_principal_scopes``, ``access_tag_owners`` -- are
  created empty. They are populated for the first time by the access tags
  compiler, not by this migration.

The downgrade reconstructs one blob per owner from its tags before dropping
the tag tables: an owner whose only tag is a principal tag (``user:<name>``
or ``service:<uuid>``) becomes a kind ``user`` blob again, with the username
taken from either prefix; anything else becomes a kind ``tags`` blob.
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3bc110ce44e9"
down_revision = "de302a096358"
branch_labels = None
depends_on = None

# The scopes.name column is a native enum (scope_name on PostgreSQL). The
# values are frozen here at this revision's authorship; the live set of valid
# scopes is the ScopeName enum in tiled.access_control.scopes, but migrations
# must not import application code that may change out from under them.
SCOPE_NAME_ENUM = sa.Enum(
    "read:metadata",
    "read:data",
    "write:metadata",
    "write:data",
    "delete:revision",
    "delete:node",
    "create:node",
    "register",
    "metrics",
    "create:apikeys",
    "revoke:apikeys",
    "admin:apikeys",
    "read:principals",
    "write:principals",
    "read:webhooks",
    "write:webhooks",
    name="scope_name",
)

ENTITY_NODE_ACCESS_TAGS_ERROR = (
    "An entity with node_id set must not have its own access tags; "
    "access is controlled by the referenced node."
)

# (owner table, tag association table, owner id column, blob association table)
OWNERS = (
    ("nodes", "node_access_tags", "node_id", "node_access_blobs"),
    ("entities", "entity_access_tags", "entity_id", "entity_access_blobs"),
    ("links", "link_access_tags", "link_id", "link_access_blobs"),
)


# ---------------------------------------------------------------------------
# Schema pieces
# ---------------------------------------------------------------------------


def _create_tag_tables():
    """Create the tag tables and indexes, mirroring tiled.catalog.orm and
    tiled.graph.orm exactly."""
    # Column order matters for parity with create_all: the Timestamped mixin
    # columns come last in the ORM-rendered DDL.
    op.create_table(
        "access_tags",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column(
            "is_public",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("time_created", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("time_updated", sa.DateTime(), server_default=sa.func.now()),
    )
    # Partial + covering, matching the ORM: index-only scan for
    # "SELECT name WHERE is_public", zero maintenance for non-public rows.
    op.create_index(
        "idx_access_tags_is_public",
        "access_tags",
        ["name"],
        postgresql_where=sa.text("is_public"),
        sqlite_where=sa.text("is_public"),
    )

    op.create_table(
        "node_access_tags",
        sa.Column(
            "node_id",
            sa.Integer(),
            sa.ForeignKey(
                "nodes.id", name="fk_node_access_tags_node", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags.id", name="fk_node_access_tags_tag", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("node_id", "tag_id", name="node_access_tags_pkey"),
    )
    op.create_index(
        "idx_node_access_tags_tag_id_node_id",
        "node_access_tags",
        ["tag_id", "node_id"],
    )

    op.create_table(
        "entity_access_tags",
        sa.Column(
            "entity_id",
            sa.String(),
            sa.ForeignKey(
                "entities.id", name="fk_entity_access_tags_entity", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags.id", name="fk_entity_access_tags_tag", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
    )
    op.create_index(
        "idx_entity_access_tags_tag_id_entity_id",
        "entity_access_tags",
        ["tag_id", "entity_id"],
    )

    op.create_table(
        "link_access_tags",
        sa.Column(
            "link_id",
            sa.String(),
            sa.ForeignKey(
                "links.id", name="fk_link_access_tags_link", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags.id", name="fk_link_access_tags_tag", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
    )
    op.create_index(
        "idx_link_access_tags_tag_id_link_id",
        "link_access_tags",
        ["tag_id", "link_id"],
    )

    # Tag-definition support tables. Created empty: they are populated for the
    # first time by the access tags compiler, never by this migration.
    op.create_table(
        "access_tags_principals",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("time_created", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("time_updated", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_table(
        "scopes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", SCOPE_NAME_ENUM, nullable=False, unique=True),
    )
    op.create_table(
        "access_tag_principal_scopes",
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags.id",
                name="fk_access_tag_principal_scopes_access_tag",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column(
            "principal_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags_principals.id",
                name="fk_access_tag_principal_scopes_principal",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column(
            "scope_id",
            sa.Integer(),
            sa.ForeignKey(
                "scopes.id",
                name="fk_access_tag_principal_scopes_scope",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "tag_id",
            "principal_id",
            "scope_id",
            name="access_tag_principal_scopes_pkey",
        ),
    )
    op.create_index(
        "idx_access_tag_principal_scopes_principal_scope",
        "access_tag_principal_scopes",
        ["principal_id", "scope_id", "tag_id"],
    )
    op.create_index(
        "idx_access_tag_principal_scopes_scope_id",
        "access_tag_principal_scopes",
        ["scope_id"],
    )
    op.create_table(
        "access_tag_owners",
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags.id",
                name="fk_access_tag_owners_access_tag",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column(
            "principal_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags_principals.id",
                name="fk_access_tag_owners_principal",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "tag_id", "principal_id", name="access_tag_owners_pkey"
        ),
    )
    op.create_index(
        "idx_access_tag_owners_principal_id",
        "access_tag_owners",
        ["principal_id"],
    )


def _drop_tag_tables():
    """Drop the tag tables, dependents before their referents."""
    op.drop_table("access_tag_principal_scopes")
    op.drop_table("access_tag_owners")
    op.drop_table("access_tags_principals")
    op.drop_table("scopes")
    op.drop_table("node_access_tags")
    op.drop_table("entity_access_tags")
    op.drop_table("link_access_tags")
    op.drop_table("access_tags")


# ---------------------------------------------------------------------------
# Data: blobs -> tags (upgrade)
# ---------------------------------------------------------------------------


def _populate_tags_from_blobs(connection):
    """
    Populate access_tags with one row per distinct tag name across all blobs
    (deduplicated), then point each owner's association rows at those shared
    rows. Blobs of kind 'user' become 'user:<username>' tags.
    """
    dialect_name = connection.engine.dialect.name
    if dialect_name == "sqlite":
        # Deduplicated tag names: UNION removes duplicates across and within
        # sources. json_each explodes the JSON array of kind='tags' blobs.
        connection.execute(
            sa.text(
                """
INSERT INTO access_tags (name, is_public)
SELECT tag_name, (tag_name = 'public') FROM (
    SELECT je.value AS tag_name
    FROM access_blobs ab, json_each(ab.tags) je
    WHERE ab.kind = 'tags'
    UNION
    SELECT 'user:' || ab.username AS tag_name
    FROM access_blobs ab
    WHERE ab.kind = 'user'
)
"""
            )
        )
        for _, tag_assoc, owner_column, blob_assoc in OWNERS:
            # DISTINCT guards against duplicate names within one blob's array,
            # which would violate the association's composite primary key.
            connection.execute(
                sa.text(
                    f"""
INSERT INTO {tag_assoc} ({owner_column}, tag_id)
SELECT DISTINCT assoc.{owner_column}, at.id
FROM {blob_assoc} assoc
JOIN access_blobs ab ON ab.id = assoc.access_blob_id, json_each(ab.tags) je
JOIN access_tags at ON at.name = je.value
WHERE ab.kind = 'tags'
"""
                )
            )
            connection.execute(
                sa.text(
                    f"""
INSERT INTO {tag_assoc} ({owner_column}, tag_id)
SELECT assoc.{owner_column}, at.id
FROM {blob_assoc} assoc
JOIN access_blobs ab ON ab.id = assoc.access_blob_id
JOIN access_tags at ON at.name = 'user:' || ab.username
WHERE ab.kind = 'user'
"""
                )
            )
    elif dialect_name == "postgresql":
        connection.execute(
            sa.text(
                """
INSERT INTO access_tags (name, is_public)
SELECT tag_name, (tag_name = 'public') FROM (
    SELECT unnest(ab.tags) AS tag_name
    FROM access_blobs ab
    WHERE ab.kind = 'tags'
    UNION
    SELECT 'user:' || ab.username AS tag_name
    FROM access_blobs ab
    WHERE ab.kind = 'user'
) tag_names
"""
            )
        )
        for _, tag_assoc, owner_column, blob_assoc in OWNERS:
            connection.execute(
                sa.text(
                    f"""
INSERT INTO {tag_assoc} ({owner_column}, tag_id)
SELECT DISTINCT assoc.{owner_column}, at.id
FROM {blob_assoc} assoc
JOIN access_blobs ab ON ab.id = assoc.access_blob_id
CROSS JOIN LATERAL unnest(ab.tags) AS tag_name
JOIN access_tags at ON at.name = tag_name
WHERE ab.kind = 'tags'
"""
                )
            )
            connection.execute(
                sa.text(
                    f"""
INSERT INTO {tag_assoc} ({owner_column}, tag_id)
SELECT assoc.{owner_column}, at.id
FROM {blob_assoc} assoc
JOIN access_blobs ab ON ab.id = assoc.access_blob_id
JOIN access_tags at ON at.name = 'user:' || ab.username
WHERE ab.kind = 'user'
"""
                )
            )
    else:
        raise RuntimeError(f"Unsupported dialect for migration: {dialect_name}")


# ---------------------------------------------------------------------------
# Data: tags -> blobs (downgrade)
# ---------------------------------------------------------------------------


def _reconstruct_blobs_from_tags(
    connection, owner_table, tag_assoc, owner_column, blob_assoc, where=""
):
    """
    Reconstruct one access_blobs row per owner from its tags, restoring the
    blob world's 1:1 owner->blob shape:

    * an owner whose only tag is a principal tag -- 'user:<username>' or
      'service:<uuid>' (the access policy applies exactly one, never both) --
      -> kind='user' blob, with the username taken from whichever prefix is
      present (the blob world stored service principals as kind='user' with
      the uuid in the username column);
    * anything else (including zero tags) -> kind='tags' blob with the
      aggregated tag names ('[]' for untagged owners).

    Correlation between a newly inserted access_blobs row and its owner is
    done through a temporary _migrate_owner column (dropped at the end),
    following the pattern of revision de302a096358.
    """
    dialect_name = connection.engine.dialect.name
    owner_type = "INTEGER" if owner_column == "node_id" else "VARCHAR"
    connection.execute(
        sa.text(f"ALTER TABLE access_blobs ADD COLUMN _migrate_owner {owner_type}")
    )
    # 'user:' is 5 characters and 'service:' is 8, so the identifier starts at
    # (1-indexed) position 6 and 9 respectively. substr() is available on both
    # SQLite and PostgreSQL.
    is_user_blob = "agg.n_tags = 1 AND agg.n_user + agg.n_service = 1"
    if dialect_name == "sqlite":
        empty_tags = "json('[]')"
        tags_agg = (
            "CASE WHEN COUNT(t.id) = 0 THEN json('[]') "
            "ELSE json_group_array(t.name) END"
        )
        kind_expr = f"CASE WHEN {is_user_blob} THEN 'user' ELSE 'tags' END"
    elif dialect_name == "postgresql":
        empty_tags = "ARRAY[]::varchar[]"
        tags_agg = (
            "CASE WHEN COUNT(t.id) = 0 THEN ARRAY[]::varchar[] "
            "ELSE array_agg(t.name)::varchar[] END"
        )
        kind_expr = (
            f"(CASE WHEN {is_user_blob} THEN 'user' ELSE 'tags' END)::access_kind"
        )
    else:
        raise RuntimeError(f"Unsupported dialect for migration: {dialect_name}")

    connection.execute(
        sa.text(
            f"""
INSERT INTO access_blobs (kind, username, tags, _migrate_owner)
SELECT
    {kind_expr},
    CASE WHEN {is_user_blob}
         THEN COALESCE(agg.user_name, agg.service_name) ELSE NULL END,
    CASE WHEN {is_user_blob} THEN NULL ELSE COALESCE(agg.tag_names, {empty_tags}) END,
    agg.owner
FROM (
    SELECT
        owner_table.id AS owner,
        COUNT(t.id) AS n_tags,
        COALESCE(
            SUM(CASE WHEN t.name LIKE 'user:%' THEN 1 ELSE 0 END), 0
        ) AS n_user,
        COALESCE(
            SUM(CASE WHEN t.name LIKE 'service:%' THEN 1 ELSE 0 END), 0
        ) AS n_service,
        MAX(CASE WHEN t.name LIKE 'user:%'
                 THEN substr(t.name, 6) END) AS user_name,
        MAX(CASE WHEN t.name LIKE 'service:%'
                 THEN substr(t.name, 9) END) AS service_name,
        {tags_agg} AS tag_names
    FROM {owner_table} owner_table
    LEFT JOIN {tag_assoc} assoc ON assoc.{owner_column} = owner_table.id
    LEFT JOIN access_tags t ON t.id = assoc.tag_id
    {where}
    GROUP BY owner_table.id
) agg
"""
        )
    )
    connection.execute(
        sa.text(
            f"""
INSERT INTO {blob_assoc} ({owner_column}, access_blob_id)
SELECT _migrate_owner, id
FROM access_blobs
WHERE _migrate_owner IS NOT NULL
"""
        )
    )
    # Use a native ALTER rather than Alembic's batch mode, which would rebuild
    # the access_blobs table and disturb the foreign keys the association
    # tables hold against it (see de302a096358).
    connection.execute(sa.text("ALTER TABLE access_blobs DROP COLUMN _migrate_owner"))


# ---------------------------------------------------------------------------
# Triggers: node-backed entities must not carry their own access tags
# ---------------------------------------------------------------------------


def _create_entity_tag_triggers(connection):
    """Create the delegation-invariant triggers, mirroring tiled.graph.orm."""
    dialect_name = connection.engine.dialect.name
    if dialect_name == "sqlite":
        connection.execute(
            sa.text(
                f"""
CREATE TRIGGER entities_node_access_tags_update
BEFORE UPDATE OF node_id ON entities
WHEN (NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_tags WHERE entity_id = NEW.id
))
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_TAGS_ERROR}');
END"""
            )
        )
        for operation in ("INSERT", "UPDATE OF entity_id"):
            connection.execute(
                sa.text(
                    f"""
CREATE TRIGGER entity_access_tags_{operation.split()[0].lower()}_reject_node_backed_entity
BEFORE {operation} ON entity_access_tags
WHEN EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_TAGS_ERROR}');
END"""
                )
            )
    elif dialect_name == "postgresql":
        # asyncpg cannot run multiple statements in one prepared execute, so
        # each CREATE FUNCTION / CREATE TRIGGER is issued individually.
        # PostgreSQL does not allow subqueries in a trigger WHEN clause, so
        # the EXISTS checks live in the function bodies.
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_tags()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM entity_access_tags WHERE entity_id = NEW.id
    ) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_TAGS_ERROR}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER entities_node_access_tags_check
BEFORE UPDATE OF node_id ON entities
FOR EACH ROW
WHEN (NEW.node_id IS NOT NULL)
EXECUTE FUNCTION entities_reject_node_access_tags();"""
            )
        )
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION entity_access_tags_reject_node_backed_entity()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM entities WHERE id = NEW.entity_id AND node_id IS NOT NULL) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_TAGS_ERROR}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER entity_access_tags_reject_node_backed_entity
BEFORE INSERT OR UPDATE OF entity_id ON entity_access_tags
FOR EACH ROW EXECUTE FUNCTION entity_access_tags_reject_node_backed_entity();"""
            )
        )


def _drop_entity_tag_triggers(connection):
    dialect_name = connection.engine.dialect.name
    if dialect_name == "sqlite":
        connection.execute(
            sa.text("DROP TRIGGER IF EXISTS entities_node_access_tags_update")
        )
        for operation in ("insert", "update"):
            connection.execute(
                sa.text(
                    f"DROP TRIGGER IF EXISTS entity_access_tags_{operation}_reject_node_backed_entity"
                )
            )
    elif dialect_name == "postgresql":
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS entities_node_access_tags_check ON entities"
            )
        )
        connection.execute(
            sa.text(
                "DROP TRIGGER IF EXISTS entity_access_tags_reject_node_backed_entity "
                "ON entity_access_tags"
            )
        )
        for function in (
            "entities_reject_node_access_tags",
            "entity_access_tags_reject_node_backed_entity",
        ):
            connection.execute(sa.text(f"DROP FUNCTION IF EXISTS {function}"))


# ---------------------------------------------------------------------------
# Blob-era triggers (recreated on downgrade; dropped on upgrade). These are
# copied verbatim from revision de302a096358 so that a downgraded database is
# indistinguishable from one sitting at that revision.
# ---------------------------------------------------------------------------

BLOB_ASSOC_TABLES = ("node_access_blobs", "entity_access_blobs", "link_access_blobs")
ENTITY_NODE_ACCESS_BLOB_ERROR = (
    "An entity with node_id set must not have its own access_blob; "
    "access is controlled by the referenced node."
)


def _create_blob_triggers(connection):
    dialect_name = connection.engine.dialect.name
    if dialect_name == "sqlite":
        for table in BLOB_ASSOC_TABLES:
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
                for other in BLOB_ASSOC_TABLES
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
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_BLOB_ERROR}');
END"""
                )
            )
        connection.execute(
            sa.text(
                f"""
CREATE TRIGGER entities_node_access_blob_update
BEFORE UPDATE OF node_id ON entities
WHEN NEW.node_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
)
BEGIN
    SELECT RAISE(ABORT, '{ENTITY_NODE_ACCESS_BLOB_ERROR}');
END"""
            )
        )
    elif dialect_name == "postgresql":
        for table in BLOB_ASSOC_TABLES:
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
        connection.execute(
            sa.text(
                f"""
CREATE OR REPLACE FUNCTION entities_reject_node_access_blob()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM entity_access_blobs WHERE entity_id = NEW.id
    ) THEN
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_BLOB_ERROR}';
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
        RAISE EXCEPTION '{ENTITY_NODE_ACCESS_BLOB_ERROR}';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        for table in BLOB_ASSOC_TABLES:
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


def _drop_blob_triggers(connection):
    dialect_name = connection.engine.dialect.name
    if dialect_name == "sqlite":
        for table in BLOB_ASSOC_TABLES:
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
        for table in BLOB_ASSOC_TABLES:
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
                "DROP TRIGGER IF EXISTS entity_access_blobs_reject_node_backed_entity "
                "ON entity_access_blobs"
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


# ---------------------------------------------------------------------------
# upgrade / downgrade
# ---------------------------------------------------------------------------


def upgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name

    _create_tag_tables()
    _populate_tags_from_blobs(connection)

    # The blob-era delete-cleanup triggers on the association tables would
    # cascade into access_blobs while the association tables are dropped;
    # remove all blob triggers (and, on PostgreSQL, their functions) first.
    _drop_blob_triggers(connection)
    op.drop_table("node_access_blobs")
    op.drop_table("entity_access_blobs")
    op.drop_table("link_access_blobs")
    # Dropping access_blobs also drops its indexes (ix_access_blobs_username_user,
    # ix_access_blobs_kind_id, and on PostgreSQL ix_access_blobs_tags_gin).
    op.drop_table("access_blobs")
    if dialect_name == "postgresql":
        op.execute("DROP TYPE IF EXISTS access_kind")

    _create_entity_tag_triggers(connection)


def downgrade():
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name

    _drop_entity_tag_triggers(connection)

    # Recreate the blob tables exactly as revision de302a096358 created them.
    access_tags_variant = sa.JSON(none_as_null=True).with_variant(
        sa.ARRAY(sa.String()), "postgresql"
    )
    op.create_table(
        "access_blobs",
        sa.Column("id", sa.Integer(), nullable=False, primary_key=True),
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
    op.create_index("ix_access_blobs_kind_id", "access_blobs", ["kind", "id"])
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

    # Reconstruct blobs before dropping the tag tables. Every node and link
    # owns a blob in the blob world; entities only when standalone (an entity
    # with node_id set delegates access control to the referenced node).
    _reconstruct_blobs_from_tags(
        connection, "nodes", "node_access_tags", "node_id", "node_access_blobs"
    )
    _reconstruct_blobs_from_tags(
        connection,
        "entities",
        "entity_access_tags",
        "entity_id",
        "entity_access_blobs",
        where="WHERE owner_table.node_id IS NULL",
    )
    _reconstruct_blobs_from_tags(
        connection, "links", "link_access_tags", "link_id", "link_access_blobs"
    )

    _create_blob_triggers(connection)

    _drop_tag_tables()
    if dialect_name == "postgresql":
        op.execute("DROP TYPE IF EXISTS scope_name")
