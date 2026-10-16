"""Rename the access-tag principal scope table to access_grants.

Revision ID: 917dbbcfc4b1
Revises: 3e87cbeb195b
Create Date: 2026-10-08 20:48:01.653907

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "917dbbcfc4b1"
down_revision = "3e87cbeb195b"
branch_labels = None
depends_on = None

OLD_NAMES = {
    "table": "access_tag_principal_scopes_association",
    "pkey": "access_tag_principal_scopes_association_pkey",
    "fk_tag": "fk_access_tag_principal_scopes_association_access_tag",
    "fk_principal": "fk_access_tag_principal_scopes_association_principal",
    "index": "ix_access_tag_principal_scopes_association_principal_scope",
}
NEW_NAMES = {
    "table": "access_grants",
    "pkey": "access_grants_pkey",
    "fk_tag": "fk_access_grants_access_tag",
    "fk_principal": "fk_access_grants_principal",
    "index": "ix_access_grants_principal_scope",
}

COLUMNS = ("tag_id", "principal_id", "scope")
INDEX_COLUMNS = ["principal_id", "scope", "tag_id"]

# The scope set enforced by the SQLite CHECK constraint, frozen as of this
# revision (unchanged since 3bc110ce44e9). Migrations must not import the live
# ScopeName enum, which may change out from under them.
SCOPE_NAMES = (
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
)


def _rename_postgresql(src, dst):
    # PostgreSQL renames these in place, without rewriting the table or its
    # indexes. Renaming the primary key constraint also renames its index.
    op.rename_table(src["table"], dst["table"])
    for key in ("pkey", "fk_tag", "fk_principal"):
        op.execute(
            f"ALTER TABLE {dst['table']} RENAME CONSTRAINT {src[key]} TO {dst[key]}"
        )
    op.execute(f"ALTER INDEX {src['index']} RENAME TO {dst['index']}")


def _rebuild_sqlite(src, dst):
    # SQLite's ALTER TABLE ... RENAME TO keeps constraint names verbatim in the
    # stored CREATE TABLE statement, and it has no way to rename constraints or
    # indexes. A bare rename would therefore leave a renamed table carrying the
    # old constraint names, diverging from a database created fresh from the
    # ORM and breaking any later migration that refers to them by name. So the
    # table is rebuilt under its new name, mirroring tiled.catalog.orm exactly.
    # No other table references this one, so dropping the source is safe.
    op.create_table(
        dst["table"],
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey("access_tags.id", name=dst["fk_tag"], ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "principal_id",
            sa.Integer(),
            sa.ForeignKey(
                "access_tags_principals.id",
                name=dst["fk_principal"],
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column(
            "scope",
            sa.Enum(*SCOPE_NAMES, name="scope_name", create_constraint=True),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(*COLUMNS, name=dst["pkey"]),
    )
    columns = ", ".join(COLUMNS)
    op.execute(
        f"INSERT INTO {dst['table']} ({columns}) "
        f"SELECT {columns} FROM {src['table']}"
    )
    # Dropping the table drops its index too.
    op.drop_table(src["table"])
    # Built after the copy so the bulk insert does not maintain it per row.
    op.create_index(dst["index"], dst["table"], INDEX_COLUMNS)


def _migrate(src, dst):
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _rename_postgresql(src, dst)
    elif dialect_name == "sqlite":
        _rebuild_sqlite(src, dst)
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect_name}")


def upgrade():
    _migrate(OLD_NAMES, NEW_NAMES)


def downgrade():
    _migrate(NEW_NAMES, OLD_NAMES)
