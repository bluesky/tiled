"""Reconcile the schema of migrated databases with the ORM.

Revision ID: bfbcd5d137d0
Revises: 917dbbcfc4b1
Create Date: 2026-10-27 16:10:00.000000

A database created by `initialize_database` (`create_all` from the ORM) and
one brought to the same revision by migrations differ in a few places where
an earlier migration did not match the ORM. Every step below is conditional
on the current state of the database, so it is a no-op where the schema
already matches, including on a fresh database.

Only the differences with no effect on behavior are left as they are: on
SQLite, `assets.size` is declared `INTEGER` rather than `BIGINT` (the two are
the same 64-bit storage class there).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bfbcd5d137d0"
down_revision = "917dbbcfc4b1"
branch_labels = None
depends_on = None

# Foreign keys that the ORM declares with ON DELETE CASCADE, by table. On
# SQLite they were created without it (`nodes`, `nodes_closure`: by the batch
# operations of e05e918092c3) or with ON DELETE SET NULL (`entities`: by
# c31f6a1d7e20, on PostgreSQL as well). Recursive deletion of nodes relies on
# these cascades.
CASCADING_FOREIGN_KEYS = {
    "nodes": [("parent", "nodes", "id")],
    "nodes_closure": [("ancestor", "nodes", "id"), ("descendant", "nodes", "id")],
    "entities": [("node_id", "nodes", "id")],
}


def _foreign_key(inspector, table, column):
    for fk in inspector.get_foreign_keys(table):
        if fk["constrained_columns"] == [column]:
            return fk
    return None


def _rename_index(inspector, dialect, table, old_name, new_name, columns):
    names = {index["name"] for index in inspector.get_indexes(table)}
    if old_name not in names:
        return
    if new_name in names:
        op.drop_index(old_name, table_name=table)
    elif dialect == "postgresql":
        op.execute(f"ALTER INDEX {old_name} RENAME TO {new_name}")
    else:
        op.drop_index(old_name, table_name=table)
        op.create_index(new_name, table, columns)


def _sqlite_needs_rebuild(inspector, table):
    "Whether any foreign key of `table` in CASCADING_FOREIGN_KEYS does not cascade."
    for column, _, _ in CASCADING_FOREIGN_KEYS[table]:
        fk = _foreign_key(inspector, table, column)
        if fk is None or fk["options"].get("ondelete") != "CASCADE":
            return True
    return False


def _sqlite_rebuild_with_cascading_foreign_keys(inspector, table):
    """
    Recreate `table` so that its foreign keys in CASCADING_FOREIGN_KEYS cascade.

    SQLite cannot alter a constraint in place, so the table is rebuilt (alembic
    "batch mode"). The caller has dropped all triggers, and recreates them
    afterwards: those on this table would be lost with it, and SQLite refuses
    to rename the rebuilt table while triggers on other tables refer to the
    table that was dropped.
    """
    # Foreign keys created without a name get one from this convention during
    # reflection, so that they can be dropped.
    naming_convention = {
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s"
    }
    with op.batch_alter_table(
        table, recreate="always", naming_convention=naming_convention
    ) as batch_op:
        for column, referred_table, referred_column in CASCADING_FOREIGN_KEYS[table]:
            fk = _foreign_key(inspector, table, column)
            name = f"fk_{table}_{column}_{referred_table}"
            if fk is not None:
                batch_op.drop_constraint(fk["name"] or name, type_="foreignkey")
            batch_op.create_foreign_key(
                name, referred_table, [column], [referred_column], ondelete="CASCADE"
            )
        if table == "nodes_closure":
            # The primary key on (ancestor, descendant) makes this redundant.
            for constraint in inspector.get_unique_constraints(table):
                if set(constraint["column_names"]) == {"ancestor", "descendant"}:
                    batch_op.drop_constraint(constraint["name"], type_="unique")


def _sqlite_rebuild_tables(connection, inspector):
    tables = [t for t in CASCADING_FOREIGN_KEYS if _sqlite_needs_rebuild(inspector, t)]
    if not tables:
        return
    # Alembic's connection does not enable foreign key enforcement; the
    # server's connection pool does. Rebuilding (which drops the old table)
    # under enforcement would cascade into the referencing tables.
    (foreign_keys_enabled,) = connection.execute(sa.text("PRAGMA foreign_keys")).one()
    if foreign_keys_enabled:
        raise RuntimeError("This migration requires PRAGMA foreign_keys=OFF.")
    triggers = connection.execute(
        sa.text("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")
    ).all()
    for name, _ in triggers:
        connection.execute(sa.text(f"DROP TRIGGER {name}"))
    for table in tables:
        _sqlite_rebuild_with_cascading_foreign_keys(inspector, table)
    for _, trigger_sql in triggers:
        connection.execute(sa.text(trigger_sql))


def upgrade():
    connection = op.get_bind()
    dialect = connection.engine.dialect.name
    inspector = sa.inspect(connection)

    # Migration a86a48befff6 named this index idx_data_sources_node_id.
    _rename_index(
        inspector,
        dialect,
        "data_sources",
        "idx_data_sources_node_id",
        "ix_data_sources_node_id",
        ["node_id"],
    )

    if dialect == "postgresql":
        fk = _foreign_key(inspector, "entities", "node_id")
        if fk is not None and fk["options"].get("ondelete") != "CASCADE":
            op.drop_constraint(fk["name"], "entities", type_="foreignkey")
            op.create_foreign_key(
                fk["name"], "entities", "nodes", ["node_id"], ["id"], ondelete="CASCADE"
            )
        # Migration a66028395cab recreated this table with nullable columns.
        for column in inspector.get_columns("data_source_asset_association"):
            if column["name"] in ("data_source_id", "asset_id") and column["nullable"]:
                op.alter_column(
                    "data_source_asset_association",
                    column["name"],
                    existing_type=sa.Integer(),
                    nullable=False,
                )
    else:
        # A batch operation in e05e918092c3 left this duplicate of
        # ix_nodes_parent, named after alembic's temporary table.
        if "ix__alembic_tmp_nodes_parent" in {
            index["name"] for index in inspector.get_indexes("nodes")
        }:
            op.drop_index("ix__alembic_tmp_nodes_parent", table_name="nodes")
        _sqlite_rebuild_tables(connection, inspector)


def downgrade():
    # The schema this migration produces is the one the ORM has declared all
    # along, and it is valid at the previous revision; there is nothing to restore.
    pass
