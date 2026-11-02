"""Add ON DELETE SET NULL to pending_sessions.session_id.

Deleting a Session (e.g. purging an expired one) must not be blocked by a
pending session that still references it: the reference is nulled instead.

Revision ID: 0a017b76d0d0
Revises: 2d1b550e12e0
Create Date: 2026-10-14 13:22:43.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0a017b76d0d0"
down_revision = "2d1b550e12e0"
branch_labels = None
depends_on = None

# The foreign key was created without an explicit name, so PostgreSQL named it
# by its default convention. A database created fresh by the ORM's create_all
# carries the same name.
PG_FK_NAME = "pending_sessions_session_id_fkey"


def _pg_fk_name(connection):
    # Discover the actual constraint name, in case it differs from the default.
    inspector = sa.inspect(connection)
    for fk in inspector.get_foreign_keys("pending_sessions"):
        if fk["constrained_columns"] == ["session_id"]:
            return fk["name"] or PG_FK_NAME
    raise RuntimeError("Could not find the foreign key on pending_sessions.session_id.")


def _rebuild_sqlite(ondelete):
    # SQLite cannot alter a foreign key in place, so rebuild the table. The
    # schema below is frozen as of this revision and mirrors what the ORM's
    # create_all emits (unnamed constraints), so that migrated and fresh
    # databases carry identical DDL. No other table references this one, so
    # dropping it is safe. Pending sessions are short-lived bookkeeping for
    # the device-code flow, so the copied data is small.
    op.create_table(
        "_tmp_pending_sessions",
        sa.Column("hashed_device_code", sa.LargeBinary(32), nullable=False),
        sa.Column("user_code", sa.Unicode(8), nullable=False),
        sa.Column("expiration_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "session_id",
            sa.Integer(),
            sa.ForeignKey("sessions.id", ondelete=ondelete),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("hashed_device_code"),
    )
    op.execute(
        "INSERT INTO _tmp_pending_sessions "
        "(hashed_device_code, user_code, expiration_time, session_id) "
        "SELECT hashed_device_code, user_code, expiration_time, session_id "
        "FROM pending_sessions"
    )
    # Dropping the table drops its indexes too.
    op.drop_table("pending_sessions")
    op.rename_table("_tmp_pending_sessions", "pending_sessions")
    # Recreated after the copy and rename so the bulk insert does not maintain
    # them per row, and so they carry the names the ORM would give them.
    op.create_index(
        "ix_pending_sessions_hashed_device_code",
        "pending_sessions",
        ["hashed_device_code"],
    )
    op.create_index("ix_pending_sessions_user_code", "pending_sessions", ["user_code"])


def _migrate(ondelete):
    connection = op.get_bind()
    dialect_name = connection.dialect.name
    if dialect_name == "postgresql":
        fk_name = _pg_fk_name(connection)
        op.drop_constraint(fk_name, "pending_sessions", type_="foreignkey")
        op.create_foreign_key(
            fk_name,
            "pending_sessions",
            "sessions",
            ["session_id"],
            ["id"],
            ondelete=ondelete,
        )
    elif dialect_name == "sqlite":
        _rebuild_sqlite(ondelete)
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect_name}")


def upgrade():
    _migrate("SET NULL")


def downgrade():
    _migrate(None)
