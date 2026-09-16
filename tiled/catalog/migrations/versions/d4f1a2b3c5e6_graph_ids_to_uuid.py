"""Convert graph entity/link id columns to native UUID

Revision ID: d4f1a2b3c5e6
Revises: 7f2a1c9e4b3d
Create Date: 2026-08-27 00:00:02.000000

Store the experimental graph's `entities.id` and `links.id` primary keys (and
the `links.subject_id` / `links.object_id` foreign keys) using PostgreSQL's
native `UUID` type: smaller and faster to index on the primary backend.

On SQLite (and any other backend) these ids are the canonical hyphenated string,
which the `GUID` type represents as `CHAR(36)`, so this migration is a no-op
there and requires no data rewrite.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d4f1a2b3c5e6"
down_revision = "7f2a1c9e4b3d"
branch_labels = None
depends_on = None

# PostgreSQL auto-names the (unnamed) foreign keys created in
# c31f6a1d7e20 as "<table>_<column>_fkey".
_SUBJECT_FK = "links_subject_id_fkey"
_OBJECT_FK = "links_object_id_fkey"


def upgrade():
    if op.get_bind().dialect.name != "postgresql":
        # SQLite et al. already store these ids as the canonical hyphenated
        # string, which is exactly what the GUID type continues to use there
        # (CHAR(36)); nothing to convert.
        return
    # Drop the FKs so the referenced/referencing column types can change, then
    # cast all four columns to uuid and re-create the FKs.
    op.drop_constraint(_SUBJECT_FK, "links", type_="foreignkey")
    op.drop_constraint(_OBJECT_FK, "links", type_="foreignkey")
    op.execute("ALTER TABLE entities ALTER COLUMN id TYPE uuid USING id::uuid")
    op.execute("ALTER TABLE links ALTER COLUMN id TYPE uuid USING id::uuid")
    op.execute(
        "ALTER TABLE links ALTER COLUMN subject_id TYPE uuid USING subject_id::uuid"
    )
    op.execute(
        "ALTER TABLE links ALTER COLUMN object_id TYPE uuid USING object_id::uuid"
    )
    op.create_foreign_key(
        _SUBJECT_FK, "links", "entities", ["subject_id"], ["id"], ondelete="CASCADE"
    )
    op.create_foreign_key(
        _OBJECT_FK, "links", "entities", ["object_id"], ["id"], ondelete="CASCADE"
    )


def downgrade():
    if op.get_bind().dialect.name != "postgresql":
        return
    op.drop_constraint(_SUBJECT_FK, "links", type_="foreignkey")
    op.drop_constraint(_OBJECT_FK, "links", type_="foreignkey")
    op.execute("ALTER TABLE entities ALTER COLUMN id TYPE varchar USING id::text")
    op.execute("ALTER TABLE links ALTER COLUMN id TYPE varchar USING id::text")
    op.execute(
        "ALTER TABLE links ALTER COLUMN subject_id TYPE varchar USING subject_id::text"
    )
    op.execute(
        "ALTER TABLE links ALTER COLUMN object_id TYPE varchar USING object_id::text"
    )
    op.create_foreign_key(
        _SUBJECT_FK, "links", "entities", ["subject_id"], ["id"], ondelete="CASCADE"
    )
    op.create_foreign_key(
        _OBJECT_FK, "links", "entities", ["object_id"], ["id"], ondelete="CASCADE"
    )
