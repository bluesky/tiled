"""Demote `Composite` structure_familty to `composite spec`

Revision ID: dfbb7478c6bd
Revises: a963a6c32a0c
Create Date: 2025-08-18 17:56:14.996646

"""
from alembic import op
import sqlalchemy as sa
import json


# revision identifiers, used by Alembic.
revision = 'dfbb7478c6bd'
down_revision = 'a963a6c32a0c'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    # Step 1: Add to `specs` wherever structure_family = 'composite';
    # change structure_family to 'container'
    if connection.engine.dialect.name == "postgresql":
        stmnt = """specs || jsonb_build_array(jsonb_build_object('name', 'composite', 'version', NULL))"""  # noqa: E501
    elif connection.engine.dialect.name == "sqlite":
        stmnt = """jsonb_array(specs, json('[{"name":"composite","version":null}]'))"""
    op.execute(
        f"UPDATE nodes SET specs = {stmnt}, structure_family = 'container' "
        "WHERE structure_family = 'composite'"
    )

    # Step 2. Remove 'composite' structure family from the `structurefamily` enum
    if connection.engine.dialect.name == "postgresql":
        # 2.1. Rename the existing type
        op.execute("ALTER TYPE structurefamily RENAME TO structurefamily_old")

        # 2.2. Get all enum values from the existing type
        values = connection.execute(
            sa.text("SELECT unnest(enum_range(NULL::structurefamily_old))")
        ).fetchall()
        values = [r[0] for r in values if r[0] != "composite"]

        # 2.3. Create the new type (without 'composite')
        op.execute(sa.text(
                "CREATE TYPE structurefamily AS ENUM (" +
                ", ".join(f"'{v}'" for v in values) + ")"
            ))

        # 2.4. Alter columns to use the new type
        for table_name in ["nodes", "data_sources"]:
            op.execute(sa.text(
                f"""
                ALTER TABLE {table_name}
                ALTER COLUMN structure_family TYPE structurefamily
                USING structure_family::text::structurefamily
                """
            ))

        # 2.5. Drop the old type
        op.execute("DROP TYPE structurefamily_old")


def downgrade():
    pass
