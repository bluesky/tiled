"""Demote `Composite` structure_familty to `composite spec`

Revision ID: dfbb7478c6bd
Revises: a963a6c32a0c
Create Date: 2025-08-18 17:56:14.996646

"""
import json
from dataclasses import asdict

import sqlalchemy as sa
from alembic import op

from tiled.structures.core import Spec

# revision identifiers, used by Alembic.
revision = "dfbb7478c6bd"
down_revision = "a963a6c32a0c"
branch_labels = None
depends_on = None

# Representations of Spec objects as JSON strings
json_spec_composite = json.dumps(asdict(Spec("composite")))
json_spec_flattened = json.dumps(asdict(Spec("flattened")))


def upgrade():
    connection = op.get_bind()

    # Step 1: Update the node wherever structure_family = 'composite':
    # - Add 'composite' spec to specs, in the end
    # - Change structure_family to 'container'
    if connection.engine.dialect.name == "postgresql":
        json_stmnt = f"specs || '[{json_spec_composite}]'::jsonb"
    elif connection.engine.dialect.name == "sqlite":
        json_stmnt = f"json_insert(specs, '$[#]', json('{json_spec_composite}'))"
    op.execute(
        f"""
        UPDATE nodes SET specs = {json_stmnt},
            structure_family = 'container'
        WHERE structure_family = 'composite'
        """
    )

    # Step 2. Update the data_sources table (usually unnecessary)
    op.execute(
        """
        UPDATE data_sources
        SET structure_family = 'container'
        WHERE structure_family = 'composite'
        """
    )

    # Step 3. Remove 'flattened' spec used for tables in Composite nodes
    if connection.engine.dialect.name == "postgresql":
        op.execute(
            f"""
            UPDATE nodes
            SET specs = (
                SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                FROM jsonb_array_elements(specs) AS arr(elem)
                WHERE elem != '{json_spec_flattened}'::jsonb
            )
            WHERE specs @> '[{json_spec_flattened}]'::jsonb
            """
        )
    elif connection.engine.dialect.name == "sqlite":
        op.execute(
            f"""
            UPDATE nodes
            SET specs = (
                SELECT json_group_array(value)
                FROM json_each(specs)
                WHERE value != json('{json_spec_flattened}')
            )
            WHERE EXISTS (
                SELECT 1
                FROM json_each(specs)
                WHERE value = json('{json_spec_flattened}')
            )
            """
        )

    # Step 4. Remove 'composite' structure family from the `structurefamily` enum
    if connection.engine.dialect.name == "postgresql":
        # 4.1. Rename the existing type
        op.execute("ALTER TYPE structurefamily RENAME TO structurefamily_old")

        # 4.2. Get all enum values from the existing type
        values = connection.execute(
            sa.text("SELECT unnest(enum_range(NULL::structurefamily_old))")
        ).fetchall()
        values = [r[0] for r in values if r[0] != "composite"]

        # 4.3. Create the new type (without 'composite')
        op.execute(
            sa.text(
                "CREATE TYPE structurefamily AS ENUM ("
                + ", ".join(f"'{v}'" for v in values)
                + ")"
            )
        )

        # 4.4. Alter columns to use the new type
        for table_name in ["nodes", "data_sources"]:
            op.execute(
                f"""
                ALTER TABLE {table_name}
                ALTER COLUMN structure_family TYPE structurefamily
                USING structure_family::text::structurefamily
                """
            )

        # 4.5. Drop the old type
        op.execute("DROP TYPE structurefamily_old")


def downgrade():
    connection = op.get_bind()

    # Step 1: Re-add 'composite' to the enum
    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                """
            ALTER TYPE structurefamily
            ADD VALUE IF NOT EXISTS 'composite' AFTER 'awkward'
            """
            )

    # Step 2: Revert data changes
    # Look for rows whose specs include the 'composite' spec
    json_spec_composite = json.dumps(asdict(Spec("composite")))

    if connection.engine.dialect.name == "postgresql":
        # Revert structure_family
        op.execute(
            f"""
            UPDATE nodes SET structure_family = 'composite'
            WHERE specs @> '[{json_spec_composite}]'::jsonb
            """
        )
        # Remove the 'composite' entry from specs
        op.execute(
            f"""
            UPDATE nodes
            SET specs = (
                SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                FROM jsonb_array_elements(specs) AS arr(elem)
                WHERE elem != '{json_spec_composite}'::jsonb
            )
            WHERE specs @> '[{json_spec_composite}]'::jsonb
            """
        )

    elif connection.engine.dialect.name == "sqlite":
        # Revert structure_family
        op.execute(
            f"""
            UPDATE nodes
            SET structure_family = 'composite'
            WHERE EXISTS (
                SELECT 1 FROM json_each(specs)
                WHERE value = json('{json_spec_composite}')
            )
            """
        )
        # Remove the 'composite' entry from specs
        op.execute(
            f"""
            UPDATE nodes
            SET specs = (
                SELECT json_group_array(value)
                FROM json_each(specs)
                WHERE value != json('{json_spec_composite}')
            )
            WHERE EXISTS (
                SELECT 1
                FROM json_each(specs)
                WHERE value = json('{json_spec_composite}')
            )
            """
        )

    # Step 3: Mark tables belonging to composite nodes as 'flattened'
    if connection.engine.dialect.name == "postgresql":
        json_stmnt = f"specs || '[{json_spec_flattened}]'::jsonb"
    elif connection.engine.dialect.name == "sqlite":
        json_stmnt = f"json_insert(specs, '$[#]', json('{json_spec_flattened}'))"
    op.execute(
        f"""
        UPDATE nodes SET specs = {json_stmnt}
        WHERE structure_family = 'table'
            AND EXISTS (
                SELECT 1
                FROM nodes AS parent
                WHERE parent.id = nodes.parent
                    AND parent.structure_family = 'composite'
            )
        """
    )

    # Step 4. Update the data_sources table (usually unnecessary)
    op.execute(
        """
        UPDATE data_sources
        SET structure_family = 'composite'
        WHERE structure_family = 'container'
            AND EXISTS (
                SELECT 1
                FROM nodes
                WHERE nodes.id = data_sources.node_id
                    AND nodes.structure_family = 'composite'
            )
        """
    )
