"""Reorganize 'structure' and rename 'dataframe' to 'table'.

Revision ID: 83889e049ddc
Revises: 6825c778aa3c
Create Date: 2023-08-04 06:38:48.775874

"""
import base64

import pyarrow
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

from tiled.serialization.table import deserialize_arrow
from tiled.structures.core import StructureFamily
from tiled.structures.table import B64_ENCODED_PREFIX

# revision identifiers, used by Alembic.
revision = "83889e049ddc"
down_revision = "6825c778aa3c"
branch_labels = None
depends_on = None


# Use JSON with SQLite and JSONB with PostgreSQL.
JSONVariant = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        # This change must be committed before the new 'table' enum value can be used.
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'table' AFTER 'dataframe'"
                )
            )
    # Rename "dataframe" to "table".
    nodes = sa.Table(
        "nodes",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("structure_family", sa.Unicode(32)),
    )
    # Use a raw text query to work around type fussiness in postgres.
    op.execute(
        sa.text(
            "UPDATE nodes SET structure_family='table' WHERE nodes.structure_family = 'dataframe'"
        )
    )

    data_sources = sa.Table(
        "data_sources",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("node_id", sa.Integer),
        sa.Column("structure", JSONVariant),
    )
    joined = data_sources.join(nodes, data_sources.c.node_id == nodes.c.id)
    results = connection.execute(
        sa.select(
            data_sources.c.id,
            data_sources.c.structure,
            nodes.c.structure_family,
        ).select_from(joined)
    ).fetchall()
    for id_, structure, structure_family in results:
        if structure_family == StructureFamily.array:
            # Consolidate "macro" and "micro".
            new_structure = {}
            new_structure["shape"] = structure["macro"]["shape"]
            new_structure["dims"] = structure["macro"]["dims"]
            new_structure["chunks"] = structure["macro"]["chunks"]
            new_structure["resizable"] = structure["macro"]["resizable"]
            new_structure["data_type"] = structure["micro"]
        elif structure_family == StructureFamily.table:
            # Consolidate "macro" and "micro".
            new_structure = {}
            new_structure["columns"] = structure["macro"]["columns"]
            new_structure["npartitions"] = structure["macro"]["npartitions"]
            new_structure["resizable"] = structure["macro"]["resizable"]
            # Re-encode the Arrow schema.
            meta_bytes = structure["micro"]["meta"]
            meta = deserialize_arrow(base64.b64decode(meta_bytes))
            schema_bytes = pyarrow.Table.from_pandas(meta).schema.serialize()
            schema_b64 = base64.b64encode(schema_bytes).decode("utf-8")
            data_uri = B64_ENCODED_PREFIX + schema_b64
            new_structure["arrow_schema"] = data_uri
        else:
            continue
        connection.execute(
            data_sources.update()
            .where(data_sources.c.id == id_)
            .values(structure=new_structure)
        )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
