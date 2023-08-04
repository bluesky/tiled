"""Reorganize 'structure' and rename 'dataframe' to 'table'.

Revision ID: 83889e049ddc
Revises: 6825c778aa3c
Create Date: 2023-08-04 06:38:48.775874

"""
import base64

import pyarrow
import sqlalchemy as sa
from alembic import op

from tiled.serialization.table import deserialize_arrow
from tiled.structures.table import B64_ENCODED_PREFIX

# revision identifiers, used by Alembic.
revision = "83889e049ddc"
down_revision = "6825c778aa3c"
branch_labels = None
depends_on = None

# This is a *data migration* only. There are no changes to the SQL schema.


def upgrade():
    connection = op.get_bind()
    nodes = sa.Table(
        "nodes",
        sa.MetaData(),
        sa.Column("structure_family", sa.String(32)),
        sa.Column("structure", sa.Unicode(length=100)),
    )
    results = connection.execute(
        sa.select(
            [
                nodes.c.id,
                nodes.c.structure_family,
                nodes.c.structure,
            ]
        )
    ).fetchall()
    for id_, structure_family, structure in results:
        if structure_family == "array":
            new_structure_family = structure_family
            # Consolidate "macro" and "micro".
            new_structure = {}
            new_structure["shape"] = structure["macro"]["shape"]
            new_structure["dims"] = structure["macro"]["dims"]
            new_structure["chunks"] = structure["macro"]["chunks"]
            new_structure["resizable"] = structure["macro"]["resizable"]
            new_structure["data_type"] = structure["micro"]
        if structure_family == "dataframe":
            # Rename "dataframe" to "table".
            new_structure_family = "table"
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
            new_structure_family = structure_family
            new_structure = structure
        connection.execute(
            nodes.update()
            .where(nodes.c.id == id_)
            .values(
                structure=new_structure,
                structure_family=new_structure_family,
            )
        )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
