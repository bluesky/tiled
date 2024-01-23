"""Separate structure table

Revision ID: 2ca16566d692
Revises: 1cd99c02d0c7
Create Date: 2024-01-22 20:44:23.132801

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant
from tiled.catalog.utils import compute_structure_id
from tiled.server.schemas import Management

# revision identifiers, used by Alembic.
revision = "2ca16566d692"
down_revision = "1cd99c02d0c7"
branch_labels = None
depends_on = None


def upgrade():
    # We use a copy-and-move strategy here because we cannot get
    # exactly the result we want by adding a FOREIGN KEY to SQLite
    # on an existing table.

    # Create new 'structures' table.
    op.create_table(
        "structures",
        sa.Column("id", sa.Unicode(32), primary_key=True, unique=True),
        sa.Column("structure", JSONVariant, nullable=False),
    )
    # Create 'new_data_sources' table, which will be renamed to (and replace)
    # 'data_sources' at the end of this migration.
    op.create_table(
        "new_data_sources",
        sa.Column("id", sa.Integer, primary_key=True, index=True, autoincrement=True),
        sa.Column(
            "node_id",
            sa.Integer,
            sa.ForeignKey("nodes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "structure_id",
            sa.Integer,
            sa.ForeignKey("structures.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("mimetype", sa.Unicode(255), nullable=False),
        sa.Column("parameters", JSONVariant, nullable=True),
        sa.Column("management", sa.Enum(Management), nullable=False),
    )

    # Get references to these tables, to be used for copying data.
    structures = sa.Table(
        "structures",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("structure", JSONVariant),
    )
    new_data_sources = sa.Table(
        "new_data_sources",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column(
            "node_id",
            sa.Integer,
            sa.ForeignKey("nodes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("structure_id", sa.Integer),
        sa.Column("mimetype", sa.Unicode(255), nullable=False),
        sa.Column("parameters", JSONVariant, nullable=True),
        sa.Column("management", sa.Enum(Management), nullable=False),
    )

    # Extract rows from data_sources and compute structure_id.
    connection = op.get_bind()
    results = connection.execute(
        sa.text(
            "SELECT id, node_id, structure, mimetype, parameters, management FROM data_sources"
        )
    ).fetchall()

    new_data_sources_rows = []
    unique_structures = {}  # map unique structure_id -> structure
    for row in results:
        structure_id = compute_structure_id(row[2])
        new_row = {
            "id": row[0],
            "node_id": row[1],
            "structure_id": structure_id,
            "mimetype": row[3],
            "parameters": row[4],
            "management": row[5],
        }
        new_data_sources_rows.append(new_row)
        unique_structures[structure_id] = row[2]
    structures_rows = [
        {"id": structure_id, "structure": structure}
        for structure_id, structure in unique_structures.items()
    ]

    # Copy data into new tables.
    op.bulk_insert(new_data_sources, new_data_sources_rows)
    op.bulk_insert(structures, structures_rows)

    # Drop old 'data_structures' and move 'new_data_structures' into its place.
    op.drop_table("data_sources")
    op.rename_table("new_data_sources", "data_sources")


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
