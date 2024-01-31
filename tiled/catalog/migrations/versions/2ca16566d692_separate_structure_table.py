"""Separate structure table

Revision ID: 2ca16566d692
Revises: 1cd99c02d0c7
Create Date: 2024-01-22 20:44:23.132801

"""
from datetime import datetime

import orjson
import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import func

from tiled.catalog.orm import JSONVariant
from tiled.catalog.utils import compute_structure_id
from tiled.server.schemas import Management

# revision identifiers, used by Alembic.
revision = "2ca16566d692"
down_revision = "1cd99c02d0c7"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    # Create new 'structures' table.
    op.create_table(
        "structures",
        sa.Column("id", sa.Unicode(32), primary_key=True, unique=True),
        sa.Column("structure", JSONVariant, nullable=False),
    )
    # Get reference, to be used for copying data.
    structures = sa.Table(
        "structures",
        sa.MetaData(),
        sa.Column("id", sa.Unicode(32)),
        sa.Column("structure", JSONVariant),
    )
    if connection.engine.dialect.name == "sqlite":
        # We use a copy-and-move strategy here because we cannot get exactly
        # the result we want by adding a FOREIGN KEY to SQLite on an existing
        # table.
        op.create_table(
            "new_data_sources",
            sa.Column(
                "id", sa.Integer, primary_key=True, index=True, autoincrement=True
            ),
            sa.Column(
                "node_id",
                sa.Integer,
                sa.ForeignKey("nodes.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "structure_id",
                sa.Unicode(32),
                sa.ForeignKey("structures.id", ondelete="CASCADE"),
                nullable=True,
            ),
            sa.Column("mimetype", sa.Unicode(255), nullable=False),
            sa.Column("parameters", JSONVariant, nullable=True),
            sa.Column("management", sa.Enum(Management), nullable=False),
            sa.Column(
                "time_created",
                sa.DateTime(timezone=False),
                server_default=func.now(),
            ),
            sa.Column(
                "time_updated",
                sa.DateTime(timezone=False),
                onupdate=func.now(),
                server_default=func.now(),
            ),
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
            sa.Column("structure_id", sa.Unicode(32)),
            sa.Column("mimetype", sa.Unicode(255), nullable=False),
            sa.Column("parameters", JSONVariant, nullable=True),
            sa.Column("management", sa.Enum(Management), nullable=False),
            sa.Column(
                "time_created",
                sa.DateTime(timezone=False),
                server_default=func.now(),
            ),
            sa.Column(
                "time_updated",
                sa.DateTime(timezone=False),
                onupdate=func.now(),
                server_default=func.now(),
            ),
        )
        # Extract rows from data_sources and compute structure_id.
        results = connection.execute(
            sa.text(
                "SELECT id, node_id, structure, mimetype, parameters, management, "
                "time_created, time_updated FROM data_sources"
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
                "parameters": orjson.loads(row[4]),
                "management": row[5],
                "time_created": datetime.fromisoformat(row[6]),
                "time_udpated": datetime.fromisoformat(row[7]),
            }
            new_data_sources_rows.append(new_row)
            if structure_id not in unique_structures:
                unique_structures[structure_id] = orjson.loads(row[2])
        structures_rows = [
            {"id": structure_id, "structure": structure}
            for structure_id, structure in unique_structures.items()
        ]

        # Copy data into new tables.
        op.bulk_insert(structures, structures_rows)
        op.bulk_insert(new_data_sources, new_data_sources_rows)

        # Drop old 'data_structures' and move 'new_data_structures' into its place.
        op.drop_table("data_sources")
        op.rename_table("new_data_sources", "data_sources")
        # The above leaves many partially filled pages and, run on example
        # datasets, left the database slightly _larger_. Clean up.
        with op.get_context().autocommit_block():
            connection.execute(sa.text("VACUUM"))
    else:
        # PostgreSQL
        # Extract rows from data_sources and compute structure_id.
        results = connection.execute(
            sa.text("SELECT id, structure FROM data_sources")
        ).fetchall()
        unique_structures = {}  # map unique structure_id -> structure
        data_source_id_to_structure_id = {}
        for data_source_id, structure in results:
            structure_id = compute_structure_id(structure)
            unique_structures[structure_id] = structure
            data_source_id_to_structure_id[data_source_id] = structure_id
        structures_rows = [
            {"id": structure_id, "structure": structure}
            for structure_id, structure in unique_structures.items()
        ]
        # Copy data into 'structures' table.
        op.bulk_insert(structures, structures_rows)
        op.add_column(
            "data_sources",
            sa.Column(
                "structure_id",
                sa.Unicode(32),
                sa.ForeignKey("structures.id", ondelete="CASCADE"),
            ),
        )
        data_sources = sa.Table(
            "data_sources",
            sa.MetaData(),
            sa.Column("id", sa.Integer),
            sa.Column(
                "node_id",
                sa.Integer,
                sa.ForeignKey("nodes.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("structure_id", sa.Unicode(32)),
            sa.Column("structure", sa.Unicode(32)),
            sa.Column("mimetype", sa.Unicode(255), nullable=False),
            sa.Column("parameters", JSONVariant, nullable=True),
            sa.Column("management", sa.Enum(Management), nullable=False),
        )
        for data_source_id, structure_id in data_source_id_to_structure_id.items():
            connection.execute(
                data_sources.update()
                .values(structure_id=structure_id)
                .where(data_sources.c.id == data_source_id)
            )
        op.drop_column("data_sources", "structure")


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
