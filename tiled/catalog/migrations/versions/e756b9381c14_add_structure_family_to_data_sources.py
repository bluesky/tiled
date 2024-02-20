"""Add structure_family to data_sources.

Revision ID: e756b9381c14
Revises: 2ca16566d692
Create Date: 2024-02-20 09:18:45.405078

"""
import sqlalchemy as sa
from alembic import op

from tiled.structures.core import StructureFamily

# revision identifiers, used by Alembic.
revision = "e756b9381c14"
down_revision = "2ca16566d692"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    # Initialize new structure_family column as a nullable at first because
    # existing rows will be empty.
    op.add_column(
        "data_sources",
        sa.Column("structure_family", sa.Enum(StructureFamily), nullable=True),
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
        sa.Column("structure_family", sa.Enum(StructureFamily), nullable=True),
    )
    # Fill in structure_family from associated node.
    results = connection.execute(
        sa.text(
            "SELECT data_sources.id, nodes.structure_family "
            "FROM data_sources JOIN nodes ON "
            "data_sources.node_id = nodes.id"
        )
    ).fetchall()
    for data_source_id, structure_family in results:
        connection.execute(
            data_sources.update()
            .values(structure_family=structure_family)
            .where(data_sources.c.id == data_source_id)
        )
    # Now make structure_family NOT NULL.
    if connection.engine.dialect.name == "sqlite":
        with op.batch_alter_table("data_sources") as batch_op:
            batch_op.alter_column("structure_family", nullable=False)
    else:
        op.alter_column("data_sources", "structure_family", nullable=False)


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
