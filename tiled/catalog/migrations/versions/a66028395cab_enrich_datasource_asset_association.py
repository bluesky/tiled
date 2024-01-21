"""Enrich DataSource-Asset association.

Revision ID: a66028395cab
Revises: 3db11ff95b6c
Create Date: 2024-01-21 15:17:20.571763

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import (  # unique_parameter_num_null_check,
    DataSourceAssetAssociation,
    JSONVariant,
)

# revision identifiers, used by Alembic.
revision = "a66028395cab"
down_revision = "3db11ff95b6c"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    data_sources = sa.Table(
        "data_sources",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("node_id", sa.Integer),
        sa.Column("mimetype", sa.Unicode),
        sa.Column("structure", JSONVariant),
    )
    data_source_asset_association = sa.Table(
        DataSourceAssetAssociation.__tablename__,
        sa.MetaData(),
        sa.Column("asset_id", sa.Integer),
        sa.Column("data_source_id", sa.Integer),
        sa.Column("parameter", sa.Unicode(255)),
        sa.Column("num", sa.Integer),
    )

    # Rename some MIME types.

    # While Awkward data is typically _transmitted_ in a ZIP archive,
    # it is stored as directory of buffers, with no ZIP involved.
    # Thus using 'application/zip' in the database was a mistake.
    connection.execute(
        data_sources.update()
        .where(data_sources.c.mimetype == "application/zip")
        .values(mimetype="application/x-awkward-buffers")
    )
    # The format is standard parquet. We will use a MIME type
    # parameter to let Tiled know to use the Adapter for sparse
    # data, as opposed to the Adapter for tabular data.
    connection.execute(
        data_sources.update()
        .where(data_sources.c.mimetype == "application/x-parquet-sparse")
        .values(mimetype="application/x-parquet;structure=sparse")
    )

    # Add columns 'parameter' and 'num' to association table.
    op.add_column(
        DataSourceAssetAssociation.__tablename__,
        sa.Column("parameter", sa.Unicode(255), nullable=True),
    )
    op.add_column(
        DataSourceAssetAssociation.__tablename__,
        sa.Column("num", sa.Integer, nullable=True),
    )

    # First populate the columns to bring them into compliance with
    # constraints. Then, apply constraints.

    connection.execute(
        data_source_asset_association.update()
        .where(
            data_source_asset_association.c.data_source_id
            == sa.select(data_sources.c.id)
            .where(
                sa.not_(
                    sa.or_(
                        data_sources.c.mimetype == "multipart/related;type=image/tiff",
                        data_sources.c.mimetype == "application/x-parquet",
                        data_sources.c.mimetype
                        == "application/x-parquet;structure=sparse",
                    )
                )
            )
            .as_scalar()
        )
        .values(parameter="data_uri")
    )
    connection.execute(
        data_source_asset_association.update()
        .where(
            data_source_asset_association.c.data_source_id
            == sa.select(data_sources.c.id)
            .where(
                sa.or_(
                    data_sources.c.mimetype == "multipart/related;type=image/tiff",
                    data_sources.c.mimetype == "application/x-parquet",
                    data_sources.c.mimetype == "application/x-parquet;structure=sparse",
                )
            )
            .as_scalar()
        )
        .values(parameter="data_uris")  # plural
    )
    # results = connection.execute(
    #     sa.select(
    #         data_sources.c.id,
    #         data_sources.c.structure,
    #         nodes.c.structure_family,
    #     ).select_from(joined)
    # ).fetchall()

    # Create unique constraint and triggers.
    # op.create_unique_constraint(
    #     constraint_name="parameter_num_unique_constraint",
    #     table_name=DataSourceAssetAssociation.__tablename__,
    #     columns=[
    #         "data_source_id",
    #         "parameter",
    #         "num",
    #     ],
    # )
    # unique_parameter_num_null_check(data_source_asset_association, connection)


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
