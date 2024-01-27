"""Enrich DataSource-Asset association.

Revision ID: a66028395cab
Revises: 3db11ff95b6c
Create Date: 2024-01-21 15:17:20.571763

"""
import sqlalchemy as sa
from alembic import op

from tiled.catalog.orm import JSONVariant

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
        "data_source_asset_association",
        sa.MetaData(),
        sa.Column("asset_id", sa.Integer),
        sa.Column("data_source_id", sa.Integer),
        sa.Column("parameter", sa.Unicode(255)),
        sa.Column("num", sa.Integer),
    )
    assets = sa.Table(
        "assets",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("data_uri", sa.Unicode),
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
        "data_source_asset_association",
        sa.Column("parameter", sa.Unicode(255), nullable=True),
    )
    op.add_column(
        "data_source_asset_association",
        sa.Column("num", sa.Integer, nullable=True),
    )

    # First populate the columns to bring them into compliance with
    # constraints. Then, apply constraints.

    results = connection.execute(
        sa.select(data_sources.c.id)
        .where(
            sa.not_(
                data_sources.c.mimetype.in_(
                    [
                        "multipart/related;type=image/tiff",
                        "application/x-parquet",
                        "application/x-parquet;structure=sparse",
                    ]
                )
            )
        )
        .select_from(data_sources)
        .join(
            data_source_asset_association,
            data_sources.c.id == data_source_asset_association.c.data_source_id,
        )
        .join(
            assets,
            data_source_asset_association.c.asset_id == assets.c.id,
        )
        .distinct()
    ).fetchall()
    for (data_source_id,) in results:
        connection.execute(
            data_source_asset_association.update()
            .where(data_source_asset_association.c.data_source_id == data_source_id)
            .values(parameter="data_uri")
        )
    results = connection.execute(
        sa.select(data_sources.c.id)
        .where(
            data_sources.c.mimetype.in_(
                [
                    "multipart/related;type=image/tiff",
                    "application/x-parquet",
                    "application/x-parquet;structure=sparse",
                ]
            )
        )
        .select_from(data_sources)
        .join(
            data_source_asset_association,
            data_sources.c.id == data_source_asset_association.c.data_source_id,
        )
        .join(
            assets,
            data_source_asset_association.c.asset_id == assets.c.id,
        )
        .distinct()
    ).fetchall()
    for (data_source_id,) in results:
        connection.execute(
            data_source_asset_association.update()
            .where(data_source_asset_association.c.data_source_id == data_source_id)
            .values(parameter="data_uris")  # plural
        )
        sorted_assoc = connection.execute(
            sa.select(data_source_asset_association.c.data_source_id, assets.c.id)
            .where(data_source_asset_association.c.data_source_id == data_source_id)
            .order_by(assets.c.data_uri)
            .select_from(data_sources)
            .join(
                data_source_asset_association,
                data_sources.c.id == data_source_asset_association.c.data_source_id,
            )
            .join(
                assets,
                data_source_asset_association.c.asset_id == assets.c.id,
            )
        ).fetchall()
        for num, (data_source_id, asset_id) in enumerate(sorted_assoc, start=1):
            connection.execute(
                data_source_asset_association.update()
                .where(data_source_asset_association.c.data_source_id == data_source_id)
                .where(data_source_asset_association.c.asset_id == asset_id)
                .values(num=num)
            )

    # Create unique constraint and triggers.
    if connection.engine.dialect.name == "sqlite":
        # SQLite does not supported adding constraints to an existing table.
        # We invoke its 'copy and move' functionality.
        with op.batch_alter_table("data_source_asset_association") as batch_op:
            # Gotcha: This does not take table_name because it is bound into batch_op.
            batch_op.create_unique_constraint(
                "parameter_num_unique_constraint",
                [
                    "data_source_id",
                    "parameter",
                    "num",
                ],
            )
        # This creates a pair of triggers on the data_source_asset_association
        # table. Each pair include one trigger that runs when NEW.num IS NULL and
        # one trigger than runs when NEW.num IS NOT NULL. Thus, for a given insert,
        # only one of these triggers is run.
        with op.get_context().autocommit_block():
            connection.execute(
                sa.text(
                    """
    CREATE TRIGGER cannot_insert_num_null_if_num_exists
    BEFORE INSERT ON data_source_asset_association
    WHEN NEW.num IS NULL
    BEGIN
        SELECT RAISE(ABORT, 'Can only insert num=NULL if no other row exists for the same parameter')
        WHERE EXISTS
        (
            SELECT 1
            FROM data_source_asset_association
            WHERE parameter = NEW.parameter
            AND data_source_id = NEW.data_source_id
        );
    END"""
                )
            )
            connection.execute(
                sa.text(
                    """
    CREATE TRIGGER cannot_insert_num_int_if_num_null_exists
    BEFORE INSERT ON data_source_asset_association
    WHEN NEW.num IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'Can only insert INTEGER num if no NULL row exists for the same parameter')
        WHERE EXISTS
        (
            SELECT 1
            FROM data_source_asset_association
            WHERE parameter = NEW.parameter
            AND num IS NULL
            AND data_source_id = NEW.data_source_id
        );
    END"""
                )
            )
    else:
        # PostgreSQL
        op.create_unique_constraint(
            "parameter_num_unique_constraint",
            "data_source_asset_association",
            [
                "data_source_id",
                "parameter",
                "num",
            ],
        )
        connection.execute(
            sa.text(
                """
CREATE OR REPLACE FUNCTION raise_if_parameter_exists()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND data_source_id = NEW.data_source_id
    ) THEN
        RAISE EXCEPTION 'Can only insert num=NULL if no other row exists for the same parameter';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER cannot_insert_num_null_if_num_exists
BEFORE INSERT ON data_source_asset_association
FOR EACH ROW
WHEN (NEW.num IS NULL)
EXECUTE FUNCTION raise_if_parameter_exists();"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE OR REPLACE FUNCTION raise_if_null_parameter_exists()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND data_source_id = NEW.data_source_id
        AND num IS NULL
    ) THEN
        RAISE EXCEPTION 'Can only insert INTEGER num if no NULL row exists for the same parameter';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;"""
            )
        )
        connection.execute(
            sa.text(
                """
CREATE TRIGGER cannot_insert_num_int_if_num_null_exists
BEFORE INSERT ON data_source_asset_association
FOR EACH ROW
WHEN (NEW.num IS NOT NULL)
EXECUTE FUNCTION raise_if_null_parameter_exists();"""
            )
        )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
