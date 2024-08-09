"""Change 'path' to 'dataset' in HDF5 assets

Revision ID: 562203c724c7
Revises: ed3a4223a600
Create Date: 2024-08-09 10:13:36.384838

"""
from alembic import op
import sqlalchemy as sa
from tiled.catalog.orm import JSONVariant

# revision identifiers, used by Alembic.
revision = '562203c724c7'
down_revision = 'ed3a4223a600'
branch_labels = None
depends_on = None

def upgrade():
    connection = op.get_bind()
    nodes = sa.Table(
        "nodes",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("metadata", JSONVariant),
    )

    # Loop over all nodes that have 'parameters' field, choose and update those related to hdf5 files
    condition = sa.text("CAST(nodes.metadata->>'parameters' AS TEXT) != ''")
    cursor = connection.execute(sa.select(nodes.c.id, nodes.c.metadata).filter(condition).select_from(nodes))
    for _id, _md in cursor:
        if 'hdf5' in (_md.get('mimetype', '') + _md.get('spec', '')).lower():
                if isinstance(_md['parameters'], dict) and ('path' in _md['parameters'].keys()):
                    _md['parameters']['dataset'] = _md['parameters'].pop('path')
                    connection.execute(nodes.update().where(nodes.c.id == _id).values(metadata=_md))

def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
