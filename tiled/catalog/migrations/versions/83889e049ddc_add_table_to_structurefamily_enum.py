"""Reorganize 'structure' and rename 'dataframe' to 'table'.

Revision ID: 83889e049ddc
Revises: 6825c778aa3c
Create Date: 2023-08-04 06:38:48.775874

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "83889e049ddc"
down_revision = "6825c778aa3c"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        from sqlalchemy import orm

        session = orm.Session(bind=connection)
        with session.begin():
            session.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'table' AFTER 'dataframe'"
                )
            )
            session.commit()
    # Nothing to do for SQLite


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in alpha releases.
    raise NotImplementedError
