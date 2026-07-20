"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
import sqlalchemy as sa  # noqa
from alembic import op  # noqa
${imports if imports else ""}
# revision identifiers, used by Alembic.
revision = ${repr(up_revision).replace("'", '"')}
down_revision = ${repr(down_revision).replace("'", '"')}
branch_labels = ${repr(branch_labels).replace("'", '"')}
depends_on = ${repr(depends_on).replace("'", '"')}


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
