"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
import sqlalchemy as sa  # noqa
from alembic import op  # noqa
${imports if imports else ""}
# revision identifiers, used by Alembic.
% if isinstance(up_revision, str):
revision = ${'"' + up_revision + '"'}
% else:
revision = ${repr(up_revision)}
% endif
% if isinstance(down_revision, str):
down_revision = ${'"' + down_revision + '"'}
% else:
down_revision = ${repr(down_revision)}
% endif
% if isinstance(branch_labels, str):
branch_labels = ${'"' + branch_labels + '"'}
% else:
branch_labels = ${repr(branch_labels)}
% endif
% if isinstance(depends_on, str):
branch_labels = ${'"' + depends_on + '"'}
% else:
depends_on = ${repr(depends_on)}
% endif


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
