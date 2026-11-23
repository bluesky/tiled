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
depends_on = ${'"' + depends_on + '"'}
% else:
depends_on = ${repr(depends_on)}
% endif

# Set to False only if the server at this revision runs correctly on a database
# at the previous revision; i.e. this migration may be applied after the new server
# version is deployed. Otherwise the server refuses to start on an older database.
required_at_startup = True


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
