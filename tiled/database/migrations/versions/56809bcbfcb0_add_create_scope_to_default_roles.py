"""Add 'create' scope to default roles.

Revision ID: 56809bcbfcb0
Revises: 722ff4e4fcc7
Create Date: 2022-09-29 09:16:32.797138

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.database.orm import Role

# revision identifiers, used by Alembic.
revision = "56809bcbfcb0"
down_revision = "722ff4e4fcc7"
branch_labels = None
depends_on = None


ROLES = ["admin", "user"]
NEW_SCOPES = ["create"]


def upgrade():
    """
    Add new scopes to Roles.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            scopes = role.scopes.copy()
            scopes.extend(NEW_SCOPES)
            role.scopes = scopes
            db.commit()


def downgrade():
    """
    Remove new scopes from Roles, if present.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            scopes = role.scopes.copy()
            for scope in NEW_SCOPES:
                if scope in scopes:
                    scopes.remove(scope)
            role.scopes = scopes
            db.commit()
