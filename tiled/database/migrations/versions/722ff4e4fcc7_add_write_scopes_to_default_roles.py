""""Add write scopes to default Roles."

Revision ID: 722ff4e4fcc7
Revises: 481830dd6c11
Create Date: 2022-03-22 16:54:02.764016

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.database.orm import Role

# revision identifiers, used by Alembic.
revision = "722ff4e4fcc7"
down_revision = "481830dd6c11"
branch_labels = None
depends_on = None


ROLES = ["admin", "user"]
NEW_SCOPES = ["write:metadata", "write:data"]


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
