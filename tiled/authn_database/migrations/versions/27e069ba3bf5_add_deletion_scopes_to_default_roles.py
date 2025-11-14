"""Add deletion scopes to default Roles

Revision ID: 27e069ba3bf5
Revises: a806cc635ab2
Create Date: 2025-11-06 19:53:44.355094

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import Role

# revision identifiers, used by Alembic.
revision = "27e069ba3bf5"
down_revision = "a806cc635ab2"
branch_labels = None
depends_on = None


ROLES = ["admin", "user"]
NEW_SCOPES = ["delete:revision", "delete:node"]


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
