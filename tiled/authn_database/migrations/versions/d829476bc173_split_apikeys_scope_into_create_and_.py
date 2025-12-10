"""Split apikeys scope into create and delete

Revision ID: d829476bc173
Revises: 27e069ba3bf5
Create Date: 2025-12-08 17:09:18.062287

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import Role

# revision identifiers, used by Alembic.
revision = "d829476bc173"
down_revision = "27e069ba3bf5"
branch_labels = None
depends_on = None


ROLES = ["admin", "user"]
NEW_SCOPES_USER = ["create:apikeys", "revoke:apikeys", "create:node"]
OLD_SCOPES_USER = ["apikeys", "create"]
NEW_SCOPES_ADMIN = ["create:node"]
OLD_SCOPES_ADMIN = ["create"]


def upgrade():
    """
    Add new scopes to Roles.
    Remove old scopes from Roles, if present.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            scopes = role.scopes.copy()
            if role_name == "admin":
                NEW_SCOPES = NEW_SCOPES_ADMIN
                OLD_SCOPES = OLD_SCOPES_ADMIN
            else:
                NEW_SCOPES = NEW_SCOPES_USER
                OLD_SCOPES = OLD_SCOPES_USER
            for scope in OLD_SCOPES:
                if scope in scopes:
                    scopes.remove(scope)
            scopes.extend(NEW_SCOPES)
            role.scopes = scopes
            db.commit()


def downgrade():
    """
    Remove new scopes from Roles, if present.
    Add old scopes to Roles, if not preesent.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            scopes = role.scopes.copy()
            if role_name == "admin":
                NEW_SCOPES = NEW_SCOPES_ADMIN
                OLD_SCOPES = OLD_SCOPES_ADMIN
            else:
                NEW_SCOPES = NEW_SCOPES_USER
                OLD_SCOPES = OLD_SCOPES_USER
            for scope in NEW_SCOPES:
                if scope in scopes:
                    scopes.remove(scope)
            for scope in OLD_SCOPES:
                if scope not in scopes:
                    scopes.append(scope)
            role.scopes = scopes
            db.commit()
