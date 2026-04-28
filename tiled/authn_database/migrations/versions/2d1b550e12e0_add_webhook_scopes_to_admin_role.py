"""Add read:webhooks and write:webhooks scopes to admin role

Revision ID: 2d1b550e12e0
Revises: d829476bc173
Create Date: 2026-04-24 16:17:12.427500

"""

from alembic import op
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import Role

# revision identifiers, used by Alembic.
revision = "2d1b550e12e0"
down_revision = "d829476bc173"
branch_labels = None
depends_on = None


ROLES = ["admin"]
NEW_SCOPES = ["read:webhooks", "write:webhooks"]


def upgrade():
    """
    Add webhook scopes to the admin Role.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            if not role:
                raise RuntimeError(f"Expected role '{role_name}' not found in db!")
            scopes = role.scopes.copy()
            for scope in NEW_SCOPES:
                if scope not in scopes:
                    scopes.append(scope)
            role.scopes = scopes
            db.commit()


def downgrade():
    """
    Remove webhook scopes from the admin Role, if present.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            if not role:
                raise RuntimeError(f"Expected role '{role_name}' not found in db!")
            scopes = role.scopes.copy()
            for scope in NEW_SCOPES:
                if scope in scopes:
                    scopes.remove(scope)
            role.scopes = scopes
            db.commit()
