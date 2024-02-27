"""Add 'register' scope to default roles

Revision ID: 13024b8a6b74
Revises: 769180ce732e
Create Date: 2024-02-21 07:49:08.993168

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import Role

# revision identifiers, used by Alembic.
revision = "13024b8a6b74"
down_revision = "769180ce732e"
branch_labels = None
depends_on = None


ROLES = ["admin"]
NEW_SCOPES = ["register"]


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
