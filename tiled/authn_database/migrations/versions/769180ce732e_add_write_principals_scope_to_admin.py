"""Add 'write:principals' scope to admin

Revision ID: 769180ce732e
Revises: c7bd2573716d
Create Date: 2023-12-12 17:57:56.388145

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import Role

# revision identifiers, used by Alembic.
revision = "769180ce732e"
down_revision = "c7bd2573716d"
branch_labels = None
depends_on = None


SCOPE = "write:principals"


def upgrade():
    """
    Add 'write:principals' scope to default 'admin' Role.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        role = db.query(Role).filter(Role.name == "admin").first()
        scopes = role.scopes.copy()
        scopes.append(SCOPE)
        role.scopes = scopes
        db.commit()


def downgrade():
    """
    Remove new scopes from Roles, if present.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        role = db.query(Role).filter(Role.name == "admin").first()
        scopes = role.scopes.copy()
        if SCOPE in scopes:
            scopes.remove(SCOPE)
        role.scopes = scopes
        db.commit()
