"""Add self revoking scope to default user role

Revision ID: 2abc44b9f196
Revises: 2d1b550e12e0
Create Date: 2026-07-09 09:44:24.224144

"""
from alembic import op
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import Role

# revision identifiers, used by Alembic.
revision = "2abc44b9f196"
down_revision = "2d1b550e12e0"
branch_labels = None
depends_on = None


NEW_SCOPE = ["revoke:apikeys:self"]


def upgrade():
    """
    Add new scope to user role.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        role = db.query(Role).filter(Role.name == "user").first()
        scopes = role.scopes.copy()
        scopes.extend(NEW_SCOPE)
        role.scopes = scopes
        db.commit()


def downgrade():
    """
    Remove new scope from user role, if present.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        role = db.query(Role).filter(Role.name == "user").first()
        scopes = role.scopes.copy()
        if "revoke:apikeys:self" in scopes:
            scopes.remove("revoke:apikeys:self")
        role.scopes = scopes
        db.commit()
