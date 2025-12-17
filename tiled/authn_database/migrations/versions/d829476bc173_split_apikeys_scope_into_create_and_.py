"""Split apikeys scope into create and delete

Revision ID: d829476bc173
Revises: 27e069ba3bf5
Create Date: 2025-12-08 17:09:18.062287

"""
from alembic import op
from sqlalchemy.orm import lazyload
from sqlalchemy.orm.session import Session

from tiled.authn_database.orm import APIKey, Role

# revision identifiers, used by Alembic.
revision = "d829476bc173"
down_revision = "27e069ba3bf5"
branch_labels = None
depends_on = None


ROLES = ["admin", "user"]
OLD_TO_NEW_SCOPES = {
    "create": ["create:node"],
    "apikeys": ["revoke:apikeys", "create:apikeys"],
}
NEW_TO_OLD_SCOPES = {
    "create:node": ["create"],
    "revoke:apikeys": ["apikeys"],
    "create:apikeys": ["apikeys"],
}


def upgrade():
    """
    Add new scopes to Roles.
    Remove old scopes from Roles, if present.

    Also adjust scopes for server API keys.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            if not role:
                raise RuntimeError(f"Expected role '{role_name}' not found in db!")
            scopes = set(role.scopes or [])
            for old_scope, new_scopes in OLD_TO_NEW_SCOPES.items():
                if old_scope in scopes:
                    scopes.discard(old_scope)
                    scopes |= set(new_scopes)
            role.scopes = list(scopes)

        for apikey in (
            db.query(APIKey).options(lazyload(APIKey.principal)).yield_per(500)
        ):
            scopes = set(apikey.scopes)
            for old_scope, new_scopes in OLD_TO_NEW_SCOPES.items():
                if old_scope in scopes:
                    scopes.discard(old_scope)
                    scopes |= set(new_scopes)
            apikey.scopes = list(scopes)

        db.commit()


def downgrade():
    """
    Remove new scopes from Roles, if present.
    Add old scopes to Roles, if not preesent.

    Also adjust scopes for server API keys.
    """
    connection = op.get_bind()
    with Session(bind=connection) as db:
        for role_name in ROLES:
            role = db.query(Role).filter(Role.name == role_name).first()
            if not role:
                raise RuntimeError(f"Expected role '{role_name}' not found in db!")
            scopes = set(role.scopes or [])
            for new_scope, old_scopes in NEW_TO_OLD_SCOPES.items():
                if new_scope in scopes:
                    scopes.discard(new_scope)
                    scopes |= set(old_scopes)
            role.scopes = list(scopes)

        for apikey in (
            db.query(APIKey).options(lazyload(APIKey.principal)).yield_per(500)
        ):
            scopes = set(apikey.scopes)
            for new_scope, old_scopes in NEW_TO_OLD_SCOPES.items():
                if new_scope in scopes:
                    scopes.discard(new_scope)
                    scopes |= set(old_scopes)
            apikey.scopes = list(scopes)

        db.commit()
