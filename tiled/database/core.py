import hashlib
import uuid as uuid_module
from datetime import datetime

from alembic import command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from .alembic_utils import temp_alembic_ini
from .base import Base
from .orm import APIKey, Identity, Principal, Role, Session

# This is the alembic revision ID of the database revision
# required by this version of Tiled.
REQUIRED_REVISION = "722ff4e4fcc7"
# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = ["722ff4e4fcc7", "481830dd6c11"]


def create_default_roles(engine):

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    db.add(
        Role(
            name="user",
            description="Default Role for users.",
            scopes=[
                "read:metadata",
                "read:data",
                "write:metadata",
                "write:data",
                "apikeys",
            ],
        ),
    )
    db.add(
        Role(
            name="admin",
            description="Role with elevated privileges.",
            scopes=[
                "read:metadata",
                "read:data",
                "write:metadata",
                "write:data",
                "admin:apikeys",
                "read:principals",
                "metrics",
            ],
        ),
    )
    db.commit()


def initialize_database(engine):

    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    # Create all tables.
    Base.metadata.create_all(engine)

    # Initialize Roles table.
    create_default_roles(engine)

    # Mark current revision.
    with temp_alembic_ini(engine.url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.stamp(alembic_cfg, "head")


def upgrade(engine, revision):
    """
    Upgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine.url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.upgrade(alembic_cfg, revision)


def downgrade(engine, revision):
    """
    Downgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine.url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.downgrade(alembic_cfg, revision)


class UnrecognizedDatabase(Exception):
    pass


class UninitializedDatabase(Exception):
    pass


class DatabaseUpgradeNeeded(Exception):
    pass


def get_current_revision(engine):

    redacted_url = engine.url._replace(password="[redacted]")
    with engine.begin() as conn:
        context = migration.MigrationContext.configure(conn)
        heads = context.get_current_heads()
    if heads == ():
        return None
    elif len(heads) != 1:
        raise UnrecognizedDatabase(
            f"This database {redacted_url} is stamped with an alembic revisions {heads}. "
            "It looks like Tiled has been configured to connect to a database "
            "already populated by some other application (not Tiled) or else "
            "its database is in a corrupted state."
        )
    (revision,) = heads
    if revision not in ALL_REVISIONS:
        raise UnrecognizedDatabase(
            f"The datbase {redacted_url} has an unrecognized revision {revision}. "
            "It may have been created by a newer version of Tiled."
        )
    return revision


def check_database(engine):
    revision = get_current_revision(engine)
    redacted_url = engine.url._replace(password="[redacted]")
    if revision is None:
        raise UninitializedDatabase(
            f"The database {redacted_url} has no revision stamp. It may be empty. "
            "It can be initialized with `initialize_database(engine)`."
        )
    elif revision != REQUIRED_REVISION:
        raise DatabaseUpgradeNeeded(
            f"The database {redacted_url} has revision {revision} and "
            f"needs to be upgraded to revision {REQUIRED_REVISION}."
        )


def purge_expired(engine, cls):
    """
    Remove expired entries.

    Return reference to cls, supporting usage like

    >>> db.query(purge_expired(engine, orm.APIKey))
    """
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    now = datetime.utcnow()
    deleted = False
    for obj in (
        db.query(cls)
        .filter(cls.expiration_time.is_not(None))
        .filter(cls.expiration_time < now)
    ):
        deleted = True
        db.delete(obj)
    if deleted:
        db.commit()
    return cls


def create_user(db, identity_provider, id):
    principal = Principal(type="user")
    user_role = db.query(Role).filter(Role.name == "user").first()
    principal.roles.append(user_role)
    db.add(principal)
    db.commit()
    db.refresh(principal)  # Refresh to sync back the auto-generated uuid.
    identity = Identity(
        provider=identity_provider,
        id=id,
        principal_id=principal.id,
    )
    db.add(identity)
    db.commit()
    return principal


def lookup_valid_session(db, session_id):
    if isinstance(session_id, int):
        # Old versions of tiled used an integer sid.
        # Reject any of those old sessions and force reauthentication.
        return None

    session = (
        db.query(Session)
        .filter(Session.uuid == uuid_module.UUID(hex=session_id))
        .first()
    )
    if (
        session.expiration_time is not None
        and session.expiration_time < datetime.utcnow()
    ):
        db.delete(session)
        db.commit()
        return None
    return session


def make_admin_by_identity(db, identity_provider, id):
    identity = (
        db.query(Identity)
        .filter(Identity.id == id)
        .filter(Identity.provider == identity_provider)
        .first()
    )
    if identity is None:
        principal = create_user(db, identity_provider, id)
    else:
        principal = identity.principal
    admin_role = db.query(Role).filter(Role.name == "admin").first()
    principal.roles.append(admin_role)
    db.commit()
    return principal


def lookup_valid_api_key(db, secret):
    """
    Look up an API key. Ensure that it is valid.
    """

    now = datetime.utcnow()
    hashed_secret = hashlib.sha256(secret).digest()
    api_key = (
        db.query(APIKey)
        .filter(APIKey.first_eight == secret.hex()[:8])
        .filter(APIKey.hashed_secret == hashed_secret)
        .first()
    )
    if api_key is None:
        # No match
        validated_api_key = None
    elif (api_key.expiration_time is not None) and (api_key.expiration_time < now):
        # Match is expired. Delete it.
        db.delete(api_key)
        db.commit()
        validated_api_key = None
    elif api_key.principal is None:
        # The Principal for the API key no longer exists. Delete it.
        db.delete(api_key)
        db.commit()
        validated_api_key = None
    else:
        validated_api_key = api_key
    return validated_api_key


def latest_principal_activity(db, principal):
    """
    The most recent time this Principal has logged in with an Identity,
    refreshed a Session, or used an APIKey.

    Note that activity that is authenticated using an access token is not
    captured here. As usual with JWTs, those requests do not interact with
    this database, for performance reasons. Therefore, this may lag actual
    activity by as much as the max age of an access token (default: 15
    minutes).
    """
    latest_identity_activity = (
        db.query(func.max(Identity.latest_login))
        .filter(Identity.principal_id == principal.id)
        .scalar()
    )
    latest_session_activity = (
        db.query(func.max(Session.time_last_refreshed))
        .filter(Session.principal_id == principal.id)
        .scalar()
    )
    latest_api_key_activity = (
        db.query(func.max(APIKey.latest_activity))
        .filter(APIKey.principal_id == principal.id)
        .scalar()
    )
    all_activity = [
        latest_identity_activity,
        latest_api_key_activity,
        latest_session_activity,
    ]
    if all([t is None for t in all_activity]):
        return None
    return max(t for t in all_activity if t is not None)
