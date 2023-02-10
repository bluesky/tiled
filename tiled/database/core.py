import hashlib
import uuid as uuid_module
from datetime import datetime

from alembic import command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from .alembic_utils import temp_alembic_ini
from .base import Base
from .orm import APIKey, Identity, PendingSession, Principal, Role, Session

# This is the alembic revision ID of the database revision
# required by this version of Tiled.
REQUIRED_REVISION = "4a9dfaba4a98"
# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = ["4a9dfaba4a98", "56809bcbfcb0", "722ff4e4fcc7", "481830dd6c11"]


async def create_default_roles(session):

    session.add_all(
        [
            Role(
                name="user",
                description="Default Role for users.",
                scopes=[
                    "read:metadata",
                    "read:data",
                    "create",
                    "write:metadata",
                    "write:data",
                    "apikeys",
                ],
            ),
            Role(
                name="admin",
                description="Role with elevated privileges.",
                scopes=[
                    "read:metadata",
                    "read:data",
                    "create",
                    "write:metadata",
                    "write:data",
                    "admin:apikeys",
                    "read:principals",
                    "metrics",
                ],
            ),
        ]
    )
    await session.commit()


async def initialize_database(engine):

    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as conn:
        # Create all tables.
        await conn.run_sync(Base.metadata.create_all)

        # Initialize Roles table.
        async with AsyncSession(engine) as session:
            await create_default_roles(session)

        # Mark current revision.
        with temp_alembic_ini(engine.url) as alembic_ini:
            alembic_cfg = Config(alembic_ini)
            await conn.run_sync(lambda conn: command.stamp(alembic_cfg, "head"))


async def upgrade(engine, revision):
    """
    Upgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine.url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        async with engine.connect() as conn:
            await conn.run_sync(lambda conn: command.upgrade(alembic_cfg, revision))


async def downgrade(engine, revision):
    """
    Downgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine.url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        async with engine.connect() as conn:
            await conn.run_sync(lambda conn: command.downgrade(alembic_cfg, revision))


class UnrecognizedDatabase(Exception):
    pass


class UninitializedDatabase(Exception):
    pass


class DatabaseUpgradeNeeded(Exception):
    pass


async def get_current_revision(engine):

    redacted_url = engine.url._replace(password="[redacted]")
    async with engine.connect() as conn:
        context = await conn.run_sync(migration.MigrationContext.configure)
        heads = await conn.run_sync(lambda conn: context.get_current_heads)
        heads = ()
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


async def check_database(engine):
    revision = await get_current_revision(engine)
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


async def create_user(session, identity_provider, id):
    user_role = (
        await session.execute(select(Role).filter(Role.name == "user"))
    ).scalar()
    assert user_role is not None, "User role is missing from Roles table"
    principal = Principal(type="user", roles=[user_role])
    session.add(principal)
    await session.commit()
    # db.refresh(principal)  # Refresh to sync back the auto-generated uuid.
    identity = Identity(
        provider=identity_provider,
        id=id,
        principal_id=principal.id,
    )
    session.add(identity)
    await session.commit()
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
    if session is None:
        return None
    if (
        session.expiration_time is not None
        and session.expiration_time < datetime.utcnow()
    ):
        db.delete(session)
        db.commit()
        return None
    return session


def lookup_valid_pending_session_by_device_code(db, device_code):
    hashed_device_code = hashlib.sha256(device_code).digest()
    pending_session = (
        db.query(PendingSession)
        .filter(PendingSession.hashed_device_code == hashed_device_code)
        .first()
    )
    if pending_session is None:
        return None
    if (
        pending_session.expiration_time is not None
        and pending_session.expiration_time < datetime.utcnow()
    ):
        db.delete(pending_session)
        db.commit()
        return None
    return pending_session


def lookup_valid_pending_session_by_user_code(db, user_code):
    pending_session = (
        db.query(PendingSession).filter(PendingSession.user_code == user_code).first()
    )
    if pending_session is None:
        return None
    if (
        pending_session.expiration_time is not None
        and pending_session.expiration_time < datetime.utcnow()
    ):
        db.delete(pending_session)
        db.commit()
        return None
    return pending_session


async def make_admin_by_identity(session, identity_provider, id):
    identity = (
        await session.execute(
            select(Identity)
            .filter(Identity.id == id)
            .filter(Identity.provider == identity_provider)
        )
    ).first()
    if identity is None:
        principal = await create_user(session, identity_provider, id)
    else:
        principal = identity.principal
    admin_role = (
        await session.execute(select(Role).filter(Role.name == "admin"))
    ).scalar()
    assert admin_role is not None, "Admin role is missing from Roles table"
    principal.roles.append(admin_role)
    await session.commit()
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
