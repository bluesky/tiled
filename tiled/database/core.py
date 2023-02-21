import hashlib
import uuid as uuid_module
from datetime import datetime

from alembic import command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import func

from .alembic_utils import temp_alembic_ini
from .base import Base
from .orm import APIKey, Identity, PendingSession, Principal, Role, Session

# This is the alembic revision ID of the database revision
# required by this version of Tiled.
REQUIRED_REVISION = "4a9dfaba4a98"
# This is list of all valid revisions (from current to oldest).
ALL_REVISIONS = ["4a9dfaba4a98", "56809bcbfcb0", "722ff4e4fcc7", "481830dd6c11"]


async def create_default_roles(db_session):

    db_session.add_all(
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
    await db_session.commit()


async def initialize_database(engine):

    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    async with engine.connect() as conn:
        # Create all tables.
        await conn.run_sync(Base.metadata.create_all)

        # Initialize Roles table.
        async with AsyncSession(engine) as db_session:
            await create_default_roles(db_session)


def stamp_head(engine_url):
    """
    Upgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine_url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.stamp(alembic_cfg, "head")


def upgrade(engine_url, revision):
    """
    Upgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine_url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.upgrade(alembic_cfg, revision)


def downgrade(engine_url, revision):
    """
    Downgrade schema to the specified revision.
    """
    with temp_alembic_ini(engine_url) as alembic_ini:
        alembic_cfg = Config(alembic_ini)
        command.downgrade(alembic_cfg, revision)


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
        heads = await conn.run_sync(lambda conn: context.get_current_heads())
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


async def purge_expired(db_session, cls):
    """
    Remove expired entries.
    """
    now = datetime.utcnow()
    num_expired = 0
    statement = (
        select(cls)
        .filter(cls.expiration_time.is_not(None))
        .filter(cls.expiration_time < now)
    )
    result = await db_session.execute(statement)
    for obj in result.scalars():
        num_expired += 1
        db_session.delete(obj)
    if num_expired:
        await db_session.commit()
    return num_expired


async def create_user(db_session, identity_provider, id):
    user_role = (
        await db_session.execute(select(Role).filter(Role.name == "user"))
    ).scalar()
    assert user_role is not None, "User role is missing from Roles table"
    principal = Principal(type="user", roles=[user_role])
    db_session.add(principal)
    await db_session.commit()
    identity = Identity(
        provider=identity_provider,
        id=id,
        principal_id=principal.id,
    )
    db_session.add(identity)
    await db_session.commit()
    refreshed_principal = (
        await db_session.execute(
            select(Principal)
            .filter(Principal.id == principal.id)
            .options(selectinload(Principal.identities))
        )
    ).scalar()
    return refreshed_principal


async def lookup_valid_session(db_session, session_id):
    if isinstance(session_id, int):
        # Old versions of tiled used an integer sid.
        # Reject any of those old sessions and force reauthentication.
        return None

    session = (
        await db_session.execute(
            select(Session).filter(Session.uuid == uuid_module.UUID(hex=session_id))
        )
    ).scalar()
    if session is None:
        return None
    if (
        session.expiration_time is not None
        and session.expiration_time < datetime.utcnow()
    ):
        db_session.delete(session)
        await db_session.commit()
        return None
    return session


async def lookup_valid_pending_session_by_device_code(db_session, device_code):
    hashed_device_code = hashlib.sha256(device_code).digest()
    pending_session = (
        await db_session.execute(
            select(PendingSession).filter(
                PendingSession.hashed_device_code == hashed_device_code
            )
        )
    ).scalar()
    if pending_session is None:
        return None
    if (
        pending_session.expiration_time is not None
        and pending_session.expiration_time < datetime.utcnow()
    ):
        db_session.delete(pending_session)
        await db_session.commit()
        return None
    return pending_session


async def lookup_valid_pending_session_by_user_code(db_session, user_code):
    pending_session = (
        await db_session.execute(
            select(PendingSession).filter(PendingSession.user_code == user_code)
        )
    ).scalar()
    if pending_session is None:
        return None
    if (
        pending_session.expiration_time is not None
        and pending_session.expiration_time < datetime.utcnow()
    ):
        db_session.delete(pending_session)
        await db_session.commit()
        return None
    return pending_session


async def make_admin_by_identity(db_session, identity_provider, id):
    identity = (
        await db_session.execute(
            select(Identity)
            .filter(Identity.id == id)
            .filter(Identity.provider == identity_provider)
        )
    ).first()
    if identity is None:
        principal = await create_user(db_session, identity_provider, id)
    else:
        principal = identity.principal
    admin_role = (
        await db_session.execute(select(Role).filter(Role.name == "admin"))
    ).scalar()
    assert admin_role is not None, "Admin role is missing from Roles table"
    principal.roles.append(admin_role)
    await db_session.commit()
    return principal


async def lookup_valid_api_key(db_session, secret):
    """
    Look up an API key. Ensure that it is valid.
    """

    now = datetime.utcnow()
    hashed_secret = hashlib.sha256(secret).digest()
    api_key = (
        await db_session.execute(
            select(APIKey)
            .filter(APIKey.first_eight == secret.hex()[:8])
            .filter(APIKey.hashed_secret == hashed_secret)
        )
    ).scalar()
    if api_key is None:
        # No match
        validated_api_key = None
    elif (api_key.expiration_time is not None) and (api_key.expiration_time < now):
        # Match is expired. Delete it.
        db_session.delete(api_key)
        await db_session.commit()
        validated_api_key = None
    elif api_key.principal is None:
        # The Principal for the API key no longer exists. Delete it.
        db_session.delete(api_key)
        await db_session.commit()
        validated_api_key = None
    else:
        validated_api_key = api_key
    return validated_api_key


async def latest_principal_activity(db_session, principal):
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
        await db_session.execute(
            select(func.max(Identity.latest_login)).filter(
                Identity.principal_id == principal.id
            )
        )
    ).scalar()
    latest_session_activity = (
        await db_session.execute(
            select(func.max(Session.time_last_refreshed)).filter(
                Session.principal_id == principal.id
            )
        )
    ).scalar()
    latest_api_key_activity = (
        await db_session.execute(
            select(func.max(APIKey.latest_activity)).filter(
                APIKey.principal_id == principal.id
            )
        )
    ).scalar()
    all_activity = [
        latest_identity_activity,
        latest_api_key_activity,
        latest_session_activity,
    ]
    if all([t is None for t in all_activity]):
        return None
    return max(t for t in all_activity if t is not None)
