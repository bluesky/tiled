import hashlib
import uuid as uuid_module
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import func

from .base import Base
from .orm import APIKey, Identity, PendingSession, Principal, Role, Session

# This is list of all valid alembic revisions (from current to oldest).
ALL_REVISIONS = [
    "a806cc635ab2",
    "0c705a02954c",
    "d88e91ea03f9",
    "13024b8a6b74",
    "769180ce732e",
    "c7bd2573716d",
    "4a9dfaba4a98",
    "56809bcbfcb0",
    "722ff4e4fcc7",
    "481830dd6c11",
]
REQUIRED_REVISION = ALL_REVISIONS[0]


async def create_default_roles(db):
    default_roles = [
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
                "register",
                "write:metadata",
                "write:data",
                "admin:apikeys",
                "read:principals",
                "write:principals",
                "metrics",
            ],
        ),
    ]

    roles_result = await db.execute(select(Role.name))
    existing_role_names = set(roles_result.scalars().all())
    roles_to_add = [
        role for role in default_roles if role.name not in existing_role_names
    ]

    if roles_to_add:
        db.add_all(roles_to_add)
        await db.commit()


async def initialize_database(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        # Create all tables.
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

        # Initialize Roles table.
        async with AsyncSession(engine) as db:
            await create_default_roles(db)


async def purge_expired(db: AsyncSession, cls) -> int:
    """
    Remove expired entries.
    """
    now = datetime.now(timezone.utc)
    num_expired = 0
    statement = (
        select(cls)
        .filter(cls.expiration_time.is_not(None))
        .filter(cls.expiration_time.replace(tzinfo=timezone.utc) < now)
    )
    result = await db.execute(statement)
    for obj in result.scalars():
        num_expired += 1
        await db.delete(obj)
    if num_expired:
        await db.commit()
    return num_expired


async def create_user(db: AsyncSession, identity_provider: str, id: str) -> Principal:
    user_role = (await db.execute(select(Role).filter(Role.name == "user"))).scalar()
    assert user_role is not None, "User role is missing from Roles table"
    principal = Principal(type="user", roles=[user_role])
    db.add(principal)
    await db.commit()
    identity = Identity(
        provider=identity_provider,
        id=id,
        principal_id=principal.id,
    )
    db.add(identity)
    await db.commit()
    refreshed_principal = (
        await db.execute(
            select(Principal)
            .filter(Principal.id == principal.id)
            .options(selectinload(Principal.identities))
        )
    ).scalar()
    assert (
        refreshed_principal is not None
    ), f"Newly created user {id} ({identity_provider}) missing from Principals table"
    return refreshed_principal


async def create_service(db: AsyncSession, role: str) -> Principal:
    role_ = (await db.execute(select(Role).filter(Role.name == role))).scalar()
    if role_ is None:
        raise ValueError(f"Role named {role!r} is not found")
    principal = Principal(type="service", roles=[role_])
    db.add(principal)
    await db.commit()
    return principal


async def lookup_valid_session(db: AsyncSession, session_id: str) -> Optional[Session]:
    if isinstance(session_id, int):
        # Old versions of tiled used an integer sid.
        # Reject any of those old sessions and force reauthentication.
        return None

    session = (
        await db.execute(
            select(Session)
            .options(
                selectinload(Session.principal).selectinload(Principal.roles),
                selectinload(Session.principal).selectinload(Principal.identities),
            )
            .filter(Session.uuid == uuid_module.UUID(hex=session_id))
        )
    ).scalar()
    if session is None:
        return None
    if session.expiration_time is not None and session.expiration_time.replace(
        tzinfo=timezone.utc
    ) < datetime.now(timezone.utc):
        await db.delete(session)
        await db.commit()
        return None
    return session


async def lookup_valid_pending_session_by_device_code(
    db: AsyncSession, device_code: str
) -> Optional[PendingSession]:
    hashed_device_code = hashlib.sha256(device_code).digest()
    pending_session = (
        await db.execute(
            select(PendingSession)
            .filter(PendingSession.hashed_device_code == hashed_device_code)
            .options(
                selectinload(PendingSession.session)
                .selectinload(Session.principal)
                .selectinload(Principal.identities),
            )
        )
    ).scalar()
    if pending_session is None:
        return None
    if (
        pending_session.expiration_time is not None
        and pending_session.expiration_time.replace(tzinfo=timezone.utc)
        < datetime.now(timezone.utc)
    ):
        await db.delete(pending_session)
        await db.commit()
        return None
    return pending_session


async def lookup_valid_pending_session_by_user_code(
    db: AsyncSession, user_code: str
) -> Optional[PendingSession]:
    pending_session = (
        await db.execute(
            select(PendingSession).filter(PendingSession.user_code == user_code)
        )
    ).scalar()
    if pending_session is None:
        return None
    if (
        pending_session.expiration_time is not None
        and pending_session.expiration_time.replace(tzinfo=timezone.utc)
        < datetime.now(timezone.utc)
    ):
        await db.delete(pending_session)
        await db.commit()
        return None
    return pending_session


async def make_admin_by_identity(
    db: AsyncSession, identity_provider: str, id: str
) -> Principal:
    identity = (
        await db.execute(
            select(Identity)
            .options(selectinload(Identity.principal).selectinload(Principal.roles))
            .filter(Identity.id == id)
            .filter(Identity.provider == identity_provider)
        )
    ).scalar()
    if identity is None:
        principal = await create_user(db, identity_provider, id)
    else:
        principal = identity.principal

    # check if principal already has admin role
    for role in principal.roles:
        if role.name == "admin":
            return principal

    admin_role = (await db.execute(select(Role).filter(Role.name == "admin"))).scalar()
    assert admin_role is not None, "Admin role is missing from Roles table"
    principal.roles.append(admin_role)
    await db.commit()
    return principal


async def lookup_valid_api_key(db: AsyncSession, secret: bytes) -> Optional[APIKey]:
    """
    Look up an API key. Ensure that it is valid.
    """

    now = datetime.now(timezone.utc)
    hashed_secret = hashlib.sha256(secret).digest()
    api_key = (
        await db.execute(
            select(APIKey)
            .options(
                selectinload(APIKey.principal).selectinload(Principal.roles),
                selectinload(APIKey.principal).selectinload(Principal.identities),
                selectinload(APIKey.principal).selectinload(Principal.sessions),
            )
            .filter(APIKey.first_eight == secret.hex()[:8])
            .filter(APIKey.hashed_secret == hashed_secret)
        )
    ).scalar()
    if api_key is None:
        # No match
        validated_api_key = None
    elif (api_key.expiration_time is not None) and (
        api_key.expiration_time.replace(tzinfo=timezone.utc) < now
    ):
        # Match is expired. Delete it.
        await db.delete(api_key)
        await db.commit()
        validated_api_key = None
    elif api_key.principal is None:
        # The Principal for the API key no longer exists. Delete it.
        await db.delete(api_key)
        await db.commit()
        validated_api_key = None
    else:
        validated_api_key = api_key
    return validated_api_key


async def latest_principal_activity(
    db: AsyncSession, principal: Principal
) -> Optional[datetime]:
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
        await db.execute(
            select(func.max(Identity.latest_login)).filter(
                Identity.principal_id == principal.id
            )
        )
    ).scalar()
    latest_session_activity = (
        await db.execute(
            select(func.max(Session.time_last_refreshed)).filter(
                Session.principal_id == principal.id
            )
        )
    ).scalar()
    latest_api_key_activity = (
        await db.execute(
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
