from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select

from tiled.authn_database import orm
from tiled.authn_database.core import create_user, initialize_database, purge_expired


@pytest.fixture
async def db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    # Enforce foreign keys (including ON DELETE actions), as the server's
    # connection pool does. SQLite leaves them off by default.
    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await initialize_database(engine)
    async with AsyncSession(engine, autoflush=False, expire_on_commit=False) as session:
        yield session
    await engine.dispose()


async def test_purge_expired_api_keys(db_session):
    principal = await create_user(db_session, "test", "alice")
    now = datetime.now(timezone.utc)
    expired = orm.APIKey(
        principal_id=principal.id,
        expiration_time=now - timedelta(seconds=10),
        scopes=["inherit"],
        first_eight="aaaaaaaa",
        hashed_secret=b"0" * 32,
    )
    valid = orm.APIKey(
        principal_id=principal.id,
        expiration_time=now + timedelta(seconds=600),
        scopes=["inherit"],
        first_eight="bbbbbbbb",
        hashed_secret=b"1" * 32,
    )
    never_expires = orm.APIKey(
        principal_id=principal.id,
        expiration_time=None,
        scopes=["inherit"],
        first_eight="cccccccc",
        hashed_secret=b"2" * 32,
    )
    db_session.add_all([expired, valid, never_expires])
    await db_session.commit()

    num_purged = await purge_expired(db_session, orm.APIKey)
    assert num_purged == 1

    remaining = {
        key.first_eight
        for key in (await db_session.execute(select(orm.APIKey))).unique().scalars()
    }
    assert remaining == {"bbbbbbbb", "cccccccc"}


async def test_purge_expired_sessions(db_session):
    principal = await create_user(db_session, "test", "alice")
    now = datetime.now(timezone.utc)
    expired = orm.Session(
        principal_id=principal.id,
        expiration_time=now - timedelta(seconds=10),
        state={},
    )
    valid = orm.Session(
        principal_id=principal.id,
        expiration_time=now + timedelta(seconds=600),
        state={},
    )
    db_session.add_all([expired, valid])
    await db_session.commit()

    num_purged = await purge_expired(db_session, orm.Session)
    assert num_purged == 1

    remaining = (await db_session.execute(select(orm.Session))).unique().scalars().all()
    assert [s.id for s in remaining] == [valid.id]


async def test_purge_expired_pending_sessions(db_session):
    now = datetime.now(timezone.utc)
    expired = orm.PendingSession(
        hashed_device_code=b"e" * 32,
        user_code="AAAABBBB",
        expiration_time=now - timedelta(seconds=10),
    )
    valid = orm.PendingSession(
        hashed_device_code=b"v" * 32,
        user_code="CCCCDDDD",
        expiration_time=now + timedelta(seconds=600),
    )
    db_session.add_all([expired, valid])
    await db_session.commit()

    num_purged = await purge_expired(db_session, orm.PendingSession)
    assert num_purged == 1

    remaining = (
        (await db_session.execute(select(orm.PendingSession))).unique().scalars().all()
    )
    assert [p.hashed_device_code for p in remaining] == [b"v" * 32]


async def test_purging_session_nulls_pending_session_reference(db_session):
    """
    Purging an expired Session must not be blocked by a pending device-code
    login that still references it: `ON DELETE SET NULL` detaches the
    reference instead, returning the pending session to its unapproved state.
    """
    principal = await create_user(db_session, "test", "alice")
    now = datetime.now(timezone.utc)
    expired_session = orm.Session(
        principal_id=principal.id,
        expiration_time=now - timedelta(seconds=10),
        state={},
    )
    db_session.add(expired_session)
    await db_session.commit()
    pending = orm.PendingSession(
        hashed_device_code=b"p" * 32,
        user_code="EEEEFFFF",
        expiration_time=now + timedelta(seconds=600),
        session_id=expired_session.id,
    )
    db_session.add(pending)
    await db_session.commit()

    num_purged = await purge_expired(db_session, orm.Session)
    assert num_purged == 1

    assert (
        await db_session.execute(select(orm.Session))
    ).unique().scalars().all() == []
    # The pending session survives, with its reference nulled.
    session_ids = (
        (await db_session.execute(select(orm.PendingSession.session_id)))
        .scalars()
        .all()
    )
    assert session_ids == [None]
