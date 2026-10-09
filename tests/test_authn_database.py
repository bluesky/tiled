from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select

from tiled.authn_database import orm
from tiled.authn_database.core import create_user, initialize_database, purge_expired


@pytest.fixture
async def db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
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
