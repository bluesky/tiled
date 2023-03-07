from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from ..server.settings import get_settings

# A given process probably only has one of these at a time, but we
# key on database_settings just case in some testing context or something
# we have two servers running in the same process.
_connection_pools = {}


def open_database_connection_pool(database_settings):
    connect_args = {}
    kwargs = {}  # extra kwargs passed to create_engine
    # kwargs["pool_size"] = database_settings.pool_size
    # kwargs["pool_pre_ping"] = database_settings.pool_pre_ping
    # kwargs["max_overflow"] = database_settings.max_overflow
    engine = create_async_engine(
        database_settings.uri, connect_args=connect_args, **kwargs
    )
    _connection_pools[database_settings] = engine
    return engine


async def close_database_connection_pool(database_settings):
    engine = _connection_pools.pop(database_settings, None)
    if engine is not None:
        await engine.dispose()


async def get_database_engine(settings=Depends(get_settings)):
    # Special case for single-user mode
    if settings.database_uri is None:
        return None
    try:
        return _connection_pools[settings.database_settings]
    except KeyError:
        raise RuntimeError(
            f"Could not find connection pool for {settings.database_settings}"
        )


async def get_database_session(engine=Depends(get_database_engine)):
    # Special case for single-user mode
    if engine is None:
        yield None
    else:
        async with AsyncSession(
            engine, autoflush=False, expire_on_commit=False
        ) as session:
            yield session
