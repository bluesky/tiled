from collections.abc import AsyncGenerator
from typing import Optional

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from ..server.settings import DatabaseSettings, Settings, get_settings
from ..utils import ensure_specified_sql_driver

# A given process probably only has one of these at a time, but we
# key on database_settings just case in some testing context or something
# we have two servers running in the same process.
_connection_pools: dict[DatabaseSettings, AsyncEngine] = {}


def open_database_connection_pool(database_settings: DatabaseSettings) -> AsyncEngine:
    connect_args = {}
    kwargs = {}  # extra kwargs passed to create_engine
    # kwargs["pool_size"] = database_settings.pool_size
    # kwargs["pool_pre_ping"] = database_settings.pool_pre_ping
    # kwargs["max_overflow"] = database_settings.max_overflow
    engine = create_async_engine(
        ensure_specified_sql_driver(database_settings.uri),
        connect_args=connect_args,
        **kwargs,
    )
    _connection_pools[database_settings] = engine
    return engine


async def close_database_connection_pool(database_settings: DatabaseSettings):
    engine = _connection_pools.pop(database_settings, None)
    if engine is not None:
        await engine.dispose()


async def get_database_engine(
    settings: Settings = Depends(get_settings),
) -> AsyncEngine:
    # Special case for single-user mode
    if settings.database_settings.uri is None:
        return None
    try:
        return _connection_pools[settings.database_settings]
    except KeyError:
        raise RuntimeError(
            f"Could not find connection pool for {settings.database_settings}"
        )


async def get_database_session(
    engine: AsyncEngine = Depends(get_database_engine),
) -> AsyncGenerator[Optional[AsyncSession]]:
    # Special case for single-user mode
    if engine is None:
        yield None
    else:
        async with AsyncSession(
            engine, autoflush=False, expire_on_commit=False
        ) as session:
            yield session
