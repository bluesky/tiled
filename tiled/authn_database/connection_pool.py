from typing import Optional

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from ..server.settings import DatabaseSettings
from ..utils import ensure_specified_sql_driver


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
    return engine


async def close_database_connection_pool(engine: AsyncEngine) -> None:
    if engine is not None:
        await engine.dispose()


async def get_database_engine(request: Request) -> Optional[AsyncEngine]:
    "Return engine if multi-user server, None is single-user server."
    return request.app.state.authn_database_engine


async def get_database_session(
    engine: AsyncEngine = Depends(get_database_engine),
) -> Optional[AsyncSession]:
    # Special case for single-user mode
    if engine is None:
        yield None
    else:
        async with AsyncSession(
            engine, autoflush=False, expire_on_commit=False
        ) as session:
            yield session
