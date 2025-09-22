import os
from collections.abc import AsyncGenerator
from typing import Optional, Union

from fastapi import Depends
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from ..server.settings import DatabaseSettings, Settings, get_settings
from ..utils import ensure_specified_sql_driver, safe_json_dump

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))

# A given process probably only has one of these at a time, but we
# key on database_settings just case in some testing context or something
# we have two servers running in the same process.
_connection_pools: dict[DatabaseSettings, AsyncEngine] = {}


def open_database_connection_pool(database_settings: DatabaseSettings) -> AsyncEngine:
    parsed_url = make_url(database_settings.uri)
    if (
        (parsed_url.get_dialect().name == "sqlite")
        and (parsed_url.database != ":memory:")
        and (parsed_url.query.get("mode", None) != "memory")
    ):
        # For file-backed SQLite databases, connection pooling offers a
        # significant performance boost. For SQLite databases that exist
        # only in process memory, pooling is not applicable.
        poolclass = AsyncAdaptedQueuePool
    elif parsed_url.get_dialect().name.startswith("postgresql"):
        poolclass = AsyncAdaptedQueuePool  # Default for PostgreSQL
    else:
        poolclass = None  # defer to sqlalchemy default

    # Optionally set pool size and max overflow for the catalog engine;
    # if not specified, use the default values from sqlalchemy.
    pool_kwargs = (
        {
            "pool_size": database_settings.pool_size,
            "max_overflow": database_settings.max_overflow,
            "pool_pre_ping": database_settings.pool_pre_ping,
        }
        if poolclass == AsyncAdaptedQueuePool
        else {}
    )
    pool_kwargs = {k: v for k, v in pool_kwargs.items() if v is not None}

    # Create the async engine with the specified parameters
    engine = create_async_engine(
        ensure_specified_sql_driver(database_settings.uri),
        echo=DEFAULT_ECHO,
        json_serializer=json_serializer,
        poolclass=poolclass,
        **pool_kwargs,
    )
    if engine.dialect.name == "sqlite":
        event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragma)

    _connection_pools[database_settings] = engine

    return engine


async def close_database_connection_pool(database_settings: DatabaseSettings):
    engine = _connection_pools.pop(database_settings, None)
    if engine is not None:
        await engine.dispose()


async def get_database_engine(
    settings: Union[Settings, DatabaseSettings] = Depends(get_settings),
) -> AsyncEngine:
    database_settings = (
        settings.database_settings if isinstance(settings, Settings) else settings
    )
    # Special case for single-user mode
    if database_settings.uri is None:
        return None
    if database_settings in _connection_pools:
        return _connection_pools[database_settings]
    else:
        return open_database_connection_pool(database_settings)


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


def json_serializer(obj):
    "The PostgreSQL JSON serializer requires str, not bytes."
    return safe_json_dump(obj).decode()


def _set_sqlite_pragma(conn, record):
    cursor = conn.cursor()
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#foreign-key-support
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
