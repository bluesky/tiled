import os
import sys
from collections.abc import AsyncGenerator
from typing import Callable, Optional, Union

from fastapi import Depends
from sqlalchemy import event
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, StaticPool

from ..server.settings import DatabaseSettings, Settings, get_settings
from ..utils import ensure_specified_sql_driver, safe_json_dump, sanitize_uri
from .metrics import monitor_db_pool

# contextlib.nullcontext got async context manager support in 3.10
if sys.version_info < (3, 10):
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    class nullcontext(AbstractContextManager, AbstractAsyncContextManager):
        """Context manager that does no additional processing.

        Used as a stand-in for a normal context manager, when a particular
        block of code is only sometimes used with a normal context manager:

        cm = optional_cm if condition else nullcontext()
        with cm:
            # Perform operation, using optional_cm if condition is True
        """

        def __init__(self, enter_result=None):
            self.enter_result = enter_result

        def __enter__(self):
            return self.enter_result

        def __exit__(self, *excinfo):
            pass

        async def __aenter__(self):
            return self.enter_result

        async def __aexit__(self, *excinfo):
            pass

else:
    from contextlib import nullcontext

DEFAULT_ECHO = bool(int(os.getenv("TILED_ECHO_SQL", "0") or "0"))

# A given process probably only has one of these at a time, but we
# key on database_settings just case in some testing context or something
# we have two servers running in the same process.
_connection_pools: dict[DatabaseSettings, AsyncEngine] = {}


def open_database_connection_pool(database_settings: DatabaseSettings) -> AsyncEngine:
    if is_memory_sqlite(database_settings.uri):
        engine = create_async_engine(
            ensure_specified_sql_driver(database_settings.uri),
            echo=DEFAULT_ECHO,
            json_serializer=json_serializer,
            poolclass=StaticPool,
        )
    else:
        engine = create_async_engine(
            ensure_specified_sql_driver(database_settings.uri),
            echo=DEFAULT_ECHO,
            json_serializer=json_serializer,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=database_settings.pool_size,
            max_overflow=database_settings.max_overflow,
            pool_pre_ping=database_settings.pool_pre_ping,
        )

    # Cache the engine so we don't create more than one pool per database_settings.
    monitor_db_pool(engine.pool, sanitize_uri(database_settings.uri)[0])
    _connection_pools[database_settings] = engine

    # For SQLite, ensure that foreign key constraints are enforced.
    if engine.dialect.name == "sqlite":
        event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragma)

    return engine


async def close_database_connection_pool(database_settings: DatabaseSettings):
    engine = _connection_pools.pop(database_settings, None)
    if engine is not None:
        await engine.dispose()


def get_database_engine(
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


async def get_database_session_factory(
    engine: AsyncEngine = Depends(get_database_engine),
) -> AsyncGenerator[Callable[[], Optional[AsyncSession]]]:
    # Special case for single-user mode
    if engine is None:

        def f():
            return nullcontext()

    else:
        # Let the caller manage the lifecycle of the AsyncSession.
        def f():
            return AsyncSession(engine, autoflush=False, expire_on_commit=False)

    yield f


def json_serializer(obj):
    "The PostgreSQL JSON serializer requires str, not bytes."
    return safe_json_dump(obj).decode()


def _set_sqlite_pragma(conn, record):
    # Support FOREIGN KEY constraint syntax in SQLite; see:
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#foreign-key-support
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def is_memory_sqlite(url: Union[URL, str]) -> bool:
    """
    Check if a SQLAlchemy URL is a memory-backed SQLite database.

    Handles various memory database URL formats:
    - sqlite:///:memory:
    - sqlite:///file::memory:?cache=shared
    - sqlite://
    - etc.
    """
    url = make_url(url)
    # Check if it's SQLite at all
    if url.get_dialect().name != "sqlite":
        return False

    # Check if database is None or empty (default memory DB)
    if not url.database:
        return True

    # Check for explicit :memory: string (case-insensitive)
    database = str(url.database).lower()
    if ":memory:" in database:
        return True

    # Check for mode=memory query parameter
    if (mode := url.query.get("mode")) and mode.lower() == "memory":
        return True

    return False
