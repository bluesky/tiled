import contextlib
import getpass
import sqlite3
import sys
import tempfile
import uuid
from enum import IntEnum
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ..client import context
from ..client.base import BaseClient

if sys.version_info < (3, 9):
    import importlib_resources as resources
else:
    from importlib import resources  # Python >= 3.9 only


@contextlib.contextmanager
def fail_with_status_code(status_code):
    with pytest.raises(httpx.HTTPStatusError) as info:
        yield info
    assert info.value.response.status_code == status_code


@contextlib.asynccontextmanager
async def temp_postgres(uri):
    if uri.endswith("/"):
        uri = uri[:-1]
    # Create a fresh database.
    engine = create_async_engine(uri)
    database_name = f"tiled_test_disposable_{uuid.uuid4().hex}"
    async with engine.connect() as connection:
        await connection.execute(
            text("COMMIT")
        )  # close the automatically-started transaction
        await connection.execute(text(f"CREATE DATABASE {database_name};"))
        await connection.commit()
    yield f"{uri}/{database_name}"
    # Drop the database.
    async with engine.connect() as connection:
        await connection.execute(
            text("COMMIT")
        )  # close the automatically-started transaction
        await connection.execute(text(f"DROP DATABASE {database_name};"))
        await connection.commit()


@contextlib.contextmanager
def enter_username_password(username, password):
    """
    Override getpass, used like:

    >>> with enter_password(...):
    ...     # Run code that calls getpass.getpass().
    """

    original_prompt = context.PROMPT_FOR_REAUTHENTICATION
    original_getusername = context.prompt_for_username
    original_getpass = getpass.getpass
    context.PROMPT_FOR_REAUTHENTICATION = True
    context.prompt_for_username = lambda u: username
    setattr(getpass, "getpass", lambda: password)
    yield
    setattr(getpass, "getpass", original_getpass)
    context.PROMPT_FOR_REAUTHENTICATION = original_prompt
    context.prompt_for_username = original_getusername


class URL_LIMITS(IntEnum):
    HUGE = 80_000
    DEFAULT = BaseClient.URL_CHARACTER_LIMIT
    TINY = 10


@contextlib.contextmanager
def sqlite_from_dump(filename):
    """Create a SQLite db in a temporary directory, loading a SQL script.

    SQL script should be given as a filename, assumed to be in tiled/_tests/sql/
    """
    with tempfile.TemporaryDirectory() as directory:
        database_path = Path(directory, "catalog.db")
        conn = sqlite3.connect(database_path)
        ref = resources.files("tiled._tests.sql") / filename
        with resources.as_file(ref) as path:
            conn.executescript(path.read_text())
        conn.close()
        yield database_path
