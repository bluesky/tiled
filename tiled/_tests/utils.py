import contextlib
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
from ..utils import ensure_specified_sql_driver

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
    engine = create_async_engine(ensure_specified_sql_driver(uri))
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
    Override getpass, when prompt_for_credentials with username only
    used like:

    >>> with enter_username_password(...):
    ...     # Run code that calls prompt_for_credentials and subsequently getpass.getpass().
    """

    original_username_input = context.username_input
    context.username_input = lambda: username
    original_password_input = context.password_input
    context.password_input = lambda: password
    try:
        # Ensures that raise in calling routine does not prevent context from being exited.
        yield
    finally:
        context.username_input = original_username_input
        context.password_input = original_password_input


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
