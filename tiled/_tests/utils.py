import contextlib
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from enum import IntEnum
from pathlib import Path

import httpx
import pytest
import uvicorn
from sqlalchemy import text
from sqlalchemy.engine import Connection
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


class Server(uvicorn.Server):
    # https://github.com/encode/uvicorn/discussions/1103#discussioncomment-941726

    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            # Wait for server to start up, or raise TimeoutError.
            for _ in range(100):
                time.sleep(0.1)
                if self.started:
                    break
            else:
                raise TimeoutError("Server did not start in 10 seconds.")
            host, port = self.servers[0].sockets[0].getsockname()
            yield f"http://{host}:{port}"
        finally:
            self.should_exit = True
            thread.join()


def sql_table_exists(conn: Connection, dialect: str, table_name: str) -> bool:
    """Check if a table exists in the SQLite database."""

    # Use a dialect-specific query and parameter style
    if dialect == "sqlite":
        query = f"""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='{table_name}';
        """
    elif dialect == "duckdb":
        query = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = '{table_name}';
        """
    elif dialect == "postgresql":
        query = f"""
            SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename = '{table_name}'
        """
        # NOTE: no trailing semicolon in PostgreSQL query as it is run as a COPY
    else:
        raise ValueError(f"Unsupported database dialect: {dialect}")

    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone() is not None
