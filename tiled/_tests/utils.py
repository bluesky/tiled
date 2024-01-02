import contextlib
import getpass
import uuid

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ..client import context


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
def enter_password(password):
    """
    Override getpass, used like:

    >>> with enter_password(...):
    ...     # Run code that calls getpass.getpass().
    """

    original_prompt = context.PROMPT_FOR_REAUTHENTICATION
    original_getpass = getpass.getpass
    context.PROMPT_FOR_REAUTHENTICATION = True
    setattr(getpass, "getpass", lambda: password)
    yield
    setattr(getpass, "getpass", original_getpass)
    context.PROMPT_FOR_REAUTHENTICATION = original_prompt
