from pathlib import Path

import pytest

from ..utils import ensure_specified_sql_driver, parse_time_string


def test_ensure_specified_sql_driver():
    # Postgres
    # Default driver is added if missing.
    assert (
        ensure_specified_sql_driver(
            "postgresql://user:password@localhost:5432/database"
        )
        == "postgresql+asyncpg://user:password@localhost:5432/database"
    )
    # Default driver passes through if specified.
    assert (
        ensure_specified_sql_driver(
            "postgresql+asyncpg://user:password@localhost:5432/database"
        )
        == "postgresql+asyncpg://user:password@localhost:5432/database"
    )
    # Do not override user-provided.
    assert (
        ensure_specified_sql_driver(
            "postgresql+custom://user:password@localhost:5432/database"
        )
        == "postgresql+custom://user:password@localhost:5432/database"
    )

    # SQLite
    # Default driver is added if missing.
    assert (
        ensure_specified_sql_driver("sqlite:////test.db")
        == "sqlite+aiosqlite:////test.db"
    )
    # Default driver passes through if specified.
    assert (
        ensure_specified_sql_driver("sqlite+aiosqlite:////test.db")
        == "sqlite+aiosqlite:////test.db"
    )
    # Do not override user-provided.
    assert (
        ensure_specified_sql_driver("sqlite+custom:////test.db")
        == "sqlite+custom:////test.db"
    )
    # Handle SQLite :memory: URIs
    assert (
        ensure_specified_sql_driver("sqlite+aiosqlite://:memory:")
        == "sqlite+aiosqlite://:memory:"
    )
    assert (
        ensure_specified_sql_driver("sqlite://:memory:")
        == "sqlite+aiosqlite://:memory:"
    )
    # Handle SQLite relative URIs
    assert (
        ensure_specified_sql_driver("sqlite+aiosqlite:///test.db")
        == "sqlite+aiosqlite:///test.db"
    )
    assert (
        ensure_specified_sql_driver("sqlite:///test.db")
        == "sqlite+aiosqlite:///test.db"
    )
    # Filepaths are implicitly SQLite databases.
    # Relative path
    assert ensure_specified_sql_driver("test.db") == "sqlite+aiosqlite:///test.db"
    # Path object
    assert ensure_specified_sql_driver(Path("test.db")) == "sqlite+aiosqlite:///test.db"
    # Relative path anchored to .
    assert ensure_specified_sql_driver("./test.db") == "sqlite+aiosqlite:///test.db"
    # Absolute path
    assert (
        ensure_specified_sql_driver(Path("/tmp/test.db"))
        == f"sqlite+aiosqlite:///{Path('/tmp/test.db')}"
    )


@pytest.mark.parametrize(
    "string_input,expected",
    [
        ("3s", 3),
        ("7m", 7 * 60),
        ("5h", 5 * 60 * 60),
        ("1d", 1 * 24 * 60 * 60),
        ("2y", 2 * 365 * 24 * 60 * 60),
    ],
)
def test_parse_time_string_valid(string_input, expected):
    assert parse_time_string(string_input) == expected


@pytest.mark.parametrize(
    "string_input",
    [
        "3z",  # unrecognized units
        "3M",  # unrecognized units
        "-3m",  # invalid character '-'
        "3 m",  # invalid character '-'
    ],
)
def test_parse_time_string_invalid(string_input):
    with pytest.raises(ValueError):
        parse_time_string(string_input)
