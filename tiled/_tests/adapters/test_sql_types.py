import os
from pathlib import Path
from typing import AsyncGenerator, Generator

import adbc_driver_sqlite
import pyarrow as pa
import pytest
import pytest_asyncio

from tiled._tests.utils import temp_postgres
from tiled.adapters.sql import arrow_schema_to_create_table, create_connection


@pytest_asyncio.fixture
async def postgres_uri() -> AsyncGenerator[str, None]:
    uri = os.getenv("TILED_TEST_POSTGRESQL_URI")
    if uri is None:
        pytest.skip("TILED_TEST_POSTGRESQL_URI is not set")

    async with temp_postgres(uri) as uri_with_database_name:
        yield uri_with_database_name


@pytest.fixture
def sqlite_uri(tmp_path: Path) -> Generator[str, None, None]:
    yield f"sqlite:///{tmp_path}/test.db"


@pytest.fixture
def duckdb_uri(tmp_path: Path) -> Generator[str, None, None]:
    yield f"duckdb:///{tmp_path}/test.db"


def test_sql_data_types(sqlite_uri: str) -> None:
    schema = pa.schema([("some_int", pa.int32()), ("some_string", pa.string())])
    query = arrow_schema_to_create_table(schema, "random_test_table", "sqlite")
    print("querry here", query)
    assert query.find("INTEGER")
    assert query.find("TEXT")

    conn = create_connection(sqlite_uri)
    assert isinstance(conn, adbc_driver_sqlite.dbapi.Connection)

    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS random_test_table")
        cursor.execute(query)
    conn.commit()

    print("the out", conn.adbc_get_table_schema("random_test_table"))
    assert conn.adbc_get_table_schema("random_test_table") == pa.schema(
        [
            ("some_int", "int32"),
            ("some_string", "string"),
        ]
    )
    conn.close()
