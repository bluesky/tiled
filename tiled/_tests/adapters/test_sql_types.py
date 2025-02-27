import os
from pathlib import Path
from typing import AsyncGenerator, Generator, List

import adbc_driver_duckdb
import adbc_driver_postgresql
import adbc_driver_sqlite

# import adbc_driver_sqlite
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
        # yield uri_with_database_name.rsplit("/", 1)[0]


@pytest.fixture
def sqlite_uri(tmp_path: Path) -> Generator[str, None, None]:
    yield f"sqlite:///{tmp_path}/test.db"


@pytest.fixture
def duckdb_uri(tmp_path: Path) -> Generator[str, None, None]:
    yield f"duckdb:///{tmp_path}/test.db"


@pytest.mark.parametrize(
    "actual_schema, expected_schema, expected_keywords",
    [
        (
            pa.schema([("some_bool", pa.bool_())]),
            pa.schema([("some_bool", "bool")]),
            ["BOOLEAN"],
        ),
        (
            pa.schema([("some_int8", pa.int8())]),
            pa.schema([("some_int8", "int8")]),
            ["TINYINT"],
        ),
        (
            pa.schema([("some_uint8", pa.uint8())]),
            pa.schema([("some_uint8", "uint8")]),
            ["UTINYINT"],
        ),
        (
            pa.schema([("some_int16", pa.int16())]),
            pa.schema([("some_int16", "int16")]),
            ["SMALLINT"],
        ),
        (
            pa.schema([("some_uint16", pa.uint16())]),
            pa.schema([("some_uint16", "uint16")]),
            ["USMALLINT"],
        ),
        (
            pa.schema([("some_int32", pa.int32())]),
            pa.schema([("some_int32", "int32")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_uint32", pa.uint32())]),
            pa.schema([("some_uint32", "uint32")]),
            ["UINTEGER"],
        ),
        (
            pa.schema([("some_int64", pa.int64())]),
            pa.schema([("some_int64", "int64")]),
            ["BIGINT"],
        ),
        (
            pa.schema([("some_uint64", pa.uint64())]),
            pa.schema([("some_uint64", "uint64")]),
            ["UBIGINT"],
        ),
        (
            pa.schema([("some_float16", pa.float16())]),
            pa.schema([("some_float16", "float")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_float32", pa.float32())]),
            pa.schema([("some_float32", "float")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_float64", pa.float64())]),
            pa.schema([("some_float64", "double")]),
            ["DOUBLE"],
        ),
        (
            pa.schema([("some_string", pa.string())]),
            pa.schema([("some_string", "string")]),
            ["VARCHAR"],
        ),
        (
            pa.schema([("some_large_string", pa.large_string())]),
            pa.schema([("some_large_string", "string")]),
            ["VARCHAR"],
        ),
        (
            pa.schema([("some_integer_array", pa.list_(pa.int32()))]),
            pa.schema([("some_integer_array", pa.list_(pa.int32()))]),
            ["INTEGER[]"],
        ),
        (
            pa.schema([("some_large_integer_array", pa.list_(pa.int64()))]),
            pa.schema([("some_large_integer_array", pa.list_(pa.int64()))]),
            ["BIGINT[]"],
        ),
        (
            pa.schema([("some_fixed_size_integer_array", pa.list_(pa.int64(), 2))]),
            pa.schema([("some_fixed_size_integer_array", pa.list_(pa.int64()))]),
            ["BIGINT[]"],
        ),
        (
            pa.schema([("some_decimal", pa.decimal128(precision=38, scale=9))]),
            pa.schema([("some_decimal", pa.decimal128(precision=38, scale=9))]),
            ["DECIMAL"],
        ),
    ],
)
def test_duckdb_data_types(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    expected_keywords: List[str],
    duckdb_uri: str,
) -> None:
    query = arrow_schema_to_create_table(actual_schema, "random_test_table", "duckdb")

    for keyword in expected_keywords:
        assert keyword in query

    conn = create_connection(duckdb_uri)
    assert isinstance(conn, adbc_driver_duckdb.dbapi.Connection)

    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS random_test_table")
        cursor.execute(query)
    conn.commit()

    assert conn.adbc_get_table_schema("random_test_table") == expected_schema

    conn.close()


@pytest.mark.parametrize(
    "actual_schema, expected_schema, expected_keywords",
    [
        (
            pa.schema([("some_bool", pa.bool_())]),
            pa.schema([("some_bool", "bool")]),
            ["BOOLEAN"],
        ),
        (
            pa.schema([("some_int8", pa.int8())]),
            pa.schema([("some_int8", "int16")]),
            ["SMALLINT"],
        ),
        (
            pa.schema([("some_uint8", pa.uint8())]),
            pa.schema([("some_uint8", "int16")]),
            ["SMALLINT"],
        ),
        (
            pa.schema([("some_int16", pa.int16())]),
            pa.schema([("some_int16", "int16")]),
            ["SMALLINT"],
        ),
        (
            pa.schema([("some_uint16", pa.uint16())]),
            pa.schema([("some_uint16", "int16")]),
            ["SMALLINT"],
        ),
        (
            pa.schema([("some_int32", pa.int32())]),
            pa.schema([("some_int32", "int32")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_uint32", pa.uint32())]),
            pa.schema([("some_uint32", "int32")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_int64", pa.int64())]),
            pa.schema([("some_int64", "int64")]),
            ["BIGINT"],
        ),
        (
            pa.schema([("some_uint64", pa.uint64())]),
            pa.schema([("some_uint64", "int64")]),
            ["BIGINT"],
        ),
        (
            pa.schema([("some_float16", pa.float16())]),
            pa.schema([("some_float16", "float")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_float32", pa.float32())]),
            pa.schema([("some_float32", "float")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_float64", pa.float64())]),
            pa.schema([("some_float64", "double")]),
            ["DOUBLE PRECISION"],
        ),
        (
            pa.schema([("some_string", pa.string())]),
            pa.schema([("some_string", "string")]),
            ["TEXT"],
        ),
        (
            pa.schema([("some_large_string", pa.large_string())]),
            pa.schema([("some_large_string", "string")]),
            ["TEXT"],
        ),
        (
            pa.schema([("some_integer_array", pa.list_(pa.int32()))]),
            pa.schema([("some_integer_array", pa.list_(pa.int32()))]),
            ["INTEGER ARRAY"],
        ),
        (
            pa.schema([("some_large_integer_array", pa.list_(pa.int64()))]),
            pa.schema([("some_large_integer_array", pa.list_(pa.int64()))]),
            ["BIGINT ARRAY"],
        ),
        (
            pa.schema([("some_fixed_size_integer_array", pa.list_(pa.int64(), 2))]),
            pa.schema([("some_fixed_size_integer_array", pa.list_(pa.int64()))]),
            ["BIGINT ARRAY"],
        ),
    ],
)
def test_psql_data_types(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    expected_keywords: List[str],
    postgres_uri: str,
) -> None:
    query = arrow_schema_to_create_table(
        actual_schema, "random_test_table", "postgresql"
    )

    for keyword in expected_keywords:
        assert keyword in query

    conn = create_connection(postgres_uri)
    assert isinstance(conn, adbc_driver_postgresql.dbapi.Connection)

    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS random_test_table")
        cursor.execute(query)
    conn.commit()

    assert conn.adbc_get_table_schema("random_test_table") == expected_schema

    conn.close()


@pytest.mark.parametrize(
    "actual_schema, expected_schema, expected_keywords",
    [
        (
            pa.schema([("some_bool", pa.bool_())]),
            pa.schema([("some_bool", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_int8", pa.int8())]),
            pa.schema([("some_int8", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_uint8", pa.uint8())]),
            pa.schema([("some_uint8", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_int16", pa.int16())]),
            pa.schema([("some_int16", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_uint16", pa.uint16())]),
            pa.schema([("some_uint16", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_int32", pa.int32())]),
            pa.schema([("some_int32", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_uint32", pa.uint32())]),
            pa.schema([("some_uint32", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_int64", pa.int64())]),
            pa.schema([("some_int64", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_uint64", pa.uint64())]),
            pa.schema([("some_uint64", "int64")]),
            ["INTEGER"],
        ),
        (
            pa.schema([("some_float16", pa.float16())]),
            pa.schema([("some_float16", "float")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_float32", pa.float32())]),
            pa.schema([("some_float32", "float")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_float64", pa.float64())]),
            pa.schema([("some_float64", "double")]),
            ["REAL"],
        ),
        (
            pa.schema([("some_string", pa.string())]),
            pa.schema([("some_string", "string")]),
            ["TEXT"],
        ),
        (
            pa.schema([("some_large_string", pa.large_string())]),
            pa.schema([("some_large_string", "string")]),
            ["TEXT"],
        ),
        (
            pa.schema([("some_integer_array", pa.list_(pa.int32()))]),
            pa.schema([("some_integer_array", "string")]),
            ["TEXT"],
        ),
        (
            pa.schema([("some_large_integer_array", pa.list_(pa.int64()))]),
            pa.schema([("some_large_integer_array", "string")]),
            ["TEXT"],
        ),
        (
            pa.schema([("some_fixed_size_integer_array", pa.list_(pa.int64(), 2))]),
            pa.schema([("some_fixed_size_integer_array", "string")]),
            ["TEXT"],
        ),
    ],
)
def test_sqlite_data_types(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    expected_keywords: List[str],
    sqlite_uri: str,
) -> None:
    query = arrow_schema_to_create_table(actual_schema, "random_test_table", "sqlite")

    for keyword in expected_keywords:
        assert keyword in query

    conn = create_connection(sqlite_uri)
    assert isinstance(conn, adbc_driver_sqlite.dbapi.Connection)

    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS random_test_table")
        cursor.execute(query)
    conn.commit()

    assert conn.adbc_get_table_schema("random_test_table") == expected_schema

    conn.close()
