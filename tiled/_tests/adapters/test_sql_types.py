import decimal
import os
from contextlib import closing
from pathlib import Path
from typing import AsyncGenerator, Generator, Literal, cast

import numpy
import pyarrow as pa
import pytest
import pytest_asyncio

from tiled._tests.utils import temp_postgres
from tiled.adapters.sql import (
    arrow_schema_to_column_defns,
    arrow_schema_to_create_table,
)
from tiled.storage import EmbeddedSQLStorage, RemoteSQLStorage


@pytest_asyncio.fixture
async def postgresql_uri() -> AsyncGenerator[str, None]:
    uri = os.getenv("TILED_TEST_POSTGRESQL_URI")
    if uri is None:
        pytest.skip("TILED_TEST_POSTGRESQL_URI is not set")

    async with temp_postgres(uri) as uri_with_database_name:
        yield uri_with_database_name
        # yield uri_with_database_name.rsplit("/", 1)[0]


@pytest_asyncio.fixture
def sqlite_uri(tmp_path: Path) -> Generator[str, None, None]:
    yield f"sqlite:///{tmp_path}/test.db"


@pytest_asyncio.fixture
def duckdb_uri(tmp_path: Path) -> Generator[str, None, None]:
    yield f"duckdb:///{tmp_path}/test.db"


INT8_INFO = numpy.iinfo(numpy.int8)
INT16_INFO = numpy.iinfo(numpy.int16)
INT32_INFO = numpy.iinfo(numpy.int32)
INT64_INFO = numpy.iinfo(numpy.int64)
UINT8_INFO = numpy.iinfo(numpy.uint8)
UINT16_INFO = numpy.iinfo(numpy.uint16)
UINT32_INFO = numpy.iinfo(numpy.uint32)
UINT64_INFO = numpy.iinfo(numpy.uint64)
FLOAT16_INFO = numpy.finfo(numpy.float16)
FLOAT32_INFO = numpy.finfo(numpy.float32)
FLOAT64_INFO = numpy.finfo(numpy.float64)
# Map schemas (testing different data types or combinations of data types)
# to an inner mapping. The inner mapping maps each dialect to a tuple,
# (SQL type definition, Arrow type read back).
# { test_case_id:  (input_table, {dialect: (expected_typedefs, expected_schema)})}
TEST_CASES = {
    "bool": (
        pa.Table.from_arrays([pa.array([True, False], "bool")], names=["x"]),
        {
            "duckdb": (["BOOLEAN NULL"], pa.schema([("x", "bool")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["BOOLEAN NULL"], pa.schema([("x", "bool")])),
        },
    ),
    "string": (
        pa.Table.from_arrays([pa.array(["a", "b"], "string")], names=["x"]),
        {
            "duckdb": (["VARCHAR NULL"], pa.schema([("x", "string")])),
            "sqlite": (["TEXT NULL"], pa.schema([("x", "string")])),
            "postgresql": (["TEXT NULL"], pa.schema([("x", "string")])),
        },
    ),
    "int8": (
        pa.Table.from_arrays(
            [pa.array([INT8_INFO.min, INT8_INFO.max], "int8")], names=["x"]
        ),
        {
            "duckdb": (["TINYINT NULL"], pa.schema([("x", "int8")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["SMALLINT NULL"], pa.schema([("x", "int16")])),
        },
    ),
    "int16": (
        pa.Table.from_arrays(
            [pa.array([INT16_INFO.min, INT16_INFO.max], "int16")], names=["x"]
        ),
        {
            "duckdb": (["SMALLINT NULL"], pa.schema([("x", "int16")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["SMALLINT NULL"], pa.schema([("x", "int16")])),
        },
    ),
    "int32": (
        pa.Table.from_arrays(
            [pa.array([INT32_INFO.min, INT32_INFO.max], "int32")], names=["x"]
        ),
        {
            "duckdb": (["INTEGER NULL"], pa.schema([("x", "int32")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["INTEGER NULL"], pa.schema([("x", "int32")])),
        },
    ),
    "int64": (
        pa.Table.from_arrays(
            [pa.array([INT64_INFO.min, INT64_INFO.max], "int64")], names=["x"]
        ),
        {
            "duckdb": (["BIGINT NULL"], pa.schema([("x", "int64")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["BIGINT NULL"], pa.schema([("x", "int64")])),
        },
    ),
    "uint8": (
        pa.Table.from_arrays(
            [pa.array([UINT8_INFO.min, UINT8_INFO.max], "uint8")], names=["x"]
        ),
        {
            "duckdb": (["UTINYINT NULL"], pa.schema([("x", "uint8")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["SMALLINT NULL"], pa.schema([("x", "int16")])),
        },
    ),
    "uint16": (
        pa.Table.from_arrays(
            [pa.array([UINT16_INFO.min, UINT16_INFO.max], "uint16")], names=["x"]
        ),
        {
            "duckdb": (["USMALLINT NULL"], pa.schema([("x", "uint16")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["INTEGER NULL"], pa.schema([("x", "int32")])),
        },
    ),
    "uint32": (
        pa.Table.from_arrays(
            [pa.array([UINT32_INFO.min, UINT32_INFO.max], "uint32")], names=["x"]
        ),
        {
            "duckdb": (["UINTEGER NULL"], pa.schema([("x", "uint32")])),
            "sqlite": (["INTEGER NULL"], pa.schema([("x", "int64")])),
            "postgresql": (["BIGINT NULL"], pa.schema([("x", "int64")])),
        },
    ),
    "uint64": (
        pa.Table.from_arrays(
            [pa.array([UINT64_INFO.min, UINT64_INFO.max], "uint64")], names=["x"]
        ),
        {
            "duckdb": (["UBIGINT NULL"], pa.schema([("x", "uint64")])),
        },
    ),
    "list_of_ints": (
        pa.Table.from_arrays(
            [pa.array([[1, 2], [3, 4]], pa.list_(pa.int32()))], names=["x"]
        ),
        {
            "duckdb": (["INTEGER[] NULL"], pa.schema([("x", pa.list_(pa.int32()))])),
            "postgresql": (
                ["INTEGER ARRAY NULL"],
                pa.schema([("x", pa.list_(pa.int32()))]),
            ),
        },
    ),
    "list_of_bounded_ints": (
        pa.Table.from_arrays(
            [pa.array([[1, 2], [3, 4]], pa.list_(pa.int32(), 2))], names=["x"]
        ),
        {
            "duckdb": (["INTEGER[] NULL"], pa.schema([("x", pa.list_(pa.int32()))])),
            "postgresql": (
                ["INTEGER ARRAY NULL"],
                pa.schema([("x", pa.list_(pa.int32()))]),
            ),
        },
    ),
    "float16": (
        pa.Table.from_arrays(
            [pa.array([FLOAT16_INFO.min, FLOAT16_INFO.max], "float16")], names=["x"]
        ),
        {},  # not supported by any backend
    ),
    "float32": (
        pa.Table.from_arrays(
            [pa.array([FLOAT32_INFO.min, FLOAT32_INFO.max], "float32")], names=["x"]
        ),
        {
            "duckdb": (["REAL NULL"], pa.schema([("x", "float32")])),
            "sqlite": (["REAL NULL"], pa.schema([("x", "double")])),
            "postgresql": (["REAL NULL"], pa.schema([("x", "float32")])),
        },
    ),
    "float64": (
        pa.Table.from_arrays(
            [pa.array([FLOAT64_INFO.min, FLOAT64_INFO.max], "float64")], names=["x"]
        ),
        {
            "duckdb": (["DOUBLE NULL"], pa.schema([("x", "float64")])),
            "sqlite": (["REAL NULL"], pa.schema([("x", "double")])),
            "postgresql": (["DOUBLE PRECISION NULL"], pa.schema([("x", "float64")])),
        },
    ),
    "decimal": (
        pa.Table.from_arrays(
            [pa.array([decimal.Decimal("123.45")], pa.decimal128(5, 2))], names=["x"]
        ),
        {
            "duckdb": (["DECIMAL(5, 2) NULL"], pa.schema([("x", pa.decimal128(5, 2))])),
        },
    ),
}


@pytest.mark.parametrize("dialect", ["duckdb", "postgresql", "sqlite"])
@pytest.mark.parametrize("test_case_id", list(TEST_CASES))
def test_data_types(
    test_case_id: str,
    dialect: Literal["postgresql", "sqlite", "duckdb"],
    request: pytest.FixtureRequest,
) -> None:
    test_table_name = f"test_{test_case_id}"
    table, dialect_results = TEST_CASES[test_case_id]

    if (dialect == "duckdb") and (test_case_id == "decimal"):
        pytest.xfail(reason="Regression in support, needs investigation")

    if dialect not in cast(dict, dialect_results):  # type: ignore
        with pytest.raises(ValueError, match="Unsupported PyArrow type"):
            arrow_schema_to_column_defns(table.schema, dialect)
        return

    expected_typedefs, expected_schema = dialect_results[dialect]  # type: ignore
    db_uri = request.getfixturevalue(f"{dialect}_uri")
    columns = arrow_schema_to_column_defns(table.schema, dialect)
    assert list(columns.values()) == expected_typedefs

    query = arrow_schema_to_create_table(table.schema, test_table_name, dialect)

    storage_cls = RemoteSQLStorage if dialect == "postgresql" else EmbeddedSQLStorage
    storage = storage_cls(uri=db_uri)
    with closing(storage.connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # For SQLite specifically, some inference is needed by ADBC to get the type
        # and on an empty table the value is not defined. As of this writing it is
        # int64 by default; in the future it may be null.
        # https://github.com/apache/arrow-adbc/issues/581
        if dialect != "sqlite":
            assert conn.adbc_get_table_schema(test_table_name) == expected_schema

        with conn.cursor() as cursor:
            cursor.adbc_ingest(test_table_name, table, mode="append")

        assert conn.adbc_get_table_schema(test_table_name) == expected_schema

        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {test_table_name}")
            result = cursor.fetch_arrow_table()

        # The result will match expected_schema, which may not be the same as
        # the schema the data was uploaded as, if the databases does not support
        # that precise type.
        assert result.schema == expected_schema

        # Before comparing the Tables, we cast the Table into the original schema,
        # which might use finer types.
        assert result.cast(table.schema) == table

    storage.dispose()  # Close all connections before deleting the storage DB
