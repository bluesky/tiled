import os
from pathlib import Path
from typing import Any, Callable, Generator, Union

import adbc_driver_duckdb
import pyarrow as pa
import pytest

from tiled.adapters.sql import SQLAdapter, check_table_name
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management, Storage
from tiled.structures.table import TableStructure

names = ["f0", "f1", "f2", "f3"]
data0 = [
    pa.array([1, 2, 3, 4, 5]),
    pa.array([1.0, 2.0, 3.0, 4.0, 5.0]),
    pa.array(["foo0", "bar0", "baz0", None, "goo0"]),
    pa.array([True, None, False, True, None]),
]
data1 = [
    pa.array([6, 7, 8, 9, 10, 11, 12]),
    pa.array([6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]),
    pa.array(["foo1", "bar1", None, "baz1", "biz", None, "goo"]),
    pa.array([None, True, True, False, False, None, True]),
]
data2 = [
    pa.array([13, 14]),
    pa.array([13.0, 14.0]),
    pa.array(["foo2", "baz2"]),
    pa.array([False, None]),
]

batch0 = pa.record_batch(data0, names=names)
batch1 = pa.record_batch(data1, names=names)
batch2 = pa.record_batch(data2, names=names)


@pytest.fixture
def data_source_from_init_storage() -> Callable[[str], DataSource[TableStructure]]:
    def _data_source_from_init_storage(data_uri: str) -> DataSource[TableStructure]:
        table = pa.Table.from_arrays(data0, names)
        structure = TableStructure.from_arrow_table(table, npartitions=1)
        data_source = DataSource(
            management=Management.writable,
            mimetype="application/x-tiled-sql-table",
            structure_family=StructureFamily.table,
            structure=structure,
            assets=[],
        )

        storage = Storage(filesystem=None, sql=data_uri)
        return SQLAdapter.init_storage(
            data_source=data_source, storage=storage, path_parts=[]
        )

    return _data_source_from_init_storage


@pytest.fixture
def adapter_sql(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str], DataSource[TableStructure]],
) -> Generator[SQLAdapter, None, None]:
    data_uri = f"duckdb:///{tmp_path}/test.db"
    data_source = data_source_from_init_storage(data_uri)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_attributes(adapter_sql: SQLAdapter) -> None:
    assert adapter_sql.structure().columns == names
    assert adapter_sql.structure().npartitions == 1
    assert isinstance(adapter_sql.conn, adbc_driver_duckdb.dbapi.Connection)


def test_write_read_sql_one(adapter_sql: SQLAdapter) -> None:
    # test writing and reading it
    adapter_sql.append_partition(batch0, 0)
    result = adapter_sql.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f3"] = result["f3"].astype("boolean")

    assert pa.Table.from_arrays(data0, names) == pa.Table.from_pandas(result)


def test_write_read_sql_list(adapter_sql: SQLAdapter) -> None:
    adapter_sql.append_partition([batch0, batch1, batch2], 0)
    result = adapter_sql.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f3"] = result["f3"].astype("boolean")
    assert pa.Table.from_batches([batch0, batch1, batch2]) == pa.Table.from_pandas(
        result
    )

    # test write , append and read all
    adapter_sql.append_partition([batch2, batch0, batch1], 0)
    adapter_sql.append_partition([batch1, batch2, batch0], 0)
    result = adapter_sql.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f3"] = result["f3"].astype("boolean")

    assert pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    ) == pa.Table.from_pandas(result)


@pytest.fixture
def postgres_uri() -> str:
    uri = os.getenv("TILED_TEST_POSTGRESQL_URI")
    if uri is not None:
        return uri
    pytest.skip("TILED_TEST_POSTGRESQL_URI is not set")
    return ""


@pytest.fixture
def adapter_psql(
    data_source_from_init_storage: Callable[[str], DataSource[TableStructure]],
    postgres_uri: str,
) -> SQLAdapter:
    data_source = data_source_from_init_storage(postgres_uri)
    return SQLAdapter(
        postgres_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_psql(adapter_psql: SQLAdapter) -> None:
    assert adapter_psql.structure().columns == names
    assert adapter_psql.structure().npartitions == 1
    # assert isinstance(
    #    adapter_psql.conn, adbc_driver_postgresql.dbapi.AdbcSqliteConnection
    # )


def test_write_read_psql_one(adapter_psql: SQLAdapter) -> None:
    # test writing and reading it
    adapter_psql.append_partition(batch0, 0)
    result = adapter_psql.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f3"] = result["f3"].astype("boolean")


def test_write_read_psql_list(adapter_psql: SQLAdapter) -> None:
    adapter_psql.append_partition([batch0, batch1, batch2], 0)
    result = adapter_psql.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f3"] = result["f3"].astype("boolean")
    assert pa.Table.from_batches([batch0, batch1, batch2]) == pa.Table.from_pandas(
        result
    )

    # test write , append and read all
    adapter_psql.append_partition([batch2, batch0, batch1], 0)
    adapter_psql.append_partition([batch1, batch2, batch0], 0)
    result = adapter_psql.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result["f3"] = result["f3"].astype("boolean")

    assert pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    ) == pa.Table.from_pandas(result)


@pytest.mark.parametrize(
    "table_name, expected",
    [
        (
            "table_abcdefg12423pnjsbldfhjdfbv_hbdhfljb128w40_ndgjfsdflfnscljm",
            pytest.raises(
                ValueError, match="Table name is too long, max character number is 63!"
            ),
        ),
        (
            "create_abcdefg12423pnjsbldfhjdfbv_hbdhfljb128w40_ndgjfsdflfnscljk_sdbf_jhvjkbefl",
            pytest.raises(
                ValueError, match="Table name is too long, max character number is 63!"
            ),
        ),
        (
            "hello_abcdefg12423pnjsbldfhjdfbv_hbdhfljb128w40_ndgjfsdflfnscljk_sdbf_jhvjkbefl",
            pytest.raises(
                ValueError, match="Table name is too long, max character number is 63!"
            ),
        ),
        ("my_table_here_123_", None),
        ("the_short_table12374620_hello_table23704ynnm", None),
    ],
)
def test_check_table_name_long_name(
    table_name: str, expected: Union[None, Any]
) -> None:
    if isinstance(expected, type(pytest.raises(ValueError))):
        with expected:
            check_table_name(table_name)
    else:
        assert check_table_name(table_name) is None  # type: ignore[func-returns-value]


@pytest.mark.parametrize(
    "table_name, expected",
    [
        (
            "_here_is_my_table",
            pytest.raises(ValueError, match="Illegal table name!"),
        ),
        (
            "create_this_table1246*",
            pytest.raises(ValueError, match="Illegal table name!"),
        ),
        (
            "create this_table1246",
            pytest.raises(ValueError, match="Illegal table name!"),
        ),
        (
            "drop this_table1246",
            pytest.raises(ValueError, match="Illegal table name!"),
        ),
        (
            "table_mytable!",
            pytest.raises(ValueError, match="Illegal table name!"),
        ),
        ("my_table_here_123_", None),
        ("the_short_table12374620_hello_table23704ynnm", None),
    ],
)
def test_check_table_name_illegal_name(
    table_name: str, expected: Union[None, Any]
) -> None:
    if isinstance(expected, type(pytest.raises(ValueError))):
        with expected:
            check_table_name(table_name)
    else:
        assert check_table_name(table_name) is None  # type: ignore[func-returns-value]


@pytest.mark.parametrize(
    "table_name, expected",
    [
        (
            "select",
            pytest.raises(
                ValueError,
                match="Reserved SQL keywords are not allowed in the table name!",
            ),
        ),
        (
            "create",
            pytest.raises(
                ValueError,
                match="Reserved SQL keywords are not allowed in the table name!",
            ),
        ),
        (
            "SELECT",
            pytest.raises(
                ValueError,
                match="Reserved SQL keywords are not allowed in the table name!",
            ),
        ),
        (
            "from",
            pytest.raises(
                ValueError,
                match="Reserved SQL keywords are not allowed in the table name!",
            ),
        ),
        ("drop_this_table123_", None),
        ("DROP_thistable123_hwejk", None),
    ],
)
def test_check_table_name_reserved_keywords(
    table_name: str, expected: Union[None, Any]
) -> None:
    if isinstance(expected, type(pytest.raises(ValueError))):
        with expected:
            check_table_name(table_name)
    else:
        assert check_table_name(table_name) is None  # type: ignore[func-returns-value]
