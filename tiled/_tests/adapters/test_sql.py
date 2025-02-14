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
def data_source_from_init_storage() -> Callable[[str, int], DataSource[TableStructure]]:
    def _data_source_from_init_storage(
        data_uri: str, num_partitions: int
    ) -> DataSource[TableStructure]:
        table = pa.Table.from_arrays(data0, names)
        structure = TableStructure.from_arrow_table(table, npartitions=num_partitions)
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
def adapter_sql_one_partition(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
) -> Generator[SQLAdapter, None, None]:
    data_uri = f"duckdb:///{tmp_path}/test.db"
    data_source = data_source_from_init_storage(data_uri, 1)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


@pytest.fixture
def adapter_sql_many_partition(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
) -> Generator[SQLAdapter, None, None]:
    data_uri = f"duckdb:///{tmp_path}/test.db"
    data_source = data_source_from_init_storage(data_uri, 3)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_attributes_one_part(adapter_sql_one_partition: SQLAdapter) -> None:
    assert adapter_sql_one_partition.structure().columns == names
    assert adapter_sql_one_partition.structure().npartitions == 1
    assert isinstance(
        adapter_sql_one_partition.conn, adbc_driver_duckdb.dbapi.Connection
    )


def test_attributes_many_part(adapter_sql_many_partition: SQLAdapter) -> None:
    assert adapter_sql_many_partition.structure().columns == names
    assert adapter_sql_many_partition.structure().npartitions == 3
    assert isinstance(
        adapter_sql_many_partition.conn, adbc_driver_duckdb.dbapi.Connection
    )


@pytest.fixture
def postgres_uri() -> str:
    uri = os.getenv("TILED_TEST_POSTGRESQL_URI")
    if uri is not None:
        return uri
    pytest.skip("TILED_TEST_POSTGRESQL_URI is not set")
    return ""


@pytest.fixture
def adapter_psql_one_partition(
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    postgres_uri: str,
) -> SQLAdapter:
    data_source = data_source_from_init_storage(postgres_uri, 1)
    return SQLAdapter(
        postgres_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


@pytest.fixture
def adapter_psql_many_partition(
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    postgres_uri: str,
) -> SQLAdapter:
    data_source = data_source_from_init_storage(postgres_uri, 3)
    return SQLAdapter(
        postgres_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_psql(adapter_psql_one_partition: SQLAdapter) -> None:
    assert adapter_psql_one_partition.structure().columns == names
    assert adapter_psql_one_partition.structure().npartitions == 1
    # assert isinstance(
    #    adapter_psql.conn, adbc_driver_postgresql.dbapi.AdbcSqliteConnection
    # )


@pytest.mark.parametrize(
    "adapter",
    [
        ("adapter_sql_one_partition"),
        ("adapter_psql_one_partition"),
    ],
)
def test_write_read_one_batch_one_part(
    adapter: SQLAdapter, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter = request.getfixturevalue(adapter)

    # test appending and reading a table as a whole
    test_table = pa.Table.from_arrays(data0, names)

    adapter.append_partition(batch0, 0)
    result_read = adapter.read()
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result_read["f3"] = result_read["f3"].astype("boolean")
    assert test_table == pa.Table.from_pandas(result_read)

    # test appending and reading a partition in a table
    result_read_partition = adapter.read_partition(0)
    # the pandas dataframe gives the last column of the data as 0 and 1 since SQL does not save boolean
    # so we explicitely convert the last column to boolean for testing purposes
    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")
    assert test_table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize(
    "adapter",
    [
        ("adapter_sql_one_partition"),
        ("adapter_psql_one_partition"),
    ],
)
def test_write_read_list_batch_one_part(
    adapter: SQLAdapter, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter = request.getfixturevalue(adapter)

    test_table = pa.Table.from_batches([batch0, batch1, batch2])
    # test appending a list of batches to a table and read as a whole
    adapter.append_partition([batch0, batch1, batch2], 0)
    result_read = adapter.read()

    result_read["f3"] = result_read["f3"].astype("boolean")
    assert test_table == pa.Table.from_pandas(result_read)

    # test appending and reading a partition in a table
    result_read_partition = adapter.read_partition(0)

    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")
    assert test_table == pa.Table.from_pandas(result_read_partition)

    # test appending few more times done correctly
    test_table = pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    )
    adapter.append_partition([batch2, batch0, batch1], 0)
    adapter.append_partition([batch1, batch2, batch0], 0)
    result_read = adapter.read()

    result_read["f3"] = result_read["f3"].astype("boolean")

    assert test_table == pa.Table.from_pandas(result_read)

    # test appending a few times and reading done correctly
    result_read_partition = adapter.read_partition(0)

    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")

    assert test_table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize(
    "adapter",
    [
        ("adapter_sql_many_partition"),
        ("adapter_psql_many_partition"),
    ],
)
def test_write_read_one_batch_many_part(
    adapter: SQLAdapter, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter = request.getfixturevalue(adapter)

    # test writing to many partitions and reading it whole
    adapter.append_partition(batch0, 0)
    adapter.append_partition(batch1, 1)
    adapter.append_partition(batch2, 2)

    result_read = adapter.read()
    result_read["f3"] = result_read["f3"].astype("boolean")

    assert pa.Table.from_batches([batch0, batch1, batch2]) == pa.Table.from_pandas(
        result_read
    )

    # test reading a specific partition
    result_read_partition = adapter.read_partition(0)
    assert pa.Table.from_arrays(data0, names) == pa.Table.from_pandas(
        result_read_partition
    )

    result_read_partition = adapter.read_partition(1)
    assert pa.Table.from_arrays(data1, names) == pa.Table.from_pandas(
        result_read_partition
    )

    result_read_partition = adapter.read_partition(2)
    assert pa.Table.from_arrays(data2, names) == pa.Table.from_pandas(
        result_read_partition
    )

    # test appending a few times and reading done correctly
    adapter.append_partition(batch0, 1)
    adapter.append_partition(batch1, 2)
    adapter.append_partition(batch2, 0)

    result_read = adapter.read()
    result_read["f3"] = result_read["f3"].astype("boolean")

    # note that now partition 1 has [batch0, batch2], partition 1 has
    # [batch1, batch0] and partititon 2 has [batch2, batch1]
    assert pa.Table.from_batches(
        [batch0, batch2, batch1, batch0, batch2, batch1]
    ) == pa.Table.from_pandas(result_read)

    # test reading a specific parition after appending
    result_read_partition = adapter.read_partition(0)
    assert pa.Table.from_batches([batch0, batch2]) == pa.Table.from_pandas(
        result_read_partition
    )

    result_read_partition = adapter.read_partition(1)
    assert pa.Table.from_batches([batch1, batch0]) == pa.Table.from_pandas(
        result_read_partition
    )

    result_read_partition = adapter.read_partition(2)
    assert pa.Table.from_batches([batch2, batch1]) == pa.Table.from_pandas(
        result_read_partition
    )


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
