from pathlib import Path
from typing import Any, Callable, Generator, Union, cast

import numpy as np
import pyarrow as pa
import pytest

from tiled.adapters.sql import (
    COLUMN_NAME_PATTERN,
    TABLE_NAME_PATTERN,
    SQLAdapter,
    is_safe_identifier,
)
from tiled.storage import SQLStorage, get_storage, parse_storage, register_storage
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import sanitize_uri

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

        storage = cast(SQLStorage, parse_storage(data_uri))
        register_storage(storage)
        return SQLAdapter.init_storage(data_source=data_source, storage=storage)

    return _data_source_from_init_storage


@pytest.fixture
def adapter_duckdb_one_partition(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    duckdb_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(duckdb_uri, 1)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


@pytest.fixture
def adapter_duckdb_many_partitions(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    duckdb_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(duckdb_uri, 3)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_attributes_duckdb_one_part(adapter_duckdb_one_partition: SQLAdapter) -> None:
    assert adapter_duckdb_one_partition.structure().columns == names
    assert adapter_duckdb_one_partition.structure().npartitions == 1


def test_attributes_duckdb_many_part(
    adapter_duckdb_many_partitions: SQLAdapter,
) -> None:
    assert adapter_duckdb_many_partitions.structure().columns == names
    assert adapter_duckdb_many_partitions.structure().npartitions == 3


@pytest.fixture
def adapter_sqlite_one_partition(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    sqlite_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(sqlite_uri, 1)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


@pytest.fixture
def adapter_sqlite_many_partitions(
    tmp_path: Path,
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    sqlite_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(sqlite_uri, 3)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_attributes_sql_one_part(adapter_sqlite_one_partition: SQLAdapter) -> None:
    assert adapter_sqlite_one_partition.structure().columns == names
    assert adapter_sqlite_one_partition.structure().npartitions == 1


def test_attributes_sql_many_part(adapter_sqlite_many_partitions: SQLAdapter) -> None:
    assert adapter_sqlite_many_partitions.structure().columns == names
    assert adapter_sqlite_many_partitions.structure().npartitions == 3


@pytest.fixture
def adapter_psql_one_partition(
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    postgres_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(postgres_uri, 1)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )

    # Close all connections and dispose of the storage
    storage = get_storage(sanitize_uri(postgres_uri)[0])
    cast(SQLStorage, storage).dispose()


@pytest.fixture
def adapter_psql_many_partitions(
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    postgres_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(postgres_uri, 3)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )

    # Close all connections and dispose of the storage
    storage = get_storage(sanitize_uri(postgres_uri)[0])
    cast(SQLStorage, storage).dispose()


def test_psql(adapter_psql_one_partition: SQLAdapter) -> None:
    assert adapter_psql_one_partition.structure().columns == names
    assert adapter_psql_one_partition.structure().npartitions == 1


@pytest.mark.parametrize(
    "adapter",
    [
        ("adapter_sqlite_one_partition"),
        ("adapter_duckdb_one_partition"),
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
        ("adapter_sqlite_one_partition"),
        ("adapter_duckdb_one_partition"),
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


def assert_same_rows(table1: pa.Table, table2: pa.Table) -> None:
    "Verify that two tables have the same rows, regardless of order."
    assert table1.num_rows == table2.num_rows

    rows1 = {tuple(row) for row in table1.to_pylist()}
    rows2 = {tuple(row) for row in table2.to_pylist()}
    assert rows1 == rows2


@pytest.mark.parametrize(
    "adapter",
    [
        ("adapter_sqlite_many_partitions"),
        ("adapter_duckdb_many_partitions"),
        ("adapter_psql_many_partitions"),
    ],
)
def test_append_single_partition(
    adapter: SQLAdapter, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter = request.getfixturevalue(adapter)

    # test writing an entire pyarrow table to a single partition
    table = pa.Table.from_batches([batch0, batch1, batch2])
    adapter.append_partition(table, 0)

    result_read = adapter.read()
    result_read["f3"] = result_read["f3"].astype("boolean")
    assert table == pa.Table.from_pandas(result_read)

    # test reading a specific partition
    result_read_partition = adapter.read_partition(0)
    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")
    assert table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize(
    "adapter",
    [
        ("adapter_sqlite_many_partitions"),
        ("adapter_psql_many_partitions"),
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
    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")
    assert pa.Table.from_arrays(data0, names) == pa.Table.from_pandas(
        result_read_partition
    )

    result_read_partition = adapter.read_partition(1)
    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")
    assert pa.Table.from_arrays(data1, names) == pa.Table.from_pandas(
        result_read_partition
    )

    result_read_partition = adapter.read_partition(2)
    result_read_partition["f3"] = result_read_partition["f3"].astype("boolean")
    assert pa.Table.from_arrays(data2, names) == pa.Table.from_pandas(
        result_read_partition
    )

    # test appending a few times and reading done correctly
    adapter.append_partition(batch0, 1)
    adapter.append_partition(batch1, 2)
    adapter.append_partition(batch2, 0)

    result_read = adapter.read()
    result_read["f3"] = result_read["f3"].astype("boolean")

    # Check that each partition matches
    assert_same_rows(
        pa.Table.from_batches([batch0, batch2]),
        pa.Table.from_pandas(adapter.read_partition(0)),
    )
    assert_same_rows(
        pa.Table.from_batches([batch1, batch0]),
        pa.Table.from_pandas(adapter.read_partition(1)),
    )
    assert_same_rows(
        pa.Table.from_batches([batch2, batch1]),
        pa.Table.from_pandas(adapter.read_partition(2)),
    )
    assert_same_rows(
        pa.Table.from_batches([batch0, batch2, batch1, batch0, batch2, batch1]),
        pa.Table.from_pandas(result_read),
    )

    # read a specific field
    result_read = adapter.read_partition(0, fields=["f1"])
    assert [*data0[1].tolist(), *data2[1].tolist()] == result_read["f1"].tolist()
    result_read = adapter.read_partition(1, fields=["f0"])
    assert [*data1[0].tolist(), *data0[0].tolist()] == result_read["f0"].tolist()
    result_read = adapter.read_partition(2, fields=["f2"])
    assert [*data2[2].tolist(), *data1[2].tolist()] == result_read["f2"].tolist()


@pytest.mark.parametrize(
    "table_name, expected",
    [
        (
            "table_abcdefg12423pnjsbldfhjdfbv_hbdhfljb128w40_ndgjfsdflfnscljm",
            pytest.raises(
                ValueError, match=r"Invalid SQL identifier.+max character number is 63"
            ),
        ),
        (
            "create_abcdefg12423pnjsbldfhjdfbv_hbdhfljb128w40_ndgjfsdflfnscljk_sdbf_jhvjkbefl",
            pytest.raises(
                ValueError, match=r"Invalid SQL identifier.+max character number is 63"
            ),
        ),
        (
            "hello_abcdefg12423pnjsbldfhjdfbv_hbdhfljb128w40_ndgjfsdflfnscljk_sdbf_jhvjkbefl",
            pytest.raises(
                ValueError, match=r"Invalid SQL identifier.+max character number is 63"
            ),
        ),
        ("my_table_here_123_", None),
        ("the_short_table12374620_hello_table23704ynnm", None),
        (
            "_here_is_my_table",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "here-is-my-table",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "-here-is-my-table",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "here-is-my-table-",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "create_this_table1246*",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "create this_table1246",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "drop this_table1246",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "table_mytable!",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        ("my_table_here_123_", None),
        ("the_short_table12374620_hello_table23704ynnm", None),
        (
            "select",
            pytest.raises(
                ValueError,
                match=r"Reserved SQL keywords are not allowed in identifiers.+",
            ),
        ),
        (
            "create",
            pytest.raises(
                ValueError,
                match=r"Reserved SQL keywords are not allowed in identifiers.+",
            ),
        ),
        (
            "SELECT",
            pytest.raises(
                ValueError,
                match=r"Reserved SQL keywords are not allowed in identifiers.+",
            ),
        ),
        (
            "from",
            pytest.raises(
                ValueError,
                match=r"Reserved SQL keywords are not allowed in identifiers.+",
            ),
        ),
        ("drop_this_table123_", None),
        (
            "DROP_thistable123_hwejk",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        (
            "CAPITALIZED_NAME",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
    ],
)
def test_check_table_name_is_safe(table_name: str, expected: Union[None, Any]) -> None:
    if isinstance(expected, type(pytest.raises(ValueError))):
        with expected:
            is_safe_identifier(
                table_name, TABLE_NAME_PATTERN, allow_reserved_words=False
            )
    else:
        assert is_safe_identifier(
            table_name, TABLE_NAME_PATTERN, allow_reserved_words=False
        )


@pytest.mark.parametrize(
    "column_name, expected",
    [
        # Valid column names
        ("valid_column_name", None),
        ("_another_valid_name123", None),
        ("column_name_with_underscores", None),
        ("COLUMNnameWITHCAPITALletters", None),
        ("short", None),
        ("a" * 63, None),  # Maximum length
        ("name with-other*allowed:special/characters?!", None),
        # Invalid identifiers - length
        (
            "a" * 64,
            pytest.raises(
                ValueError, match=r"Invalid SQL identifier.+max character number is 63"
            ),
        ),
        # Invalid identifiers - malformed
        (
            "1invalid_start",
            pytest.raises(ValueError, match=r"Malformed SQL identifier.+"),
        ),
        ("-invalid", pytest.raises(ValueError, match=r"Malformed SQL identifier.+")),
        # Invalid identifiers - forbidden characters
        (
            'invalid"name',
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid'name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid`name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid;name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid--name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid\\*name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid*\\name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid*/name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid\\name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid(name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid)name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid+name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "invalid=name",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
        (
            "yet\\another--invalid=name+with(many)forbidden*/characters",
            pytest.raises(
                ValueError,
                match=r"Invalid SQL identifier.+contains forbidden character.+",
            ),
        ),
    ],
)
def test_check_column_name_is_safe(column_name: str, expected: str) -> None:
    if isinstance(expected, type(pytest.raises(ValueError))):
        with expected:
            is_safe_identifier(
                column_name, COLUMN_NAME_PATTERN, allow_reserved_words=True
            )
    else:
        assert is_safe_identifier(
            column_name, COLUMN_NAME_PATTERN, allow_reserved_words=True
        )


@pytest.mark.parametrize("data_uri", ["sqlite_uri", "duckdb_uri", "postgres_uri"])
@pytest.mark.parametrize("column_name", ["a", "a b", "a-b", "a:b", "a*b", "a/b", "Ab"])
def test_can_query_with_valid_column_names(
    data_uri: str, column_name: str, request: pytest.FixtureRequest
) -> None:
    table = pa.Table.from_arrays([[1, 2, 3]], [column_name])
    structure = TableStructure.from_arrow_table(table)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-tiled-sql-table",
        structure_family=StructureFamily.table,
        structure=structure,
        assets=[],
    )
    data_uri = request.getfixturevalue(data_uri)
    storage = cast(SQLStorage, parse_storage(data_uri))
    register_storage(storage)
    assert SQLAdapter.init_storage(data_source=data_source, storage=storage) is not None
    storage.dispose()


@pytest.mark.parametrize("data_uri", ["sqlite_uri", "duckdb_uri", "postgres_uri"])
def test_reject_colliding_uppercase_column_names(
    data_uri: str, request: pytest.FixtureRequest
) -> None:
    # Define a table and a storage
    data_uri = request.getfixturevalue(data_uri)
    storage = cast(SQLStorage, parse_storage(data_uri))
    register_storage(storage)

    # Create a table with colliding column names
    table = pa.Table.from_arrays([[1, 2, 3], [4, 5, 6]], ["column_name", "COLUMN_NAME"])
    structure = TableStructure.from_arrow_table(table)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-tiled-sql-table",
        structure_family=StructureFamily.table,
        structure=structure,
        parameters={"table_name": "table_name"},
        assets=[],
    )
    with pytest.raises(ValueError, match=r"Column names must be unique.+"):
        SQLAdapter.init_storage(data_source=data_source, storage=storage)

    # Create a table with mixed cases in column names
    table = pa.Table.from_arrays([[1, 2, 3], [4, 5, 6]], ["lower_case", "UPPER_CASE"])
    structure = TableStructure.from_arrow_table(table)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-tiled-sql-table",
        structure_family=StructureFamily.table,
        structure=structure,
        parameters={"table_name": "table_name"},
        assets=[],
    )
    data_source = SQLAdapter.init_storage(data_source=data_source, storage=storage)
    assert data_source is not None

    # Write to and read from the table
    adapter = SQLAdapter(
        data_source.assets[0].data_uri,
        structure=data_source.structure,
        table_name=data_source.parameters["table_name"],
        dataset_id=data_source.parameters["dataset_id"],
    )
    adapter.append_partition(table, 0)
    assert adapter.table_name == "table_name"
    assert set(adapter.read().columns) == {"lower_case", "UPPER_CASE"}

    storage.dispose()  # Close all connections


@pytest.mark.parametrize(
    "initial, appended",
    [
        ([1, 2, 3], [None, None, None]),
        ([1.5, 2.5, 3.5], [None, None, None]),
        (["a", "b", "c"], [None, None, None]),
        ([[1], [2, 4], [3]], [[], [], []]),
        ([[1], [2, 4], [3]], [None, None, None]),
        ([[1], [2, 4], [3]], [[None], [None], [None]]),
        ([[1.5], [2.5, 4.5], [3.5]], [[], [], []]),
        ([[1.5], [2.5, 4.5], [3.5]], [None, None, None]),
        ([[1.5], [2.5, 4.5], [3.5]], [[None], [None], [None]]),
        ([["a"], ["b1", "b2"], ["c"]], [[], [], []]),
        ([["a"], ["b1", "b2"], ["c"]], [[None], [None], [None]]),
        ([["a"], ["b1", "b2"], ["c"]], [None, None, None]),
    ],
)
@pytest.mark.parametrize("data_uri", ["sqlite_uri", "duckdb_uri", "postgres_uri"])
def test_append_nullable(
    initial: list[Any],
    appended: list[Any],
    data_uri: str,
    request: pytest.FixtureRequest,
) -> None:
    "Test appending nullable data in an SQL table and reading them back."

    if (data_uri == "sqlite_uri") and (isinstance(initial[0], list)):
        pytest.xfail(reason="Unsupported PyArrow type in SQLite")

    def deep_array_equal(a1: Any, a2: Any) -> bool:
        "Compare two (possibly nested) arrays for equality, including NaN values."
        if not (isinstance(a1, np.ndarray) and isinstance(a2, np.ndarray)):
            # Both are scalar values
            return bool((a1 == a2) or (np.isnan(a1) and np.isnan(a2)))
        elif (len(a1) == 0) and (len(a2) == 0):
            # Both are empty arrays
            return True
        elif len(a1) != len(a2):
            return False
        else:
            return all(deep_array_equal(x1, x2) for x1, x2 in zip(a1, a2))

    # Define a table and a storage
    data_uri = request.getfixturevalue(data_uri)
    storage = cast(SQLStorage, parse_storage(data_uri))
    register_storage(storage)

    # Create a table to be appended
    table_0 = pa.Table.from_arrays([initial], ["part_column"])
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-tiled-sql-table",
        structure_family=StructureFamily.table,
        structure=TableStructure.from_arrow_table(table_0),
        parameters={"table_name": "part_table"},
        assets=[],
    )
    data_source = SQLAdapter.init_storage(data_source=data_source, storage=storage)

    # Write the first part of the data to the table
    adapter_part = SQLAdapter(
        data_source.assets[0].data_uri,
        structure=data_source.structure,
        table_name=data_source.parameters["table_name"],
        dataset_id=data_source.parameters["dataset_id"],
    )
    adapter_part.append_partition(table_0, 0)

    # Write the second part of the data to the table and read it back
    table_1 = pa.Table.from_arrays([appended], ["part_column"])
    adapter_part.append_partition(table_1, 0)
    result_part = adapter_part.read()["part_column"].to_numpy()

    # Write the full table at once and read it back
    table_full = pa.Table.from_arrays([initial + appended], ["full_column"])
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-tiled-sql-table",
        structure_family=StructureFamily.table,
        structure=TableStructure.from_arrow_table(table_full),
        parameters={"table_name": "full_table"},
        assets=[],
    )
    data_source = SQLAdapter.init_storage(data_source=data_source, storage=storage)
    adapter_full = SQLAdapter(
        data_source.assets[0].data_uri,
        structure=data_source.structure,
        table_name=data_source.parameters["table_name"],
        dataset_id=data_source.parameters["dataset_id"],
    )
    adapter_full.append_partition(table_full, 0)
    result_full = adapter_full.read()["full_column"].to_numpy()

    # Check if the data matches in both cases
    assert deep_array_equal(result_part, result_full)

    storage.dispose()  # Close all connections
