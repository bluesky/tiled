from collections.abc import Generator
from pathlib import Path
from typing import Callable

import adbc_driver_duckdb
import numpy
import pyarrow as pa
import pytest

from tiled.adapters.sql import SQLAdapter
from tiled.storage import parse_storage, register_storage
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.table import TableStructure

names = ["f0", "f1", "f2", "f3", "f4", "f5"]
data0 = [
    pa.array([numpy.ones(3, dtype=numpy.int8) * i for i in range(1, 6)]),
    pa.array([numpy.ones(3, dtype=numpy.int16) * i for i in range(1, 6)]),
    pa.array([numpy.ones(3, dtype=numpy.int32) * i for i in range(1, 6)]),
    pa.array([numpy.ones(3, dtype=numpy.int64) * i for i in range(1, 6)]),
    pa.array([numpy.ones(3, dtype=numpy.float32) * i for i in range(1, 6)]),
    pa.array([numpy.ones(3, dtype=numpy.float64) * i for i in range(1, 6)]),
]
data1 = [
    pa.array([numpy.ones(3, dtype=numpy.int8) * i for i in range(6, 13)]),
    pa.array([numpy.ones(3, dtype=numpy.int16) * i for i in range(6, 13)]),
    pa.array([numpy.ones(3, dtype=numpy.int32) * i for i in range(6, 13)]),
    pa.array([numpy.ones(3, dtype=numpy.int64) * i for i in range(6, 13)]),
    pa.array([numpy.ones(3, dtype=numpy.float32) * i for i in range(6, 13)]),
    pa.array([numpy.ones(3, dtype=numpy.float64) * i for i in range(6, 13)]),
]
data2 = [
    pa.array([numpy.ones(3, dtype=numpy.int8) * i for i in range(13, 15)]),
    pa.array([numpy.ones(3, dtype=numpy.int16) * i for i in range(13, 15)]),
    pa.array([numpy.ones(3, dtype=numpy.int32) * i for i in range(13, 15)]),
    pa.array([numpy.ones(3, dtype=numpy.int64) * i for i in range(13, 15)]),
    pa.array([numpy.ones(3, dtype=numpy.float32) * i for i in range(13, 15)]),
    pa.array([numpy.ones(3, dtype=numpy.float64) * i for i in range(13, 15)]),
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

        storage = parse_storage(data_uri)
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
    assert isinstance(
        adapter_duckdb_one_partition.conn, adbc_driver_duckdb.dbapi.Connection
    )


def test_attributes_duckdb_many_part(
    adapter_duckdb_many_partitions: SQLAdapter,
) -> None:
    assert adapter_duckdb_many_partitions.structure().columns == names
    assert adapter_duckdb_many_partitions.structure().npartitions == 3
    assert isinstance(
        adapter_duckdb_many_partitions.conn, adbc_driver_duckdb.dbapi.Connection
    )


@pytest.fixture
def adapter_psql_one_partition(
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    postgres_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = data_source_from_init_storage(postgres_uri, 1)
    adapter = SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )
    yield adapter
    adapter.close()


@pytest.fixture
def adapter_psql_many_partitions(
    data_source_from_init_storage: Callable[[str, int], DataSource[TableStructure]],
    postgres_uri: str,
) -> SQLAdapter:
    data_source = data_source_from_init_storage(postgres_uri, 3)
    return SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


def test_psql(adapter_psql_one_partition: SQLAdapter) -> None:
    assert adapter_psql_one_partition.structure().columns == names
    assert adapter_psql_one_partition.structure().npartitions == 1


@pytest.mark.parametrize(
    "adapter", [("adapter_duckdb_one_partition"), ("adapter_psql_one_partition")]
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
    assert test_table == pa.Table.from_pandas(result_read)

    # test appending and reading a partition in a table
    result_read_partition = adapter.read_partition(0)
    assert test_table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize(
    "adapter", [("adapter_duckdb_one_partition"), ("adapter_psql_one_partition")]
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

    assert test_table == pa.Table.from_pandas(result_read)

    # test appending and reading a partition in a table
    result_read_partition = adapter.read_partition(0)

    assert test_table == pa.Table.from_pandas(result_read_partition)

    # test appending few more times done correctly
    test_table = pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    )
    adapter.append_partition([batch2, batch0, batch1], 0)
    adapter.append_partition([batch1, batch2, batch0], 0)
    result_read = adapter.read()

    assert test_table == pa.Table.from_pandas(result_read)

    # test appending a few times and reading done correctly
    result_read_partition = adapter.read_partition(0)

    assert test_table == pa.Table.from_pandas(result_read_partition)


def assert_same_rows(table1: pa.Table, table2: pa.Table) -> None:
    "Verify that two tables have the same rows, regardless of order."
    assert table1.num_rows == table2.num_rows

    rows1 = {tuple(row) for row in table1.to_pylist()}
    rows2 = {tuple(row) for row in table2.to_pylist()}
    assert rows1 == rows2


@pytest.mark.parametrize(
    "adapter", [("adapter_duckdb_many_partitions"), ("adapter_psql_many_partitions")]
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
    assert table == pa.Table.from_pandas(result_read)

    # test reading a specific partition
    result_read_partition = adapter.read_partition(0)
    assert table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize("adapter", [("adapter_psql_many_partitions")])
@pytest.mark.parametrize("field", names)
def test_write_read_one_batch_many_part(
    adapter: SQLAdapter, request: pytest.FixtureRequest, field: str
) -> None:
    # get adapter from fixture
    adapter = request.getfixturevalue(adapter)

    # test writing to many partitions and reading it whole
    adapter.append_partition(batch0, 0)
    adapter.append_partition(batch1, 1)
    adapter.append_partition(batch2, 2)

    result_read = adapter.read()

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
    result_read = adapter.read_partition(0, fields=[field])
    assert numpy.array_equal(
        [*data0[1].tolist(), *data2[1].tolist()], result_read[field].tolist()
    )
    result_read = adapter.read_partition(1, fields=[field])
    assert numpy.array_equal(
        [*data1[0].tolist(), *data0[0].tolist()], result_read[field].tolist()
    )
    result_read = adapter.read_partition(2, fields=[field])
    assert numpy.array_equal(
        [*data2[2].tolist(), *data1[2].tolist()], result_read[field].tolist()
    )
