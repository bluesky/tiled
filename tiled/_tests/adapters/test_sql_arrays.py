from typing import Callable, cast

import numpy as np
import pyarrow as pa
import pytest

from tiled._tests.adapters.test_sql import adapter_duckdb_many_partitions  # noqa: F401
from tiled._tests.adapters.test_sql import adapter_duckdb_one_partition  # noqa: F401
from tiled._tests.adapters.test_sql import adapter_psql_many_partitions  # noqa: F401
from tiled._tests.adapters.test_sql import adapter_psql_one_partition  # noqa: F401
from tiled._tests.adapters.test_sql import assert_same_rows
from tiled.adapters.sql import SQLAdapter
from tiled.storage import SQLStorage, parse_storage, register_storage
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.table import TableStructure

rng = np.random.default_rng(42)

names = ["i0", "i1", "i2", "i3", "f4", "f5"]
batch_size = 5
data0 = [
    pa.array(
        [rng.integers(-100, 100, size=10, dtype=np.int8) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=11, dtype=np.int16) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=12, dtype=np.int32) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=13, dtype=np.int64) for _ in range(batch_size)]
    ),
    pa.array([rng.random(size=14, dtype=np.float32) for _ in range(batch_size)]),
    pa.array([rng.random(size=15, dtype=np.float64) for _ in range(batch_size)]),
]
batch_size = 8
data1 = [
    pa.array(
        [rng.integers(-100, 100, size=10, dtype=np.int8) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=11, dtype=np.int16) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=12, dtype=np.int32) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=13, dtype=np.int64) for _ in range(batch_size)]
    ),
    pa.array([rng.random(size=14, dtype=np.float32) for _ in range(batch_size)]),
    pa.array([rng.random(size=15, dtype=np.float64) for _ in range(batch_size)]),
]
batch_size = 3
data2 = [
    pa.array(
        [rng.integers(-100, 100, size=10, dtype=np.int8) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=11, dtype=np.int16) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=12, dtype=np.int32) for _ in range(batch_size)]
    ),
    pa.array(
        [rng.integers(-100, 100, size=13, dtype=np.int64) for _ in range(batch_size)]
    ),
    pa.array([rng.random(size=14, dtype=np.float32) for _ in range(batch_size)]),
    pa.array([rng.random(size=15, dtype=np.float64) for _ in range(batch_size)]),
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


@pytest.mark.parametrize(
    "adapter_name", [("adapter_duckdb_one_partition"), ("adapter_psql_one_partition")]
)
def test_write_read_one_batch_one_part(
    adapter_name: str, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter: SQLAdapter = request.getfixturevalue(adapter_name)

    # test appending and reading a table as a whole
    test_table = pa.Table.from_arrays(data0, names)

    adapter.append_partition(batch0, 0)
    result_read = adapter.read()
    assert test_table == pa.Table.from_pandas(result_read)

    # test appending and reading a partition in a table
    result_read_partition = adapter.read_partition(0)
    assert test_table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize(
    "adapter_name", [("adapter_duckdb_one_partition"), ("adapter_psql_one_partition")]
)
def test_write_read_list_batch_one_part(
    adapter_name: str, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter: SQLAdapter = request.getfixturevalue(adapter_name)

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


@pytest.mark.parametrize(
    "adapter_name",
    [("adapter_duckdb_many_partitions"), ("adapter_psql_many_partitions")],
)
def test_append_single_partition(
    adapter_name: str, request: pytest.FixtureRequest
) -> None:
    # get adapter from fixture
    adapter: SQLAdapter = request.getfixturevalue(adapter_name)

    # test writing an entire pyarrow table to a single partition
    table = pa.Table.from_batches([batch0, batch1, batch2])
    adapter.append_partition(table, 0)

    result_read = adapter.read()
    assert table == pa.Table.from_pandas(result_read)

    # test reading a specific partition
    result_read_partition = adapter.read_partition(0)
    assert table == pa.Table.from_pandas(result_read_partition)


@pytest.mark.parametrize("adapter_name", [("adapter_psql_many_partitions")])
@pytest.mark.parametrize("field", names)
def test_write_read_one_batch_many_part(
    adapter_name: str, request: pytest.FixtureRequest, field: str
) -> None:
    # get adapter from fixture
    adapter: SQLAdapter = request.getfixturevalue(adapter_name)

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
    field_index = names.index(field)
    assert np.array_equal(
        [*data0[field_index].tolist(), *data2[field_index].tolist()],
        result_read[field].tolist(),
    )
    result_read = adapter.read_partition(1, fields=[field])
    assert np.array_equal(
        [*data1[field_index].tolist(), *data0[field_index].tolist()],
        result_read[field].tolist(),
    )
    result_read = adapter.read_partition(2, fields=[field])
    assert np.array_equal(
        [*data2[field_index].tolist(), *data1[field_index].tolist()],
        result_read[field].tolist(),
    )
