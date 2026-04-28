from pathlib import Path
from typing import Callable, Dict, Generator, Type, Union, cast

import awkward as ak
import numpy as np
import pyarrow as pa
import pytest
import ragged

from tests.adapters.test_sql import adapter_duckdb_many_partitions  # noqa: F401
from tests.adapters.test_sql import adapter_duckdb_one_partition  # noqa: F401
from tests.adapters.test_sql import adapter_psql_many_partitions  # noqa: F401
from tests.adapters.test_sql import adapter_psql_one_partition  # noqa: F401
from tests.adapters.test_sql import assert_same_rows
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.ragged import RaggedAdapter
from tiled.adapters.sql import SQLAdapter
from tiled.storage import SQLStorage, get_storage, parse_storage, register_storage
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.table import TableStructure
from tiled.utils import sanitize_uri

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

    adapter.append_partition(0, batch0)
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
    adapter.append_partition(0, [batch0, batch1, batch2])
    result_read = adapter.read()

    assert test_table == pa.Table.from_pandas(result_read)

    # test appending and reading a partition in a table
    result_read_partition = adapter.read_partition(0)

    assert test_table == pa.Table.from_pandas(result_read_partition)

    # test appending few more times done correctly
    test_table = pa.Table.from_batches(
        [batch0, batch1, batch2, batch2, batch0, batch1, batch1, batch2, batch0]
    )
    adapter.append_partition(0, [batch2, batch0, batch1])
    adapter.append_partition(0, [batch1, batch2, batch0])
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
    adapter.append_partition(0, table)

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
    adapter.append_partition(0, batch0)
    adapter.append_partition(1, batch1)
    adapter.append_partition(2, batch2)

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
    adapter.append_partition(1, batch0)
    adapter.append_partition(2, batch1)
    adapter.append_partition(0, batch2)

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


# Ragged-specific tests: verify that SQL columns containing variable-length
# arrays are exposed as the correct adapter type and read back correctly.

ragged_names = ["integers", "floats", "ragged_floats"]
ragged_names_adapters: Dict[str, Type[Union[ArrayAdapter, RaggedAdapter]]] = {
    "integers": ArrayAdapter,
    "floats": ArrayAdapter,
    "ragged_floats": RaggedAdapter,
}
batch_size = 5
ragged_data0 = [
    pa.array([rng.integers(-100, 100, size=10) for _ in range(batch_size)]),
    pa.array([rng.random(size=15) for _ in range(batch_size)]),
    pa.array([rng.random(size=rng.integers(1, 10)) for _ in range(batch_size)]),
]
batch_size = 8
ragged_data1 = [
    pa.array([rng.integers(-100, 100, size=10) for _ in range(batch_size)]),
    pa.array([rng.random(size=15) for _ in range(batch_size)]),
    pa.array([rng.random(size=rng.integers(1, 10)) for _ in range(batch_size)]),
]
batch_size = 3
ragged_data2 = [
    pa.array([rng.integers(-100, 100, size=10) for _ in range(batch_size)]),
    pa.array([rng.random(size=15) for _ in range(batch_size)]),
    pa.array([rng.random(size=rng.integers(1, 10)) for _ in range(batch_size)]),
]
ragged_batch0 = pa.record_batch(ragged_data0, names=ragged_names)
ragged_batch1 = pa.record_batch(ragged_data1, names=ragged_names)
ragged_batch2 = pa.record_batch(ragged_data2, names=ragged_names)
ragged_table = pa.Table.from_batches([ragged_batch0, ragged_batch1, ragged_batch2])


@pytest.fixture
def ragged_data_source_from_init_storage() -> (
    Callable[[str, int], DataSource[TableStructure]]
):
    def _init(data_uri: str, num_partitions: int) -> DataSource[TableStructure]:
        structure = TableStructure.from_arrow_table(
            ragged_table, npartitions=num_partitions
        )
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

    return _init


@pytest.fixture
def ragged_adapter_duckdb(
    tmp_path: Path,
    ragged_data_source_from_init_storage: Callable[
        [str, int], DataSource[TableStructure]
    ],
    duckdb_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = ragged_data_source_from_init_storage(duckdb_uri, 1)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )


@pytest.fixture
def ragged_adapter_psql(
    ragged_data_source_from_init_storage: Callable[
        [str, int], DataSource[TableStructure]
    ],
    postgres_uri: str,
) -> Generator[SQLAdapter, None, None]:
    data_source = ragged_data_source_from_init_storage(postgres_uri, 1)
    yield SQLAdapter(
        data_source.assets[0].data_uri,
        data_source.structure,
        data_source.parameters["table_name"],
        data_source.parameters["dataset_id"],
    )
    storage = get_storage(sanitize_uri(postgres_uri)[0])
    cast(SQLStorage, storage).dispose()


@pytest.mark.parametrize(
    "sql_adapter_fixture",
    ["ragged_adapter_duckdb", "ragged_adapter_psql"],
)
@pytest.mark.parametrize(
    ("field", "expected_adapter_type"), [*ragged_names_adapters.items()]
)
def test_ragged_column_adapter_type_and_data(
    sql_adapter_fixture: str,
    field: str,
    expected_adapter_type: type,
    request: pytest.FixtureRequest,
) -> None:
    """SQL columns with variable-length list arrays should be exposed as RaggedAdapter;
    fixed-size array columns should be exposed as ArrayAdapter. Data round-trips correctly.
    """
    sql_adapter: SQLAdapter = request.getfixturevalue(sql_adapter_fixture)
    sql_adapter.append_partition(0, ragged_table)

    child_adapter = sql_adapter[field]
    assert isinstance(child_adapter, expected_adapter_type)

    field_index = ragged_names.index(field)
    result_read = child_adapter.read()

    assert isinstance(result_read, (np.ndarray, ragged.array))
    assert ak.array_equal(
        [
            *ragged_data0[field_index].tolist(),
            *ragged_data1[field_index].tolist(),
            *ragged_data2[field_index].tolist(),
        ],
        result_read.tolist(),
    )
