from collections.abc import Callable
from typing import cast

import awkward as ak
import numpy as np
import pyarrow as pa
import pyarrow.feather
import pyarrow.parquet
import pytest
import ragged

from tests.adapters.test_sql import adapter_duckdb_one_partition  # noqa: F401
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.sql import SQLAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context, record_history
from tiled.client.utils import ClientError
from tiled.serialization.ragged import (
    _construct_ragged,
    _deconstruct_ragged,
    from_json,
    from_zipped_buffers,
    to_json,
    to_zipped_buffers,
)
from tiled.server.app import build_app
from tiled.storage import SQLStorage, parse_storage, register_storage
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.ragged import make_ragged_array
from tiled.structures.table import TableStructure
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE

rng = np.random.default_rng(42)

arrays = {
    # "empty_1d": ragged.array([]),      # awkward.to_parquet raises on `0 * unknown` type
    # "empty_nd": ragged.array([[], [], []]),  # round-trips with unknown element type, not float64
    "numpy_1d": ragged.array(rng.random(10)),
    "numpy_2d": ragged.array(rng.random((3, 5))),
    "numpy_3d": ragged.array(rng.random((2, 3, 4))),
    "numpy_4d": rng.random((2, 3, 2, 3)),  # testing numpy conversion path
    "regular_1d": ragged.array(rng.random(10).tolist()),
    "regular_2d": ragged.array(rng.random((3, 5)).tolist()),
    "regular_3d": ragged.array(rng.random((2, 3, 4)).tolist()),
    "regular_4d": ragged.array(rng.random((2, 3, 2, 3)).tolist()),
    "ragged_a": [
        rng.random(3),
        rng.random(5),
        rng.random(8),
    ],  # testing list-of-lists conversion path
    "ragged_b": ak.Array(
        [rng.random((2, 3, 4)), rng.random((3, 4, 5))]
    ),  # testing awkward conversion path
    "ragged_c": ragged.array(
        [
            [rng.random(10)],
            [rng.random(8), []],
            [rng.random(5), rng.random(2)],
            [[], rng.random(7)],
        ]
    ),
    "ragged_d": ragged.array(
        [
            [rng.random((4, 3))],
            [rng.random((2, 8)), [[]]],
            [rng.random((5, 2)), rng.random((3, 3))],
            [[[]], rng.random((7, 1))],
        ],
        dtype=np.float32,
    ),
}


@pytest.fixture(scope="module")
def module_client(tmpdir_module):
    """Module-scoped client with all test arrays pre-written under their own keys."""
    catalog = in_memory(writable_storage=str(tmpdir_module))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        for name, array in arrays.items():
            client.write_ragged(array, key=name)
        yield client


@pytest.mark.parametrize("name", arrays.keys())
def test_serialization_roundtrip(name):
    array = ragged.array(arrays[name])

    # Test reduced/flattened numpy array.
    _array, _offsets, _shape = _deconstruct_ragged(array)
    array_from_flattened = _construct_ragged(
        _array, dtype=_array.dtype.type, offsets=_offsets, shape=_shape
    )
    assert ak.array_equal(array._impl, array_from_flattened._impl)

    # Test JSON serialization.
    json_contents = to_json("application/json", array, metadata={})
    array_from_json = from_json(
        json_contents, dtype=array.dtype.type, offsets=_offsets, shape=_shape
    )
    assert ak.array_equal(array._impl, array_from_json._impl)

    # Test flattened octet-stream serialization.
    octet_stream_contents = to_zipped_buffers("application/zip", array, metadata={})
    array_from_octet_stream = from_zipped_buffers(
        octet_stream_contents, dtype=array.dtype.type
    )
    assert ak.array_equal(array._impl, array_from_octet_stream._impl)


@pytest.mark.parametrize("name", arrays.keys())
def test_slicing(module_client, name):
    array = make_ragged_array(arrays[name])
    rac = module_client[name]

    # Read the data back out from the RaggedClient, progressively sliced.
    result = rac.read()
    # ragged does not have an array_equal(a, b) equivalent. Use awkward.
    assert ak.array_equal(result._impl, array._impl)

    # When sliced, the server sends less data.
    with record_history() as h:
        full_result = rac[:]
    assert ak.array_equal(full_result._impl, array._impl)
    assert len(h.responses) == 1  # sanity check
    full_response_size = len(h.responses[0].content)

    # index at first dimension
    with record_history() as h:
        sliced_result = rac[1]
    assert ak.array_equal(sliced_result._impl, array[1]._impl)
    assert len(h.responses) == 1  # sanity check
    sliced_response_size = len(h.responses[0].content)
    assert sliced_response_size < full_response_size

    if len(array.shape) < 2:
        # next slices will produce expected errors
        return

    # index at first and second dimension
    with record_history() as h:
        sliced_result = rac[1, 0]
    assert ak.array_equal(sliced_result._impl, array[1, 0]._impl)
    assert len(h.responses) == 1  # sanity check
    sliced_response_size = len(h.responses[0].content)
    assert sliced_response_size < full_response_size

    # index at second dimension
    with record_history() as h:
        sliced_result = rac[:, 0]
    assert ak.array_equal(sliced_result._impl, array[:, 0]._impl)
    assert len(h.responses) == 1  # sanity check
    sliced_response_size = len(h.responses[0].content)
    assert sliced_response_size < full_response_size


partitionable_size = 30
partitionable_arrays = [
    ragged.array(
        [
            rng.random(size=partitionable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(20)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(5)],
            rng.random(size=partitionable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(15)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(10)],
            rng.random(size=partitionable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(10)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(15)],
            rng.random(size=partitionable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(5)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(20)],
            rng.random(size=partitionable_size),
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            rng.random(size=rng.integers(0, partitionable_size)).tolist()
            for _ in range(20)
        ],
        dtype=np.float32,
    ),
]


@pytest.fixture(scope="module")
def partitioning_client(tmpdir_module):
    """Module-scoped client with all partitionable arrays pre-written under their own keys."""
    catalog = in_memory(writable_storage=str(tmpdir_module))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        max_partition_bytes = (partitionable_size * np.float32(0).nbytes) + (
            2 * np.int64(0).nbytes
        )
        for i, array in enumerate(partitionable_arrays):
            client.write_ragged(
                array, key=f"partitionable_{i}", max_partition_bytes=max_partition_bytes
            )
        yield client


@pytest.mark.parametrize("i", range(len(partitionable_arrays)))
def test_partitioning(partitioning_client, i: int):
    array = ragged.array(partitionable_arrays[i])
    rac = partitioning_client[f"partitionable_{i}"]

    # need to add a little bit to account for Awkward metadata
    assert rac.npartitions > 1

    starts = rac.partitions[:-1]
    stops = rac.partitions[1:]
    for j, (start, stop) in enumerate(zip(starts, stops, strict=True)):
        part = rac.read_block(j)
        assert ak.array_equal(part._impl, array[start:stop]._impl)

        part = rac.read_block(j, slice=(slice(None), slice(0, 4)))
        assert ak.array_equal(part._impl, array[start:stop, slice(0, 4)]._impl)

    full = rac.read()
    assert ak.array_equal(full._impl, array._impl)

    sliced = rac[1:10, 0:5]
    assert ak.array_equal(sliced._impl, array[1:10, 0:5]._impl)


@pytest.mark.parametrize("name", arrays.keys())
def test_export_json(tmpdir, module_client, name):
    array = ragged.array(arrays[name])
    rac = module_client[name]

    filepath = tmpdir / "actual.json"
    rac.export(str(filepath), format="application/json")
    actual = filepath.read_text(encoding="utf-8")
    assert actual == ak.to_json(array._impl)

    if array[1].ndim == 0:
        with pytest.raises(ClientError):
            rac.export(str(filepath), slice=(1,), format="application/json")
    else:
        rac.export(str(filepath), slice=(1,), format="application/json")
        actual = filepath.read_text(encoding="utf-8")
        assert actual == ak.to_json(array[1]._impl)


@pytest.mark.parametrize("name", arrays.keys())
def test_export_arrow(tmpdir, module_client, name):
    array = ragged.array(arrays[name])
    rac = module_client[name]

    filepath = tmpdir / "actual.arrow"
    rac.export(str(filepath), format=APACHE_ARROW_FILE_MIME_TYPE)
    actual = pyarrow.feather.read_table(filepath)
    expected = ak.to_arrow_table(array._impl)
    assert actual == expected

    if array[1].ndim == 0:
        with pytest.raises(ClientError):
            rac.export(str(filepath), slice=(1,), format=APACHE_ARROW_FILE_MIME_TYPE)
    else:
        rac.export(str(filepath), slice=(1,), format=APACHE_ARROW_FILE_MIME_TYPE)
        actual = pyarrow.feather.read_table(filepath)
        expected = ak.to_arrow_table(array[1]._impl)
        assert actual == expected


@pytest.mark.parametrize("name", arrays.keys())
def test_export_parquet(tmpdir, module_client, name):
    array = ragged.array(arrays[name])
    rac = module_client[name]

    filepath = tmpdir / "actual.parquet"
    rac.export(str(filepath), format="application/x-parquet")
    # Test this against pyarrow
    actual = pyarrow.parquet.read_table(filepath)
    expected = ak.to_arrow_table(array._impl)
    assert actual == expected

    if array[1].ndim == 0:
        with pytest.raises(ClientError):
            rac.export(str(filepath), slice=(1,), format="application/x-parquet")
    else:
        rac.export(str(filepath), slice=(1,), format="application/x-parquet")
        # Test this against pyarrow
        actual = pyarrow.parquet.read_table(filepath)
        expected = ak.to_arrow_table(array[1]._impl)
        assert actual == expected


#
# extend existing SQLAdapter tests
#


# NOTE: Arrow seems to only accept 2-dimensional arrays

arrow_keys = "a", "b"
arrow_data_0 = [
    pa.array([rng.random(size=rng.integers(1, 50)) for _ in range(3)]),
    pa.array([rng.random(size=rng.integers(1, 50)) for _ in range(3)]),
]

arrow_data_1 = [
    pa.array([rng.random(size=rng.integers(1, 50)) for _ in range(6)]),
    pa.array([rng.random(size=rng.integers(1, 50)) for _ in range(6)]),
]

arrow_batch_0 = pa.record_batch(arrow_data_0, arrow_keys)
arrow_batch_n = pa.record_batch(arrow_data_1, arrow_keys)


@pytest.fixture
def data_source_from_init_storage() -> Callable[[str, int], DataSource[TableStructure]]:
    def _data_source_from_init_storage(
        data_uri: str, num_partitions: int
    ) -> DataSource[TableStructure]:
        table = pa.Table.from_arrays(arrow_data_0, arrow_keys)
        structure = TableStructure.from_arrow_table(table, npartitions=num_partitions)
        data_source = DataSource(
            management=Management.writable,
            mimetype="application/x-tiled-sql-table",
            structure_family=StructureFamily.table,
            structure=structure,
            assets=[],
        )

        storage = cast("SQLStorage", parse_storage(data_uri))
        register_storage(storage)
        return SQLAdapter.init_storage(data_source=data_source, storage=storage)

    return _data_source_from_init_storage


@pytest.fixture
def context_from_adapter(
    adapter_duckdb_one_partition,  # noqa: F811
):
    table = pa.Table.from_batches([arrow_batch_0, arrow_batch_n])
    adapter_duckdb_one_partition.append_partition(0, table)
    adapter = MapAdapter({"foo": adapter_duckdb_one_partition})
    app = build_app(adapter)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture
def client_from_adapter(context_from_adapter):
    return from_context(context_from_adapter, include_data_sources=True)


@pytest.mark.parametrize("name", arrow_keys)
def test_read_from_sql(client_from_adapter, name: str) -> None:
    index = arrow_keys.index(name)
    expected = ragged.array(
        [*arrow_data_0[index].tolist(), *arrow_data_1[index].tolist()]
    )
    client = client_from_adapter[f"foo/{name}"]

    result = client.read()
    assert ak.array_equal(result._impl, expected._impl)

    result = client[1]
    assert ak.array_equal(result._impl, expected[1]._impl)

    result = client[2:5, 0]
    assert ak.array_equal(result._impl, expected[2:5, 0]._impl)
