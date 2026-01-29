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
from tiled.serialization.ragged import (
    from_json,
    from_numpy_array,
    from_numpy_octet_stream,
    from_zipped_buffers,
    to_json,
    to_numpy_array,
    to_numpy_octet_stream,
    to_zipped_buffers,
)
from tiled.server.app import build_app
from tiled.storage import SQLStorage, parse_storage, register_storage
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.ragged import RaggedStructure
from tiled.structures.table import TableStructure
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE


@pytest.fixture
def catalog(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    yield catalog


@pytest.fixture
def app(catalog):
    app = build_app(catalog)
    yield app


@pytest.fixture
def context(app):
    with Context.from_app(app) as context:
        yield context


@pytest.fixture
def client(context):
    client = from_context(context)
    yield client


RNG = np.random.default_rng(42)

arrays = {
    # "empty_1d": ragged.array([]),
    # "empty_nd": ragged.array([[], [], []]),
    "numpy_1d": ragged.array(RNG.random(10)),
    "numpy_2d": ragged.array(RNG.random((3, 5))),
    "numpy_3d": ragged.array(RNG.random((2, 3, 4))),
    "numpy_4d": ragged.array(RNG.random((2, 3, 2, 3))),
    "regular_1d": ragged.array(RNG.random(10).tolist()),
    "regular_2d": ragged.array(RNG.random((3, 5)).tolist()),
    "regular_3d": ragged.array(RNG.random((2, 3, 4)).tolist()),
    "regular_4d": ragged.array(RNG.random((2, 3, 2, 3)).tolist()),
    "ragged_a": ragged.array([RNG.random(3), RNG.random(5), RNG.random(8)]),
    "ragged_b": ragged.array([RNG.random((2, 3, 4)), RNG.random((3, 4, 5))]),
    "ragged_c": ragged.array(
        [
            [RNG.random(10)],
            [RNG.random(8), []],
            [RNG.random(5), RNG.random(2)],
            [[], RNG.random(7)],
        ]
    ),
    "ragged_d": ragged.array(
        [
            [RNG.random((4, 3))],
            [RNG.random((2, 8)), [[]]],
            [RNG.random((5, 2)), RNG.random((3, 3))],
            [[[]], RNG.random((7, 1))],
        ]
    ),
}


@pytest.mark.parametrize("name", arrays.keys())
def test_serialization_roundtrip(name):
    array = arrays[name]
    structure = RaggedStructure.from_array(array)

    # Test JSON serialization.
    json_contents = to_json("application/json", array, metadata={})
    array_from_json = from_json(
        json_contents,
        dtype=array.dtype.type,
        offsets=structure.offsets,
        shape=structure.shape,
    )
    assert ak.array_equal(array._impl, array_from_json._impl)  # noqa: SLF001

    # Test reduced/flattened numpy array.
    reduced_array = to_numpy_array(array)
    array_from_flattened = from_numpy_array(
        reduced_array,
        dtype=array.dtype.type,
        offsets=structure.offsets,
        shape=structure.shape,
    )
    assert ak.array_equal(array._impl, array_from_flattened._impl)  # noqa: SLF001

    # Test flattened octet-stream serialization.
    octet_stream_contents = to_numpy_octet_stream(
        "application/octet-stream", array, metadata={}
    )
    array_from_octet_stream = from_numpy_octet_stream(
        octet_stream_contents,
        dtype=array.dtype.type,
        offsets=structure.offsets,
        shape=structure.shape,
    )
    assert ak.array_equal(array._impl, array_from_octet_stream._impl)  # noqa: SLF001

    # Test flattened octet-stream serialization.
    octet_stream_contents = to_zipped_buffers("application/zip", array, metadata={})
    array_from_octet_stream = from_zipped_buffers(
        octet_stream_contents, dtype=array.dtype.type
    )
    assert ak.array_equal(array._impl, array_from_octet_stream._impl)  # noqa: SLF001


@pytest.mark.parametrize("name", arrays.keys())
def test_slicing(client, name):
    # Write data into catalog.
    array = arrays[name]

    returned = client.write_ragged(array, key="test")
    # Test with client returned, and with client from lookup.
    for rac in [returned, client["test"]]:
        # Read the data back out from the RaggedClient, progressively sliced.
        result = rac.read()
        # ragged does not have an array_equal(a, b) equivalent. Use awkward.
        assert ak.array_equal(result._impl, array._impl)  # noqa: SLF001

        # When sliced, the server sends less data.
        with record_history() as h:
            full_result = rac[:]
        assert ak.array_equal(full_result._impl, array._impl)  # noqa: SLF001
        assert len(h.responses) == 1  # sanity check
        full_response_size = len(h.responses[0].content)

        # index at first dimension
        with record_history() as h:
            sliced_result = rac[1]
        assert ak.array_equal(sliced_result._impl, array[1]._impl)  # noqa: SLF001
        assert len(h.responses) == 1  # sanity check
        sliced_response_size = len(h.responses[0].content)
        assert sliced_response_size < full_response_size

        if len(array.shape) < 2:
            # next slices will produce expected errors
            continue

        # index at first and second dimension
        with record_history() as h:
            sliced_result = rac[1, 0]
        assert ak.array_equal(sliced_result._impl, array[1, 0]._impl)  # noqa: SLF001
        assert len(h.responses) == 1  # sanity check
        sliced_response_size = len(h.responses[0].content)
        assert sliced_response_size < full_response_size

        # index at second dimension
        with record_history() as h:
            sliced_result = rac[:, 0]
        assert ak.array_equal(sliced_result._impl, array[:, 0]._impl)  # noqa: SLF001
        assert len(h.responses) == 1  # sanity check
        sliced_response_size = len(h.responses[0].content)
        assert sliced_response_size < full_response_size


@pytest.mark.parametrize("name", arrays.keys())
def test_export_json(tmpdir, client, name):
    array = arrays[name]
    rac = client.write_ragged(array, key="test")

    filepath = tmpdir / "actual.json"
    rac.export(str(filepath), format="application/json")
    actual = filepath.read_text(encoding="utf-8")
    assert actual == ak.to_json(array._impl)  # noqa: SLF001

    # TODO: cannot export a slice that results in a scalar
    rac.export(str(filepath), slice=(1,), format="application/json")
    actual = filepath.read_text(encoding="utf-8")
    assert actual == ak.to_json(array[1]._impl)  # noqa: SLF001


@pytest.mark.parametrize("name", arrays.keys())
def test_export_arrow(tmpdir, client, name):
    array = arrays[name]
    rac = client.write_ragged(array, key="test")

    filepath = tmpdir / "actual.arrow"
    rac.export(str(filepath), format=APACHE_ARROW_FILE_MIME_TYPE)
    actual = pyarrow.feather.read_table(filepath)
    expected = ak.to_arrow_table(array._impl)  # noqa: SLF001
    assert actual == expected

    # TODO: cannot export a slice that results in a scalar
    rac.export(str(filepath), slice=(1,), format=APACHE_ARROW_FILE_MIME_TYPE)
    actual = pyarrow.feather.read_table(filepath)
    expected = ak.to_arrow_table(array[1]._impl)  # noqa: SLF001
    assert actual == expected


@pytest.mark.parametrize("name", arrays.keys())
def test_export_parquet(tmpdir, client, name):
    array = arrays[name]
    rac = client.write_ragged(array, key="test")

    filepath = tmpdir / "actual.parquet"
    rac.export(str(filepath), format="application/x-parquet")
    # Test this against pyarrow
    actual = pyarrow.parquet.read_table(filepath)
    expected = ak.to_arrow_table(array._impl)  # noqa: SLF001
    assert actual == expected

    # TODO: cannot export a slice that results in a scalar
    rac.export(str(filepath), slice=(1,), format="application/x-parquet")
    # Test this against pyarrow
    actual = pyarrow.parquet.read_table(filepath)
    expected = ak.to_arrow_table(array[1]._impl)  # noqa: SLF001
    assert actual == expected


#
# extend existing SQLAdapter tests
#


# NOTE: Arrow seems to only accept 2-dimensional arrays

arrow_keys = "a", "b"
arrow_data_0 = [
    pa.array([RNG.random(size=RNG.integers(1, 50)) for _ in range(3)]),
    pa.array([RNG.random(size=RNG.integers(1, 50)) for _ in range(3)]),
]

arrow_data_1 = [
    pa.array([RNG.random(size=RNG.integers(1, 50)) for _ in range(6)]),
    pa.array([RNG.random(size=RNG.integers(1, 50)) for _ in range(6)]),
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
def client_from_adapter(
    context_from_adapter,
):
    return from_context(context_from_adapter, include_data_sources=True)


@pytest.mark.parametrize("name", arrow_keys)
def test_read_ragged_array_from_sql(
    client_from_adapter,
    name: str,
) -> None:
    index = arrow_keys.index(name)
    expected = ragged.array(
        [
            *arrow_data_0[index].tolist(),
            *arrow_data_1[index].tolist(),
        ]
    )
    client = client_from_adapter[f"foo/{name}"]

    result = client.read()
    assert ak.array_equal(result._impl, expected._impl)  # noqa: SLF001

    result = client[1]
    assert ak.array_equal(result._impl, expected[1]._impl)  # noqa: SLF001

    result = client[2:5, 0]
    assert ak.array_equal(result._impl, expected[2:5, 0]._impl)  # noqa: SLF001
