import builtins
from types import SimpleNamespace
from typing import cast

import awkward as ak
import numpy as np
import orjson
import pyarrow.feather
import pyarrow.parquet
import pytest
import ragged

from tiled.adapters.ragged import RaggedAdapter, RaggedSQLAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context, record_history
from tiled.client.utils import ClientError
from tiled.ndslice import NDBlock, NDSlice
from tiled.serialization.ragged import (
    _buffers_from_data,
    from_json,
    from_zipped_buffers,
    to_json,
    to_zipped_buffers,
)
from tiled.server.app import build_app
from tiled.storage import SQLStorage, parse_storage, register_storage
from tiled.structures.array import BuiltinDtype
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource, Management
from tiled.structures.ragged import (
    RaggedStructure,
    make_ragged_array,
    make_ragged_chunks,
)
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE, Conflicts

rng = np.random.default_rng(42)

# Explicitly define a variety of test arrays, covering different shapes and forms in Awkward

# First two dimensions are fixed, shape = (3, 2, None)
# RegularForm(size=2)
# └── ListOffsetForm
#     └── NumpyForm(int64)
node2 = ak.contents.NumpyArray(np.arange(12))
node1 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 2, 5, 5, 9, 10, 12])),
    content=node2,
)
node0 = ak.contents.RegularArray(node1, size=2)
ragged_3x2xNone = ragged.array(node0)

# Pure ListForm, shape = (4, None)
# ListForm
# └── NumpyForm(int64)
node1 = ak.contents.NumpyArray(np.arange(12))
node0 = ak.contents.ListArray(
    starts=ak.index.Index64(np.array([0, 2, 5, 9])),
    stops=ak.index.Index64(np.array([2, 5, 9, 12])),
    content=node1,
)
ragged_4xNone_listform = ragged.array(node0)

# Overlapping ListForms (pathological)
# The original ListForm:
#    content: [0, 1, 2, 3, 4, 5]
# Represents two lists referenceing the same data buffer:
#    content: [0, 1, 2, 3, 4, 5]
#              |--------|
#                    |--------|
#    indx0:    [0, 1, 2, 3]
#    indx1:          [2, 3, 4, 5]
# After canonicalization in ListOffsetForm, this becomes:
#    content: [0, 1, 2, 3, 2, 3, 4, 5]
#              |--------|  |--------|
node1 = ak.contents.NumpyArray(np.array([0, 1, 2, 3, 4, 5]))
node0 = ak.contents.ListArray(
    starts=ak.index.Index64(np.array([0, 2])),
    stops=ak.index.Index64(np.array([4, 6])),
    content=node1,
)
ragged_overlapping_listforms = ragged.array(node0)

# Pure tensor represented by RegularArrays, shape = (8, 3, 2)
# RegularForm(size=3)
# └── RegularForm(size=2)
#     └── NumpyForm(int64)
node2 = ak.contents.NumpyArray(np.arange(48))
node1 = ak.contents.RegularArray(content=node2, size=2)
node0 = ak.contents.RegularArray(content=node1, size=3)
tensor_8x3x2_regular = ragged.array(node0)

# RegularArray over NumpyForm(inner_shape=(3, 2)), shape = (4, 3, 2)
# RegularForm(size=4)
# └── NumpyForm(int64, inner_shape=(3, 2))
node1 = ak.contents.NumpyArray(np.arange(24).reshape(4, 3, 2))
tensor_4x3x2_numpy_inner_shape = ragged.array(node1)

# Array with mixed forms #1, shape = (3, None, 2, None)
# ListOffsetForm
# └── RegularForm(size=2)
#     └── ListForm
#         └── NumpyForm(int64)
node3 = ak.contents.NumpyArray(np.arange(12))
node2 = ak.contents.ListArray(
    starts=ak.index.Index64(np.array([0, 2, 5, 5, 9, 10])),
    stops=ak.index.Index64(np.array([2, 5, 5, 9, 10, 12])),
    content=node3,
)
node1 = ak.contents.RegularArray(node2, size=2)
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 1, 3, 3])),
    content=node1,
)
ragged_3xNonex2xNone = ragged.array(node0)

# Array with mixed forms #2, shape = (2, None, 3, None)
# ListOffsetForm
# └── RegularForm(size=3)
#     └── ListOffsetForm
#         └── NumpyForm(int64)
node3 = ak.contents.NumpyArray(np.arange(30))
node2 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 2, 5, 5, 9, 12, 15, 18, 20, 23, 25, 28, 30])),
    content=node3,
)
node1 = ak.contents.RegularArray(node2, size=3)
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 2, 4])),
    content=node1,
)
ragged_2xNonex3xNone = ragged.array(node0)

# Array with mixed forms #3 (ListForms only), shape = (2, None, 3, None)
# ListForm
# └── RegularForm(size=3)
#     └── ListForm
#         └── NumpyForm(int64)
node3 = ak.contents.NumpyArray(np.arange(18))
node2 = ak.contents.ListArray(
    starts=ak.index.Index64(np.array([0, 2, 5, 5, 9, 12, 15, 15, 16])),
    stops=ak.index.Index64(np.array([2, 5, 5, 9, 12, 15, 15, 16, 18])),
    content=node3,
)
node1 = ak.contents.RegularArray(node2, size=3)
node0 = ak.contents.ListArray(
    starts=ak.index.Index64(np.array([0, 1])),
    stops=ak.index.Index64(np.array([1, 3])),
    content=node1,
)
ragged_2xNonex3xNone_listforms = ragged.array(node0)

# Array with mixed forms #4, shape = (4, None, None, 3, 2)
# ListOffsetForm
# └── ListForm
#     └── RegularForm(size=3)
#         └── RegularForm(size=2)
#             └── NumpyForm(int64)
node4 = ak.contents.NumpyArray(np.arange(72))
node3 = ak.contents.RegularArray(content=node4, size=2)
node2 = ak.contents.RegularArray(content=node3, size=3)
node1 = ak.contents.ListArray(
    starts=ak.index.Index64(np.array([0, 1, 3, 3, 5, 7, 9])),
    stops=ak.index.Index64(np.array([1, 3, 3, 5, 7, 9, 12])),
    content=node2,
)
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 1, 3, 6, 7])),
    content=node1,
)
ragged_4xNonexNonex3x2 = ragged.array(node0)

# Array with mixed forms #5, shape = (4, None, 3, 2)
# leaf NumpyForm has inner_shape = (3, 2)
node1 = ak.contents.NumpyArray(np.arange(7 * 3 * 2).reshape(7, 3, 2))
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 1, 3, 5, 7])),
    content=node1,
)
ragged_4xNonex3x2 = ragged.array(node0)

# Empty ragged axis, shape = (3, None)
# ListOffsetForm
# └── NumpyForm(int64)
node1 = ak.contents.NumpyArray(np.array([], dtype=np.int64))
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 0, 0, 0])),
    content=node1,
)
ragged_3xNone_empty = ragged.array(node0)

# Mixed empty/non-empty ragged axis, shape = (3, None)
# ListOffsetForm
# └── NumpyForm(int64)
node1 = ak.contents.NumpyArray(np.arange(3))
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 0, 2, 3])),
    content=node1,
)
ragged_3xNone_mixed_empty = ragged.array(node0)

# Size-1 RegularArray, shape = (5, 1)
# RegularForm(size=1)
# └── NumpyForm(int64)
node1 = ak.contents.NumpyArray(np.arange(5))
node0 = ak.contents.RegularArray(content=node1, size=1)
ragged_5x1 = ragged.array(node0)

# Ragged with size-1 RegularArray, shape = (2, None, 1, None)
# ListOffsetForm
# └── RegularForm(size=1)
#     └── ListOffsetForm
#         └── NumpyForm(int64)
node3 = ak.contents.NumpyArray(np.arange(6))
node2 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 2, 3, 6])),
    content=node3,
)
node1 = ak.contents.RegularArray(content=node2, size=1)
node0 = ak.contents.ListOffsetArray(
    offsets=ak.index.Index64(np.array([0, 2, 3])),
    content=node1,
)
ragged_2xNonex1xNone = ragged.array(node0)

arrays = {
    # "empty_1d": ragged.array([]),      # awkward.to_parquet raises on `0 * unknown` type
    # "empty_nd": ragged.array([[], [], []]),  # round-trips with unknown element type, not float64
    "numpy_1d": ragged.array(rng.random(10)),
    "ragged_2d": ragged.array([rng.random(3), rng.random(5), rng.random(8)]),
    "numpy_2d": ragged.array(
        [rng.random((3, 5)), rng.random((2, 4)), rng.random((4, 2))]
    ),
    "numpy_3d": ragged.array(rng.random((2, 3, 4))),
    "numpy_4d": rng.random((2, 3, 2, 3)),  # testing numpy conversion path
    "regular_1d": ragged.array(rng.random(10).tolist()),
    "regular_2d": ragged.array(rng.random((3, 5)).tolist()),
    "regular_3d": ragged.array(rng.random((2, 3, 4)).tolist()),
    "regular_4d": ragged.array(rng.random((2, 3, 2, 3)).tolist()),
    # testing list-of-lists conversion path
    "ragged_a": [
        rng.random(3),
        rng.random(5),
        rng.random(8),
    ],
    # testing awkward conversion path
    "ragged_b": ak.Array([rng.random((2, 3, 4)), rng.random((3, 4, 5))]),
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
    "ragged_3x2xNone": ragged_3x2xNone,
    "ragged_3xNonex2xNone": ragged_3xNonex2xNone,
    "ragged_2xNonex3xNone": ragged_2xNonex3xNone,
    "ragged_2xNonex3xNone_listforms": ragged_2xNonex3xNone_listforms,
    "ragged_4xNonexNonex3x2": ragged_4xNonexNonex3x2,
    "ragged_4xNonex3x2": ragged_4xNonex3x2,
    "ragged_4xNone_listform": ragged_4xNone_listform,
    "ragged_overlapping_listforms": ragged_overlapping_listforms,
    "tensor_8x3x2_regular": tensor_8x3x2_regular,
    "tensor_4x3x2_numpy_inner_shape": tensor_4x3x2_numpy_inner_shape,
    "ragged_3xNone_empty": ragged_3xNone_empty,
    "ragged_3xNone_mixed_empty": ragged_3xNone_mixed_empty,
    "ragged_5x1": ragged_5x1,
    "ragged_2xNonex1xNone": ragged_2xNonex1xNone,
}


chunkable_size = 30
chunkable_arrays = [
    ragged.array(
        [
            rng.random(size=chunkable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(20)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(5)],
            rng.random(size=chunkable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(10)],
            rng.random(size=0),  # test empty arrays in between
            *[rng.random(size=rng.integers(0, 10)) for _ in range(5)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(10)],
            rng.random(size=chunkable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(10)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(15)],
            rng.random(size=chunkable_size),
            *[rng.random(size=rng.integers(0, 10)) for _ in range(5)],
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [
            *[rng.random(size=rng.integers(0, 10)) for _ in range(20)],
            rng.random(size=chunkable_size),
        ],
        dtype=np.float32,
    ),
    ragged.array(
        [rng.random(size=rng.integers(0, chunkable_size)).tolist() for _ in range(20)],
        dtype=np.float32,
    ),
]


@pytest.fixture(params=["duckdb_uri", "postgres_uri"], scope="function")
def sql_storage(request):
    storage = cast(SQLStorage, parse_storage(request.getfixturevalue(request.param)))
    register_storage(storage)

    yield cast(SQLStorage, storage)

    storage.dispose()


@pytest.fixture(scope="module")
def client(tmpdir_module):
    """Module-scoped client with all test arrays pre-written under their own keys."""
    catalog = in_memory(
        writable_storage=[
            str(tmpdir_module),
            f"duckdb:///{tmpdir_module / 'data.duckdb'}",
        ]
    )
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)

        # Write all simple test arrays
        simple = client.create_container("simple")
        for name, array in arrays.items():
            simple.write_ragged(array, key=name)

        # Write partitionable (chunked) arrays
        chunked = client.create_container("chunked")
        max_partition_bytes = (chunkable_size * np.float32(0).nbytes) + 2 * np.int64(
            0
        ).nbytes
        for i, array in enumerate(chunkable_arrays):
            chunked.write_ragged(
                array, key=f"partitionable_{i}", max_partition_bytes=max_partition_bytes
            )

        yield client


@pytest.mark.parametrize("name", arrays.keys())
def test_awkward_form_from_structure(name):
    array = make_ragged_array(arrays[name])
    structure = RaggedStructure.from_array(array)
    form, length, buffers = ak.to_buffers(array._impl)

    assert structure.awk_form == form
    assert structure.awk_length == length
    assert set(form.expected_from_buffers().keys()) == set(buffers.keys())

    for key, val in form.expected_from_buffers().items():
        assert buffers[key].dtype == val


@pytest.mark.parametrize("name", arrays.keys())
def test_adapter_read_write_patch(name, sql_storage):
    array = make_ragged_array(arrays[name])
    structure = RaggedStructure.from_array(array)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-ragged+sql",
        structure_family=StructureFamily.ragged,
        structure=structure,
        assets=[],
    )
    data_source = RaggedSQLAdapter.init_storage(
        data_source=data_source, storage=sql_storage
    )
    node = SimpleNamespace(metadata_={}, specs=[])
    adp = RaggedSQLAdapter.from_catalog(data_source, node)

    # Write the array, read it back, and check for equality
    adp.write(array)
    assert ak.array_equal(array._impl, adp.read()._impl)

    # Append along the same data along the first dimension, read back
    adp.patch(array, offset=(array.shape[0],), extend=True)
    expected = ragged.concat([array, array], axis=0)
    assert ak.array_equal(expected._impl, adp.read()._impl)

    # Append again now that the first dimension is different, read back
    adp.patch(array, offset=(2 * array.shape[0],), extend=True)
    expected = ragged.concat([array, array, array], axis=0)
    assert ak.array_equal(expected._impl, adp.read()._impl)


@pytest.mark.parametrize("name", list(arrays.keys())[:1])
def test_concurrent_patch_raises_conflicts(name, sql_storage, request):
    """Simulate two concurrent producers writing to the same ragged dataset.

    Both adapters load the same (stale) structure from the catalog, both compute
    the same chunk_index, and both attempt to INSERT. The second INSERT must fail
    with a 409 Conflicts error rather than a generic IntegrityError, since
    ragged arrays are designed for a single producer per dataset.

    DuckDB's ADBC driver does not enforce unique constraints during adbc_ingest,
    so the collision is silently accepted; xfail to document this.
    """
    # The sql_storage fixture is parametrized; detect duckdb from storage URI
    if "duckdb" in sql_storage.uri:
        pytest.xfail(
            "DuckDB ADBC adbc_ingest does not enforce unique index constraints"
        )

    array = make_ragged_array(arrays[name])
    structure = RaggedStructure.from_array(array)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-ragged+sql",
        structure_family=StructureFamily.ragged,
        structure=structure,
        assets=[],
    )
    data_source = RaggedSQLAdapter.init_storage(
        data_source=data_source, storage=sql_storage
    )
    node = SimpleNamespace(metadata_={}, specs=[])

    # First producer writes the initial chunk (chunk_index=0).
    adp1 = RaggedSQLAdapter.from_catalog(data_source, node)
    adp1.write(array)

    # Second producer was instantiated from the same data_source before adp1's
    # write was reflected in the catalog. Its in-memory structure shows zero
    # chunks, so its patch will also compute chunk_index=0 and collide.
    adp2 = RaggedSQLAdapter.from_catalog(data_source, node)
    with pytest.raises(Conflicts):
        adp2.patch(array, offset=(0,), extend=True)


@pytest.mark.parametrize("name", arrays.keys())
def test_serialization_roundtrip(name):
    array = make_ragged_array(arrays[name])
    structure = RaggedStructure.from_array(array)

    # Test JSON serialization.
    json_contents = to_json("application/json", array, metadata={})
    array_from_json = from_json(json_contents, structure=structure)
    assert ak.array_equal(array._impl, array_from_json._impl)

    # Test flattened octet-stream serialization.
    octet_stream_contents = to_zipped_buffers("application/zip", array, metadata={})
    array_from_octet_stream = from_zipped_buffers(octet_stream_contents, None)
    assert ak.array_equal(array._impl, array_from_octet_stream._impl)


@pytest.mark.parametrize("name", arrays.keys())
def test_slicing(client, name):
    array = make_ragged_array(arrays[name])
    rac = client["simple"][name]

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

    # slice at first dimension
    with record_history() as h:
        sliced_result = rac[:1]
    assert ak.array_equal(sliced_result._impl, array[:1]._impl)
    assert len(h.responses) == 1  # sanity check
    sliced_response_size = len(h.responses[0].content)
    assert sliced_response_size < full_response_size

    if len(array.shape) < 2:
        with pytest.raises(ClientError, match="Cannot apply the requested slice"):
            rac[:, 0]
        return

    # index at second dimension
    with record_history() as h:
        # Some arrays have empty ragged axes, so indexing at the second dimension is invalid
        if name in (
            "ragged_3xNonex2xNone",
            "ragged_3xNone_mixed_empty",
            "ragged_3xNone_empty",
        ):
            with pytest.raises(ClientError, match="Cannot apply the requested slice"):
                rac[:, 0]
            return
        sliced_result = rac[:, 0]
    assert ak.array_equal(sliced_result._impl, array[:, 0]._impl)
    assert len(h.responses) == 1  # sanity check
    sliced_response_size = len(h.responses[0].content)
    assert sliced_response_size < full_response_size

    # index at first and second dimension
    with record_history() as h:
        sliced_result = rac[1, 0]
    assert ak.array_equal(sliced_result._impl, array[1, 0]._impl)
    assert len(h.responses) == 1  # sanity check
    sliced_response_size = len(h.responses[0].content)
    assert sliced_response_size < full_response_size


@pytest.mark.parametrize("i", range(len(chunkable_arrays)))
def test_chunking(client, i: int):
    array = ragged.array(chunkable_arrays[i])
    rac = client["chunked"][f"partitionable_{i}"]

    # need to add a little bit to account for Awkward metadata
    assert len(rac.chunks[0]) > 1

    divisions = np.cumsum((0, *rac.chunks[0]))
    starts, stops = divisions[:-1], divisions[1:]
    for j, (start, stop) in enumerate(zip(starts, stops, strict=True)):
        part = rac.read(slice=(builtins.slice(start, stop),))
        assert ak.array_equal(part._impl, array[start:stop]._impl)

        part = rac.read(slice=(builtins.slice(start, stop), builtins.slice(0, 4)))
        assert ak.array_equal(part._impl, array[start:stop, 0:4]._impl)

    full = rac.read()
    assert ak.array_equal(full._impl, array._impl)

    sliced = rac[1:10, 0:5]
    assert ak.array_equal(sliced._impl, array[1:10, 0:5]._impl)


@pytest.mark.parametrize("name", arrays.keys())
def test_export_json(tmpdir, client, name):
    array = ragged.array(arrays[name])
    rac = client["simple"][name]

    filepath = tmpdir / "actual.json"
    rac.export(str(filepath), format="application/json")
    actual = filepath.read_text(encoding="utf-8")
    assert actual == ak.to_json(array._impl)

    rac.export(str(filepath), slice=(1,), format="application/json")
    actual = filepath.read_text(encoding="utf-8")
    if array[1].ndim == 0:
        assert actual == orjson.dumps(array[1]._impl.item()).decode("utf-8")
    else:
        assert actual == ak.to_json(array[1]._impl)


@pytest.mark.parametrize("name", arrays.keys())
def test_export_arrow(tmpdir, client, name):
    array = ragged.array(arrays[name])
    rac = client["simple"][name]

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
def test_export_parquet(tmpdir, client, name):
    array = ragged.array(arrays[name])
    rac = client["simple"][name]

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


# ---------------------------------------------------------------------------
# Section: read_block direct tests (both adapters), incl. sub-slice fusion
# ---------------------------------------------------------------------------

# Choose the same limit as the chunked client fixture so chunkable_arrays
# produce multiple chunks reliably.
_MAX_PARTITION_BYTES = (chunkable_size * np.float32(0).nbytes) + 2 * np.int64(0).nbytes


def _chunkable_with_chunks():
    """Yield (chunk_array, structure_with_chunks) for each chunkable array."""
    for i, raw in enumerate(chunkable_arrays):
        array = make_ragged_array(raw)
        chunks = make_ragged_chunks(array, _MAX_PARTITION_BYTES)
        structure = RaggedStructure.from_array(array, chunks=chunks)
        yield pytest.param(array, structure, id=f"partitionable_{i}")


@pytest.mark.parametrize("array, structure", list(_chunkable_with_chunks()))
def test_read_block_memory(array, structure):
    """RaggedAdapter.read_block: each block matches the equivalent direct slice;
    sub-slicing within a block composes correctly with the block slice."""
    assert len(structure.chunks[0]) > 1, "test requires multi-chunk structure"
    adp = RaggedAdapter(array, structure)
    divisions = [int(x) for x in np.cumsum((0, *structure.chunks[0]))]

    for blk, (start, stop) in enumerate(zip(divisions[:-1], divisions[1:])):
        block = adp.read_block(NDBlock(blk))
        assert ak.array_equal(block._impl, array[start:stop]._impl)

        # Sub-slice fusion: read only the first row of this block.
        if stop - start >= 1:
            block_sub = adp.read_block(
                NDBlock(blk), slice=NDSlice(builtins.slice(0, 1))
            )
            expected_sub = array[start : start + 1]._impl  # noqa: E203
            assert ak.array_equal(block_sub._impl, expected_sub)


@pytest.mark.parametrize("array, structure", list(_chunkable_with_chunks()))
def test_read_block_sql(array, structure, sql_storage):
    """RaggedSQLAdapter.read_block: same semantics as memory adapter, via SQL."""
    assert len(structure.chunks[0]) > 1, "test requires multi-chunk structure"

    # The on-disk structure reflects *currently stored* extent; start with the
    # first chunk only and grow via patch(), mirroring the real append path.
    divisions = [int(x) for x in np.cumsum((0, *structure.chunks[0]))]
    first_chunk = array[divisions[0] : divisions[1]]  # noqa: E203
    initial_structure = RaggedStructure.from_array(first_chunk)

    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-ragged+sql",
        structure_family=StructureFamily.ragged,
        structure=initial_structure,
        assets=[],
    )
    data_source = RaggedSQLAdapter.init_storage(
        data_source=data_source, storage=sql_storage
    )
    node = SimpleNamespace(metadata_={}, specs=[])
    adp = RaggedSQLAdapter.from_catalog(data_source, node)
    adp.write(first_chunk)
    for start, stop in zip(divisions[1:-1], divisions[2:]):
        adp.patch(array[start:stop], offset=(start,), extend=True)

    for blk, (start, stop) in enumerate(zip(divisions[:-1], divisions[1:])):
        block = adp.read_block(NDBlock(blk))
        assert ak.array_equal(block._impl, array[start:stop]._impl)

        if stop - start >= 1:
            block_sub = adp.read_block(
                NDBlock(blk), slice=NDSlice(builtins.slice(0, 1))
            )
            expected_sub = array[start : start + 1]._impl  # noqa: E203
            assert ak.array_equal(block_sub._impl, expected_sub)


# ---------------------------------------------------------------------------
# Section: validation / error-path coverage
# ---------------------------------------------------------------------------

_FLOAT64 = BuiltinDtype.from_numpy_dtype(np.dtype("float64"))


@pytest.mark.parametrize(
    "factory, exc_type, match",
    [
        pytest.param(
            lambda: RaggedStructure(data_type=_FLOAT64, shape=(), size=0, chunks=()),
            ValueError,
            "at least one dimension",
            id="empty_shape",
        ),
        pytest.param(
            lambda: RaggedStructure(
                data_type=_FLOAT64, shape=(None,), size=0, chunks=((1,),)
            ),
            ValueError,
            "first dimension",
            id="first_dim_None",
        ),
        pytest.param(
            lambda: RaggedStructure(
                data_type=_FLOAT64, shape=(3,), size=3, chunks=(None,)
            ),
            ValueError,
            "first chunks dimension",
            id="first_chunks_None",
        ),
        pytest.param(
            lambda: RaggedStructure(
                data_type=_FLOAT64, shape=(3, 2), size=6, chunks=((3,),)
            ),
            ValueError,
            "same number of dimensions",
            id="mismatched_ndim",
        ),
        pytest.param(
            lambda: RaggedStructure(
                data_type=_FLOAT64,
                shape=(3, 4),
                size=12,
                chunks=((3,), (2, 2)),
            ),
            ValueError,
            "Only the first dimension can be partitioned",
            id="multi_dim_chunking",
        ),
    ],
)
def test_ragged_structure_validation(factory, exc_type, match):
    with pytest.raises(exc_type, match=match):
        factory()


def test_buffers_from_data_regular_width_mismatch():
    """_buffers_from_data must reject rows whose length disagrees with RegularForm.size."""
    form = ak.forms.RegularForm(
        content=ak.forms.NumpyForm("int64", form_key="node1"),
        size=3,
        form_key="node0",
    )
    with pytest.raises(ValueError, match="Row width mismatch"):
        _buffers_from_data(form, [[1, 2, 3], [1, 2]])


@pytest.fixture
def _writable_sql_adapter(sql_storage):
    """Initialize a writable SQL ragged adapter with one chunk already written."""
    array = make_ragged_array(arrays["ragged_a"])  # shape = (3, None)
    structure = RaggedStructure.from_array(array)
    data_source = DataSource(
        management=Management.writable,
        mimetype="application/x-ragged+sql",
        structure_family=StructureFamily.ragged,
        structure=structure,
        assets=[],
    )
    data_source = RaggedSQLAdapter.init_storage(
        data_source=data_source, storage=sql_storage
    )
    node = SimpleNamespace(metadata_={}, specs=[])
    adp = RaggedSQLAdapter.from_catalog(data_source, node)
    adp.write(array)
    return adp, array


@pytest.mark.parametrize(
    "action, exc_type, match",
    [
        pytest.param(
            lambda adp, array: adp.patch(array, offset=(3,), extend=False),
            NotImplementedError,
            "Overwriting",
            id="patch_extend_False",
        ),
        pytest.param(
            lambda adp, array: adp.patch(array, offset=(), extend=True),
            ValueError,
            "at least one dimension",
            id="patch_empty_offset",
        ),
        pytest.param(
            lambda adp, array: adp.patch(array, offset=(0,), extend=True),
            NotImplementedError,
            "Only appending",
            id="patch_offset_not_at_end",
        ),
        pytest.param(
            # ragged_c has a different awkward form than ragged_a
            lambda adp, _: adp.patch(
                make_ragged_array(arrays["ragged_c"]),
                offset=(adp.structure().shape[0],),
                extend=True,
            ),
            ValueError,
            "structure",
            id="patch_form_mismatch",
        ),
    ],
)
def test_ragged_sql_adapter_validation(_writable_sql_adapter, action, exc_type, match):
    adp, array = _writable_sql_adapter
    with pytest.raises(exc_type, match=match):
        action(adp, array)


# ---------------------------------------------------------------------------
# Section: end-to-end HTTP PATCH (client -> server -> catalog -> SQL adapter)
# ---------------------------------------------------------------------------


@pytest.fixture
def patch_client(tmp_path):
    """Function-scoped client for HTTP PATCH testing (fresh tree per test)."""
    catalog = in_memory(
        writable_storage=[
            str(tmp_path),
            f"duckdb:///{tmp_path / 'data.duckdb'}",
        ]
    )
    app = build_app(catalog)
    with Context.from_app(app) as context:
        yield from_context(context)


@pytest.mark.parametrize(
    "name",
    ["ragged_a", "ragged_c", "ragged_3xNonex2xNone", "ragged_2xNonex3xNone"],
)
def test_http_patch_extend_updates_structure_and_data(patch_client, name):
    """Round-trip a PATCH through the HTTP layer:
    - client.patch(extend=True) appends a new chunk via the router/catalog path
    - catalog re-computes structure_id and persists updated shape/chunks
    - subsequent read returns the concatenated data
    """
    array = make_ragged_array(arrays[name])
    rac = patch_client.write_ragged(array, key="x")

    initial_shape = rac.shape
    initial_chunks = rac.chunks
    n = array.shape[0]

    # First PATCH: append the same array at the end.
    rac.patch(array, offset=n, extend=True)

    # Catalog-side metadata should reflect the append along the leftmost dim.
    assert rac.shape[0] == initial_shape[0] + n
    assert rac.shape[1:] == initial_shape[1:]
    assert len(rac.chunks[0]) == len(initial_chunks[0]) + 1
    assert rac.chunks[0][-1] == n

    # Data round-trip.
    expected = ragged.concat([array, array], axis=0)
    assert ak.array_equal(rac.read()._impl, expected._impl)

    # Second PATCH: ensure repeated appends accumulate correctly.
    rac.patch(array, offset=2 * n, extend=True)
    expected = ragged.concat([array, array, array], axis=0)
    assert ak.array_equal(rac.read()._impl, expected._impl)
    assert rac.shape[0] == initial_shape[0] + 2 * n
    assert len(rac.chunks[0]) == len(initial_chunks[0]) + 2


@pytest.mark.parametrize(
    "kwargs, exc_type, match",
    [
        pytest.param(
            {"extend": False},
            NotImplementedError,
            "extend=True",
            id="extend_False",
        ),
        pytest.param(
            {"extend": True, "persist": False},
            NotImplementedError,
            "persist=True",
            id="persist_False",
        ),
    ],
)
def test_client_patch_unsupported_kwargs(patch_client, kwargs, exc_type, match):
    """Client-side guards for unsupported patch() combinations."""
    array = make_ragged_array(arrays["ragged_a"])
    rac = patch_client.write_ragged(array, key="x")
    with pytest.raises(exc_type, match=match):
        rac.patch(array, offset=array.shape[0], **kwargs)


def test_client_patch_wrong_offset_raises(patch_client):
    array = make_ragged_array(arrays["ragged_a"])
    rac = patch_client.write_ragged(array, key="x")
    with pytest.raises(NotImplementedError, match="appending to the end"):
        # Offset must equal current shape[0] (append at end); 0 is "overwrite".
        rac.patch(array, offset=0, extend=True)


def test_client_patch_dtype_mismatch_raises(patch_client):
    rac = patch_client.write_ragged(
        ragged.array([rng.random(3), rng.random(5)], dtype=np.float64), key="x"
    )
    # Same shape & chunks, but different dtype.
    different = ragged.array([rng.random(3).astype(np.float32)], dtype=np.float32)
    with pytest.raises(ValueError, match="dtype"):
        rac.patch(different, offset=rac.shape[0], extend=True)


def test_client_write_block_accepts_tuple(patch_client):
    """After #16, write_block accepts a tuple block; the request must reach the server
    in the comma-separated form the router expects."""
    array = make_ragged_array(arrays["ragged_a"])
    # Pre-create a node with the right structure but without writing data.
    structure = RaggedStructure.from_array(array)
    rac = patch_client.new(
        StructureFamily.ragged,
        [
            DataSource(
                structure=structure,
                structure_family=StructureFamily.ragged,
                mimetype="application/x-ragged+sql",
            ),
        ],
        key="x",
    )
    # Tuple-form block (single partitioned dim).
    rac.write_block(array, block=(0,))
    assert ak.array_equal(rac.read()._impl, array._impl)
