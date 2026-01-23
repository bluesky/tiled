import awkward as ak
import numpy as np
import pyarrow.feather
import pyarrow.parquet
import pytest
import ragged

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
from tiled.structures.ragged import RaggedStructure
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
def test_structure(name):
    array = arrays[name]
    expected_form, expected_len, expected_nodes = ak.to_buffers(
        array._impl  # noqa: SLF001
    )

    structure = RaggedStructure.from_array(array)
    form = ak.forms.from_dict(structure.form)

    assert expected_form == form
    assert expected_len == structure.shape[0]
    assert len(expected_nodes) == len(structure.offsets) + 1


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


@pytest.mark.parametrize("name", arrays.keys())
def test_export_arrow(tmpdir, client, name):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = arrays[name]
    rac = client.write_ragged(array, key="test")

    filepath = tmpdir / "actual.arrow"
    rac.export(str(filepath), format=APACHE_ARROW_FILE_MIME_TYPE)
    actual = pyarrow.feather.read_table(filepath)
    expected = ak.to_arrow_table(array._impl)  # noqa: SLF001
    assert actual == expected


@pytest.mark.parametrize("name", arrays.keys())
def test_export_parquet(tmpdir, client, name):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = arrays[name]
    rac = client.write_ragged(array, key="test")

    filepath = tmpdir / "actual.parquet"
    rac.export(str(filepath), format="application/x-parquet")
    actual = pyarrow.parquet.read_table(filepath)
    expected = ak.to_arrow_table(array._impl)  # noqa: SLF001
    assert actual == expected
