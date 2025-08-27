import awkward as ak
import numpy as np
import pyarrow.feather
import pyarrow.parquet
import pytest
import ragged

from tiled.catalog import in_memory
from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app
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
    "regular_1d": ragged.array(RNG.random(10)),
    "regular_nd": ragged.array(RNG.random((2, 3, 4))),
    "ragged_simple": ragged.array(
        [
            RNG.random(3).tolist(),
            RNG.random(5).tolist(),
            RNG.random(8).tolist(),
        ]
    ),
    "ragged_complex": ragged.array(
        [
            [RNG.random(10).tolist()],
            [RNG.random(8).tolist(), []],
            [RNG.random(5).tolist(), RNG.random(2).tolist()],
            [[], RNG.random(7).tolist()],
        ]
    ),
    "ragged_complex_nd": ragged.array(
        [
            [RNG.random((4, 3)).tolist()],
            [RNG.random((2, 8)).tolist(), []],
            [RNG.random((5, 2)).tolist(), RNG.random((3, 3)).tolist()],
            [[], RNG.random((7, 1)).tolist()],
        ]
    ),
}


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
        with record_history() as h:
            sliced_result = rac[1]
        assert ak.array_equal(sliced_result._impl, array[1]._impl)  # noqa: SLF001
        assert len(h.responses) == 1  # sanity check
        sliced_response_size = len(h.responses[0].content)
        assert sliced_response_size < full_response_size


@pytest.mark.parametrize("name", arrays.keys())
def test_export_json(client, buffer, name):
    array = arrays[name]
    rac = client.write_ragged(array, key="test")

    file = buffer
    rac.export(file, format="application/json")
    actual = bytes(file.getbuffer()).decode()
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
