import io
import json

import awkward
import numpy
import pyarrow.feather
import pyarrow.parquet
import pytest

from ..catalog import in_memory
from ..client import Context, from_context, record_history
from ..server.app import build_app
from ..utils import APACHE_ARROW_FILE_MIME_TYPE


@pytest.fixture
def catalog(tmpdir):
    catalog = in_memory(writable_storage=tmpdir)
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


def test_slicing(client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array)
        assert awkward.almost_equal(aac[0], array[0])
        assert awkward.almost_equal(aac[0, "y"], array[0, "y"])
        assert awkward.almost_equal(aac[0, "y", :1], array[0, "y", :1])

        # When sliced, the serer sends less data.
        with record_history() as h:
            aac[:]
        assert len(h.responses) == 1  # sanity check
        full_response_size = len(h.responses[0].content)
        with record_history() as h:
            aac[0, "y"]
        assert len(h.responses) == 1  # sanity check
        sliced_response_size = len(h.responses[0].content)
        assert sliced_response_size < full_response_size


def test_export_json(client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    aac = client.write_awkward(array, key="test")

    file = io.BytesIO()
    aac.export(file, format="application/json")
    actual = bytes(file.getbuffer()).decode()
    assert actual == awkward.to_json(array)


def test_export_arrow(tmpdir, client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    aac = client.write_awkward(array, key="test")

    filepath = tmpdir / "actual.arrow"
    aac.export(str(filepath), format=APACHE_ARROW_FILE_MIME_TYPE)
    actual = pyarrow.feather.read_table(filepath)
    expected = awkward.to_arrow_table(array)
    assert actual == expected


def test_export_parquet(tmpdir, client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    aac = client.write_awkward(array, key="test")

    filepath = tmpdir / "actual.parquet"
    aac.export(str(filepath), format="application/x-parquet")
    actual = pyarrow.parquet.read_table(filepath)
    expected = awkward.to_arrow_table(array)
    assert actual == expected


def test_large_number_of_form_keys(client):
    "Request should succeed even when too many form_keys to fit in a URL."
    # https://github.com/bluesky/tiled/pull/577

    # The HTTP spec itself has no limit, but tools impose a pragmatic one.
    # Nginx defaults to 8k bytes. Chrome is around 2000.
    # This is a representative value.
    URL_LENGTH_LIMIT = 2000
    NUM_KEYS = 7000  # meant to achieve a large body of form_keys
    array = awkward.Array([{f"key{i:05}": i for i in range(NUM_KEYS)}])
    aac = client.write_awkward(array, key="test")
    with record_history() as h:
        aac[0]
    (request,) = h.requests
    assert request.method == "POST"
    form_keys = json.loads(request.read())
    form_key_length = len(str(form_keys))
    assert form_key_length > URL_LENGTH_LIMIT
    url_length = len(str(request.url))
    assert url_length <= URL_LENGTH_LIMIT


def test_more_slicing_1(client):
    array = awkward.Array(
        [
            {
                "stuff": 123,
                "file": [
                    {"filename": 321, "other": 3.14},
                ],
            },
        ]
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array)
        assert awkward.almost_equal(aac["file", "filename"], array["file", "filename"])


def test_more_slicing_2(client):
    array = awkward.from_numpy(
        numpy.arange(2 * 3 * 5).reshape(2, 3, 5), regulararray=True
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array)
        assert awkward.almost_equal(aac[1:], array[1:])
        assert awkward.almost_equal(aac[1:, 1:], array[1:, 1:])


def test_more_slicing_3(client):
    array = awkward.Array(
        [
            {"good": 123, "bad": 123},
            {"good": 321, "bad": [1, 2, 3]},
        ]
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array[:])
        assert awkward.almost_equal(aac["good"], array["good"])
