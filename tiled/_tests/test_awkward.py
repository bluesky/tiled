import io

import awkward
import pyarrow.feather
import pyarrow.parquet
import pytest

from functools import partial
from unittest.mock import Mock

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


@pytest.fixture
def spy():
    def mock_request(method, *args, **kwargs):
        return method(*args, **kwargs)

    spy = Mock(side_effect=mock_request)
    yield spy


@pytest.fixture
def spy_client(client, spy):
    original_http_client = client.context.http_client

    spy_client = Mock(create=True)
    # spy_client.__getattr__ = lambda attr_name: getattr(client, attr_name)

    spy_client.get = Mock(side_effect=partial(spy, original_http_client.get))
    spy_client.put = Mock(side_effect=partial(spy, original_http_client.put))
    spy_client.post = Mock(side_effect=partial(spy, original_http_client.post))

    client.context.http_client = spy_client
    yield client

    client.context.http_client = original_http_client


@pytest.mark.parametrize(
    "browser, url_length_limit", (
        # ("Chrome", 2_083),
        # ("Firefox", 65_536),
        ("Safari", 80_000),
        # ("Internet Explorer", 2_083),
    )
)
def test_long_url(spy_client, spy, url_length_limit, browser, num_keys=7_000):
    # https://github.com/bluesky/tiled/pull/577
    array = awkward.Array([{f"key{i:05}": i for i in range(num_keys)}])
    aac = spy_client.write_awkward(array, key="test")
    aac[0]

    spy.assert_called()
    spy_kwargs = spy.call_args.kwargs
    form_keys = spy_kwargs.get("json") or spy_kwargs.get("params")
    assert form_keys is not None
    form_key_length = len(str(form_keys))
    assert form_key_length > url_length_limit
