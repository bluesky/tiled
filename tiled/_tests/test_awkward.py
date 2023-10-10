import io

import awkward
import pyarrow.feather
import pyarrow.parquet

from ..catalog import in_memory
from ..client import Context, from_context, record_history
from ..server.app import build_app
from ..utils import APACHE_ARROW_FILE_MIME_TYPE


def test_slicing(tmpdir):
    catalog = in_memory(writable_storage=tmpdir)
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)

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


def test_export_json(tmpdir):
    catalog = in_memory(writable_storage=tmpdir)
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)

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


def test_export_arrow(tmpdir):
    catalog = in_memory(writable_storage=tmpdir)
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)

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


def test_export_parquet(tmpdir):
    catalog = in_memory(writable_storage=tmpdir)
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)

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


def test_long_url(tmpdir):
    # https://github.com/bluesky/tiled/pull/577
    catalog = in_memory(writable_storage=tmpdir)
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        array = awkward.Array([{f"key{i:05}": i for i in range(4000)}])
        aac = client.write_awkward(array, key="test")
        aac[0]
