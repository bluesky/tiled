import io

import awkward

from ..catalog import in_memory
from ..client import Context, from_context, record_history
from ..server.app import build_app


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
        aac = client.write_awkward(array, key="test")

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


def test_export(tmpdir):
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
