import numpy
import pytest

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app


@pytest.fixture
def client():
    metadata = {str(i): {str(j): j for j in range(100)} for i in range(100)}
    tree = MapAdapter(
        {
            # This example needs to (1) compress well and (2) be large enough
            # to be worthwhile to compress.
            "compresses_well": ArrayAdapter.from_array(
                numpy.zeros((1000, 1000)), metadata=metadata
            )
        },
    )
    app = build_app(tree)
    with Context.from_app(app) as context:
        yield from_context(context)


def test_zstd(client):
    pytest.importorskip("zstandard")
    with record_history() as h:
        client["compresses_well"]
    (response,) = h.responses
    (request,) = h.requests
    assert "zstd" in request.headers["Accept-Encoding"]
    assert "zstd" in response.headers["Content-Encoding"]


def test_blosc2(client):
    pytest.importorskip("blosc2")
    ac = client["compresses_well"]
    with record_history() as h:
        ac[:]
    (response,) = h.responses
    (request,) = h.requests
    assert "blosc2" in request.headers["Accept-Encoding"]
    assert "blosc2" in response.headers["Content-Encoding"]


def test_blosc2_decoder_multi_frame():
    """`Blosc2Decoder` must reassemble a body made of several concatenated
    blosc2 frames.

    Streaming responses are compressed one frame per server-side write(), so
    the wire body is `frame0 ++ frame1 ++ ...`. A decoder that calls
    `blosc2.decompress` once decodes only the first frame and truncates the
    payload; the decoder must instead walk every frame.
    """
    pytest.importorskip("blosc2")
    import blosc2

    from tiled.client.decoders import Blosc2Decoder

    parts = [
        b"the quick brown fox " * 4096,
        b"jumps over the lazy dog " * 4096,
        b"pack my box with five dozen liquor jugs " * 4096,
    ]
    expected = b"".join(parts)

    # Feed the frames across arbitrary `decode` calls to mimic streaming.
    decoder = Blosc2Decoder()
    assert decoder.decode(blosc2.compress(parts[0])) == b""
    assert decoder.decode(blosc2.compress(parts[1])) == b""
    assert decoder.decode(blosc2.compress(parts[2])) == b""
    assert decoder.flush() == expected

    # A single-frame body still round-trips.
    single = Blosc2Decoder()
    single.decode(blosc2.compress(expected))
    assert single.flush() == expected
