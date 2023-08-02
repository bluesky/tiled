import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, record_history
from ..server.app import build_app


@pytest.fixture
def client():
    metadata = {str(i): {str(j): j for j in range(100)} for i in range(100)}
    tree = MapAdapter(
        {
            "compresses_well": ArrayAdapter.from_array(
                numpy.zeros((100, 100)), metadata=metadata
            )
        },
    )
    app = build_app(tree)
    with Context.from_app(app) as context:
        yield from_context(context)


def test_zstd(client):
    with record_history() as h:
        client["compresses_well"]
    (response,) = h.responses
    (request,) = h.requests
    assert "zstd" in request.headers["Accept-Encoding"]
    assert "zstd" in response.headers["Content-Encoding"]


def test_blosc(client):
    ac = client["compresses_well"]
    with record_history() as h:
        ac[:]
    (response,) = h.responses
    (request,) = h.requests
    assert "blosc" in request.headers["Accept-Encoding"]
    assert "blosc" in response.headers["Content-Encoding"]
