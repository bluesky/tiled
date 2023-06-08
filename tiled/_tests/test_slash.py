import numpy

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..server.app import build_app

arr = numpy.ones(3)


def test_slash_in_key():
    tree = MapAdapter(
        {
            "x": MapAdapter({"y": ArrayAdapter(1 * arr)}),
            "x/y": ArrayAdapter(2 * arr),
        }
    )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client["x", "y"].read()[0] == 1
    assert client["x/y"].read()[0] == 2
