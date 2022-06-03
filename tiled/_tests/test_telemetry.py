from ..adapters.mapping import MapAdapter
from ..client import from_tree, telemetry

tree = MapAdapter({})


def test_telemetry():
    "Very basic exercise of telemetry"
    client = from_tree(tree)

    with telemetry() as t:
        repr(client)  # trigger a request
    assert t.requests
    assert t.responses
