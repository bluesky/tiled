from ..adapters.mapping import MapAdapter
from ..client import from_tree, record_history

tree = MapAdapter({})


def test_history():
    "Very basic exercise of history"
    client = from_tree(tree)

    with record_history() as history:
        repr(client)  # trigger a request
    assert history.requests
    assert history.responses
