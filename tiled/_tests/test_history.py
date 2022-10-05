from ..adapters.mapping import MapAdapter
from ..client import from_tree, record_history

tree = MapAdapter({})
client = from_tree(tree)


def test_history():
    "Very basic exercise of history"
    with record_history() as history:
        repr(client)  # trigger a request
    assert history.requests
    assert history.responses
