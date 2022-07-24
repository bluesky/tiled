import httpx

from ..adapters.mapping import MapAdapter
from ..client import from_tree

tree = MapAdapter({})


def test_configurable_timeout():
    c = from_tree(tree)
    assert c.context._client.timeout.connect != 17
    c = from_tree(tree, timeout=httpx.Timeout(17))
    assert c.context._client.timeout.connect == 17
