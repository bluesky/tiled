import httpx

from ..adapters.mapping import MapAdapter
from ..client import from_tree

tree = MapAdapter({})


def test_configurable_timeout():
    with from_tree(tree) as c:
        assert c.context.http_client.timeout.connect != 17
    with from_tree(tree, timeout=httpx.Timeout(17)) as c:
        assert c.context.http_client.timeout.connect == 17
