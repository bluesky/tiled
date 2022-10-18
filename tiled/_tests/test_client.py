import httpx
import pytest

from ..adapters.mapping import MapAdapter
from ..client import from_tree

tree = MapAdapter({})


def test_configurable_timeout():
    c = from_tree(tree)
    assert c.context.http_client.timeout.connect != 17
    c = from_tree(tree, timeout=httpx.Timeout(17))
    assert c.context.http_client.timeout.connect == 17


def test_prefix():
    c = from_tree(tree, server_settings={"prefix": "/a/b/c"})
    list(c)

    # Not allowed to use '/node/metadata' in the prefix.
    with pytest.raises(Exception):
        from_tree(tree, server_settings={"prefix": "/a/b/node/metadata/c"})
