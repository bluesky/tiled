import httpx

from ..adapters.mapping import MapAdapter
from ..client import Context
from ..server.app import build_app

tree = MapAdapter({})


def test_configurable_timeout():
    with Context.from_app(build_app(tree), timeout=httpx.Timeout(17)) as context:
        assert context.http_client.timeout.connect == 17
        assert context.http_client.timeout.read == 17
