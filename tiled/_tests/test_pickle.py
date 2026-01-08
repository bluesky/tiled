# Pickling is only supported when the Context is connected to a remote server
# via actual HTTP/TCP, not to an internal app via ASGI.
# We try connecting out to the demo deployment.
import pickle

import httpx
import pytest
import uvicorn

from ..client import from_context
from ..client.cache import Cache
from ..client.context import Context
from ..config import Authentication
from ..server.app import build_app
from ..utils import import_object
from .utils import Server


@pytest.fixture(scope="module")
def server_url():
    EXAMPLE = "tiled.examples.generated:tree"
    tree = import_object(EXAMPLE)
    app = build_app(tree, authentication=Authentication(single_user_api_key="secret"))

    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="info")
    server = Server(config)
    with server.run_in_thread() as url:
        yield url


def test_pickle_context(server_url):
    try:
        httpx.get(server_url).raise_for_status()
    except Exception:
        raise pytest.skip(f"Could not connect to {server_url}")
    with Context.from_any_uri(server_url, api_key="secret")[0] as context:
        pickle.loads(pickle.dumps(context))


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_pickle_clients(server_url, structure_clients, tmpdir):
    try:
        httpx.get(server_url).raise_for_status()
    except Exception:
        raise pytest.skip(f"Could not connect to {server_url}")
    cache = Cache(tmpdir / "http_response_cache.db")
    with Context.from_any_uri(server_url, api_key="secret", cache=cache)[0] as context:
        client = from_context(context, structure_clients)
        pickle.loads(pickle.dumps(client))
        for segments in [
            [],
            ["nested", "images", "small_image"],
            ["tables", "short_table"],
        ]:
            original = client
            for segment in segments:
                original = original[segment]
            roundtripped = pickle.loads(pickle.dumps(original))
            roundtripped_twice = pickle.loads(pickle.dumps(roundtripped))
            assert roundtripped.uri == roundtripped_twice.uri == original.uri


def test_lock_round_trip(tmpdir):
    cache = Cache(tmpdir / "http_response_cache.db")
    cache_round_tripped = pickle.loads(pickle.dumps(cache))
    cache_round_tripped_twice = pickle.loads(pickle.dumps(cache_round_tripped))
    # implementation detail!
    assert (
        cache._lock.lock
        is cache_round_tripped._lock.lock
        is cache_round_tripped_twice._lock.lock
    )
