# Pickling is only supported when the Context is connected to a remote server
# via actual HTTP/TCP, not to an internal app via ASGI.
# We try connecting out to the demo deployment.
import pickle

import httpx
import pytest
from packaging.version import parse

from ..client import from_context
from ..client.context import Context

MIN_VERSION = "0.1.0a104"
API_URL = "https://tiled-demo.blueskyproject.io/api/v1/"


def test_pickle_context():
    try:
        httpx.get(API_URL).raise_for_status()
    except Exception:
        raise pytest.skip(f"Could not connect to {API_URL}")
    with Context(API_URL) as context:
        pickle.loads(pickle.dumps(context))


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_pickle_clients(structure_clients):
    try:
        httpx.get(API_URL).raise_for_status()
    except Exception:
        raise pytest.skip(f"Could not connect to {API_URL}")
    with Context(API_URL) as context:
        if parse(context.server_info["library_version"]) < parse(MIN_VERSION):
            raise pytest.skip(
                f"Server at {API_URL} is running too old a version to test against."
            )
        client = from_context(context, structure_clients)
        pickle.loads(pickle.dumps(client))
        for segements in [
            ["generated"],
            ["generated", "small_image"],
            ["generated", "short_table"],
        ]:
            original = client
            for segment in segements:
                original = original[segment]
            roundtripped = pickle.loads(pickle.dumps(original))
            assert roundtripped.uri == original.uri
