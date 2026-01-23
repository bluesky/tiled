# This tests an experimental feature likely to be deprecated.
# https://github.com/bluesky/tiled/issues/217
import pathlib

import pytest

from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.server.app import build_app


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempdirFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("temp")


@pytest.fixture(scope="module")
def client(module_tmp_path):
    catalog = in_memory(writable_storage=str(module_tmp_path))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        client.write_array([1], key="x", metadata={"sample": {"color": "red"}})
        client.write_array([2], key="y", metadata={"sample": {"color": "blue"}})
        yield client


def test_select_metadata(client):
    http_client = client.context.http_client
    # /metadata
    response = http_client.get("/api/v1/metadata/x?select_metadata=[sample.color]")
    result = response.json()
    assert result["data"]["attributes"]["metadata"] == {"selected": ["red"]}
    # /search
    response = http_client.get("/api/v1/search/?select_metadata=[sample.color]")
    result = response.json()
    for item, color in zip(result["data"], ["red", "blue"]):
        assert item["attributes"]["metadata"] == {"selected": [color]}
