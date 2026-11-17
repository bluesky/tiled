"""
Node keys become URL path segments and, for writable data, directory names.
"""

import sqlite3
from pathlib import Path

import awkward
import pandas
import pytest
from starlette.status import HTTP_200_OK, HTTP_422_UNPROCESSABLE_CONTENT

from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.utils import UnsafePath


@pytest.fixture
def layout(tmp_path):
    "A catalog in a file, with writable storage in a directory next to it."
    database = tmp_path / "catalog.db"
    storage = tmp_path / "storage"
    storage.mkdir()
    catalog = from_uri(
        f"sqlite:///{database}",
        writable_storage=str(storage),
        init_if_not_exists=True,
    )
    return catalog, database, storage


@pytest.fixture
def client(layout):
    catalog, _database, _storage = layout
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context)


def files_and_directories(directory):
    return sorted(str(path.relative_to(directory)) for path in directory.rglob("*"))


def creation_request(client, key, source_key):
    "A request to create a node like `source_key`, with new storage, named `key`."
    item = (
        client.context.http_client.get(
            f"/api/v1/metadata/{source_key}", params={"include_data_sources": "true"}
        )
        .raise_for_status()
        .json()["data"]["attributes"]
    )
    (data_source,) = item["data_sources"]
    return {
        "id": key,
        "structure_family": item["structure_family"],
        "metadata": {},
        "specs": [],
        "data_sources": [
            {
                "structure_family": data_source["structure_family"],
                "structure": data_source["structure"],
                "mimetype": data_source["mimetype"],
                "management": "writable",
                "assets": [],
                "parameters": {},
            }
        ],
    }


@pytest.mark.parametrize("key", ["..", ".", "a/b", "/a", "a/", "../a", "a\x00b"])
def test_unsafe_key_is_refused(client, key):
    "A key that is not a single path segment is refused."
    body = {"id": key, "structure_family": "container", "metadata": {}, "specs": []}

    response = client.context.http_client.post("/api/v1/metadata/", json=body)

    assert response.status_code == HTTP_422_UNPROCESSABLE_CONTENT
    assert list(client) == []


@pytest.mark.parametrize(
    "key", ["a", "a.b", "..a", "a..", "a b", "run#1", "é", "a%2Fb", "a\\b"]
)
def test_ordinary_key_is_accepted(client, key):
    body = {"id": key, "structure_family": "container", "metadata": {}, "specs": []}

    response = client.context.http_client.post("/api/v1/metadata/", json=body)

    assert response.status_code == HTTP_200_OK
    assert list(client) == [key]


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["..", ".", "a/b"])
async def test_unsafe_key_is_refused_by_catalog(layout, key):
    "The catalog refuses the key itself, not only the HTTP route in front of it."
    catalog, _database, _storage = layout
    with pytest.raises(UnsafePath):
        await catalog.create_node(structure_family="container", metadata={}, key=key)


@pytest.mark.parametrize("kind", ["table", "awkward"])
def test_node_under_container_with_unsafe_key(layout, client, kind):
    """Storage is not created outside of the writable storage directory.

    A catalog may hold a container named "..".
    """
    _catalog, database, storage = layout
    client.write_table(pandas.DataFrame({"x": [1, 2, 3]}), key="table")
    client.write_awkward(awkward.Array([[1, 2], [3]]), key="awkward")
    client.create_container("legacy")
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE nodes SET key = '..' WHERE key = 'legacy'")
    outside_before = files_and_directories(storage.parent)

    response = client.context.http_client.post(
        "/api/v1/metadata/%2E%2E", json=creation_request(client, "escaped", kind)
    )

    assert response.status_code == HTTP_422_UNPROCESSABLE_CONTENT
    assert not Path(storage.parent, "escaped").exists()
    assert files_and_directories(storage.parent) == outside_before
