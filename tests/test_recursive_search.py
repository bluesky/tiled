"""
Tests for recursive search: `Container.search_recursive(...)` (Python) and
`GET /api/v1/search-recursive/{path}` (HTTP).

These tests are written test-first, per issue #1368:
https://github.com/bluesky/tiled/issues/1368#issuecomment-5284649768

They are expected to FAIL until the feature is implemented.
"""

import subprocess
import sys

import numpy
import pytest
import pytest_asyncio

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import from_uri, in_memory
from tiled.client import Context, from_context, record_history
from tiled.queries import Key
from tiled.server.app import build_app

from .conftest import TILED_TEST_POSTGRESQL_URI
from .utils import temp_postgres

# Build a nested tree, three levels deep, with metadata for searching.
#
# root/
#   top_level_match          (sample_id="abc123", depth=1)
#   nested/
#     no_match                (depth=2)
#     images/
#       sample_042            (sample_id="abc123", depth=3)
#       sample_043            (sample_id="other", depth=3)
#   other_branch/
#     sample_099              (sample_id="abc123", depth=2)


def _nested_map_tree():
    images = MapAdapter(
        {
            "sample_042": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123"}
            ),
            "sample_043": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "other"}
            ),
        }
    )
    nested = MapAdapter(
        {
            "no_match": ArrayAdapter.from_array(numpy.ones(3), metadata={}),
            "images": images,
        }
    )
    other_branch = MapAdapter(
        {
            "sample_099": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123"}
            ),
        }
    )
    return MapAdapter(
        {
            "top_level_match": ArrayAdapter.from_array(
                numpy.ones(3), metadata={"sample_id": "abc123"}
            ),
            "nested": nested,
            "other_branch": other_branch,
        }
    )


def _populate_catalog_tree(client):
    client.write_array(
        numpy.ones(3), key="top_level_match", metadata={"sample_id": "abc123"}
    )
    nested = client.create_container("nested")
    nested.write_array(numpy.ones(3), key="no_match", metadata={})
    images = nested.create_container("images")
    images.write_array(
        numpy.ones(3), key="sample_042", metadata={"sample_id": "abc123"}
    )
    images.write_array(numpy.ones(3), key="sample_043", metadata={"sample_id": "other"})
    other_branch = client.create_container("other_branch")
    other_branch.write_array(
        numpy.ones(3), key="sample_099", metadata={"sample_id": "abc123"}
    )


@pytest_asyncio.fixture(
    loop_scope="module", scope="module", params=["map", "sqlite", "postgresql"]
)
async def client(request, tmpdir_module):
    if request.param == "map":
        tree = _nested_map_tree()
        app = build_app(tree)
        with Context.from_app(app) as context:
            yield from_context(context)
    elif request.param == "sqlite":
        tree = in_memory(writable_storage=str(tmpdir_module / "sqlite"))
        app = build_app(tree)
        with Context.from_app(app) as context:
            client = from_context(context)
            _populate_catalog_tree(client)
            yield client
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri_with_database_name:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tiled",
                    "catalog",
                    "init",
                    uri_with_database_name,
                ],
                check=True,
                capture_output=True,
            )
            tree = from_uri(
                uri_with_database_name,
                writable_storage=str(tmpdir_module / "postgresql"),
            )
            app = build_app(tree)
            with Context.from_app(app) as context:
                client = from_context(context)
                _populate_catalog_tree(client)
                yield client
    else:
        assert False


def test_search_recursive_finds_matches_at_all_depths(client):
    "Matches at depth 1, 2, and 3 are all found from the root."
    results = client.search_recursive(Key("sample_id") == "abc123")
    assert len(results) == 3
    found_keys = {path[-1] for path in results.keys()}
    assert found_keys == {"top_level_match", "sample_042", "sample_099"}


def test_search_recursive_no_matches(client):
    "An unmatched query returns an empty Mapping, not an error."
    results = client.search_recursive(Key("sample_id") == "does-not-exist")
    assert len(results) == 0
    assert list(results) == []


def test_search_recursive_mapping_protocol(client):
    "RecursiveSearchResults behaves like a Mapping: len, iteration, .items(), .values()."
    results = client.search_recursive(Key("sample_id") == "abc123")
    assert len(results) == len(list(results))
    for path, node in results.items():
        assert isinstance(path, tuple)
        assert node.metadata["sample_id"] == "abc123"
    for node in results.values():
        assert node.metadata["sample_id"] == "abc123"


def test_search_recursive_getitem_tuple_key(client):
    "A path tuple relative to the search root can be used to look up a result."
    results = client.search_recursive(Key("sample_id") == "abc123")
    node = results[("nested", "images", "sample_042")]
    assert node.metadata["sample_id"] == "abc123"


def test_search_recursive_getitem_string_key(client):
    "A slash-delimited string path is equivalent to a tuple path."
    results = client.search_recursive(Key("sample_id") == "abc123")
    assert results["nested/images/sample_042"].metadata == (
        results[("nested", "images", "sample_042")].metadata
    )


def test_search_recursive_getitem_key_error(client):
    "Looking up a path that did not match the query raises KeyError."
    results = client.search_recursive(Key("sample_id") == "abc123")
    with pytest.raises(KeyError):
        results[("nested", "images", "sample_043")]  # exists, but does not match
    with pytest.raises(KeyError):
        results[("does", "not", "exist")]


def test_search_recursive_ancestors_relative_to_root(client):
    "Searching from a subcontainer yields paths relative to that subcontainer, not the true root."
    nested = client["nested"]
    results = nested.search_recursive(Key("sample_id") == "abc123")
    assert set(results.keys()) == {("images", "sample_042")}


def test_search_recursive_max_depth(client):
    "An optional max_depth bounds how far down the tree the search descends."
    # depth=1 is direct children only -- same as .search() from the root.
    results = client.search_recursive(Key("sample_id") == "abc123", max_depth=1)
    assert set(results.keys()) == {("top_level_match",)}


def test_search_recursive_laziness(client):
    "Slicing .values() sends a single request with an explicit page[limit]."
    results = client.search_recursive(Key("sample_id") == "abc123")
    with record_history() as history:
        values = results.values()[:2]
    assert len(values) == 2
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "2"


@pytest.mark.asyncio(loop_scope="module")
async def test_search_recursive_http_response_shape(client):
    "The raw HTTP response mirrors /search/{path} conventions."
    link = client.item["links"]["search_recursive"]
    response = client.context.http_client.get(
        link,
        params={
            "filter[eq][condition][key]": "sample_id",
            "filter[eq][condition][value]": '"abc123"',
        },
    )
    content = response.json()
    assert response.status_code == 200
    assert content["meta"]["count"] == 3
    assert isinstance(content["data"], list)
    for item in content["data"]:
        assert "ancestors" in item["attributes"]
    assert "links" in content
    assert "next" in content["links"]
