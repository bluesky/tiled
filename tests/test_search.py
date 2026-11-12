import json

import h5py
import numpy
import pytest

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.client.utils import handle_error
from tiled.queries import FullText, Key
from tiled.server.app import build_app

tree = MapAdapter(
    {
        "a": ArrayAdapter.from_array(
            numpy.arange(10), metadata={"apple": "red", "animal": "dog"}
        ),
        "b": ArrayAdapter.from_array(
            numpy.arange(10), metadata={"banana": "yellow", "animal": "dog"}
        ),
        "c": ArrayAdapter.from_array(
            numpy.arange(10), metadata={"cantalope": "orange", "animal": "cat"}
        ),
    }
)


@pytest.fixture(scope="module")
def map_client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@pytest.fixture
def catalog_client(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        # Upload arrays and metadata from tree into client so that it has matching content.
        for key, value in tree.items():
            client.write_array(value.read(), metadata=value.metadata(), key=key)
        yield client


@pytest.fixture(
    params=("map_client", "catalog_client"),
)
def client(request: pytest.FixtureRequest):
    yield request.getfixturevalue(request.param)


@pytest.mark.parametrize(
    "key, value, expected_keys",
    [
        ("apple", "red", ["a"]),
        ("banana", "yellow", ["b"]),
        ("cantalope", "orange", ["c"]),
        ("animal", "dog", ["a", "b"]),
        ("animal", "cat", ["c"]),
    ],
)
def test_search(client, key, value, expected_keys):
    query = Key(key)
    results = client.search(query == value)
    assert list(results) == expected_keys


def test_compound_search(client):
    results = client.search(Key("animal") == "dog").search(Key("banana") == "yellow")
    assert list(results) == ["b"]


def test_indexing_over_search(client):
    results = client.search(Key("animal") == "dog")
    assert dict(results["a"].metadata) == tree["a"].metadata()


def test_key_into_results(client):
    results = client.search(Key("animal") == "dog")
    assert "apple" in results["a"].metadata
    assert "banana" in results["b"].metadata
    assert "c" not in results  # This *is* in the tree but not among the results.


def test_compound_key_into_results():
    nested_tree = MapAdapter(
        {
            "i": MapAdapter({"X": tree}, metadata={"temp": "hot"}),
            "j": MapAdapter({}, metadata={"temp": "cold"}),
        }
    )
    app = build_app(nested_tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        result = client.search(FullText("hot"))["i", "X", "a"]
        assert "apple" in result.metadata


@pytest.mark.parametrize("format", ["application/json", "application/x-hdf5"])
def test_export_search_results(client, buffer, format):
    "An export of search results contains only the matching entries."
    results = client.search(Key("animal") == "dog")
    assert list(results) == ["a", "b"]
    results.export(buffer, format=format)
    assert set(_exported_keys(buffer, format)) == {"a", "b"}


@pytest.mark.parametrize("format", ["application/json", "application/x-hdf5"])
def test_export_search_results_no_matches(client, buffer, format):
    "An export of a search with no matches is empty (but not an error)."
    results = client.search(Key("animal") == "does-not-exist")
    assert list(results) == []
    results.export(buffer, format=format)
    assert _exported_keys(buffer, format) == []


@pytest.mark.parametrize("format", ["application/json", "application/x-hdf5"])
def test_export_search_results_with_fields(client, buffer, format):
    "The `fields` argument and search queries compose."
    results = client.search(Key("animal") == "dog")
    results.export(buffer, fields=["a"], format=format)
    assert _exported_keys(buffer, format) == ["a"]


def test_export_search_results_post_route(client, buffer):
    "The POST sibling of /container/full honors filters too."
    params = {
        "format": "application/json",
        "filter[eq][condition][key]": "animal",
        "filter[eq][condition][value]": json.dumps("dog"),
    }
    response = handle_error(
        client.context.http_client.post(client.item["links"]["full"], params=params)
    )
    assert set(json.loads(response.read())["contents"]) == {"a", "b"}


def _exported_keys(buffer, format):
    "Read back the top-level keys of an exported container file or buffer."
    if format == "application/json":
        return sorted(json.loads(buffer.getvalue())["contents"])
    if format == "application/x-hdf5":
        with h5py.File(buffer) as file:
            return sorted(file.keys())
    raise ValueError(format)
