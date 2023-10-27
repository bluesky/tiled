import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..catalog import in_memory
from ..client import Context, from_context
from ..queries import FullText, Key
from ..server.app import build_app

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
