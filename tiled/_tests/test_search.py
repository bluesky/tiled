import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..queries import FullText
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
def client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@pytest.mark.parametrize(
    "term, expected_keys",
    [
        ("red", ["a"]),
        ("yellow", ["b"]),
        ("orange", ["c"]),
        ("dog", ["a", "b"]),
        ("cat", ["c"]),
    ],
)
def test_search(client, term, expected_keys):
    query = FullText(term)
    results = client.search(query)
    assert list(results) == expected_keys


def test_compound_search(client):
    results = client.search(FullText("dog")).search(FullText("yellow"))
    assert list(results) == ["b"]


def test_key_into_results(client):
    results = client.search(FullText("dog"))
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
