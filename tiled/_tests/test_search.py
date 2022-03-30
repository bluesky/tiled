import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree
from ..queries import FullText

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
def test_search(term, expected_keys):
    client = from_tree(tree)
    query = FullText(term)
    results = client.search(query)
    assert list(results) == expected_keys


def test_compound_search():
    client = from_tree(tree)
    results = client.search(FullText("dog")).search(FullText("yellow"))
    assert list(results) == ["b"]


def test_key_into_results():
    client = from_tree(tree)
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
    client = from_tree(nested_tree)
    result = client.search(FullText("hot"))["i", "X", "a"]
    assert "apple" in result.metadata
