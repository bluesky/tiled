import numpy
import pytest

from ..readers.array import ArrayAdapter
from ..client import from_tree
from ..trees.in_memory import Tree
from ..queries import FullText


tree = Tree(
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
    },
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
