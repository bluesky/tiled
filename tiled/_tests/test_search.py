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
        "nested_with_md": Tree(
            {
                "i": ArrayAdapter.from_array(
                    numpy.arange(10), metadata={"apple": "red", "animal": "dog"}
                ),
                "j": ArrayAdapter.from_array(
                    numpy.arange(10), metadata={"banana": "yellow", "animal": "dog"}
                ),
                "k": ArrayAdapter.from_array(
                    numpy.arange(10), metadata={"cantalope": "orange", "animal": "cat"}
                ),
            },
            metadata={"animal": "dog"},
        ),
        "nested_without_md": Tree(
            {
                "x": ArrayAdapter.from_array(
                    numpy.arange(10), metadata={"apple": "red", "animal": "dog"}
                ),
                "y": ArrayAdapter.from_array(
                    numpy.arange(10), metadata={"banana": "yellow", "animal": "dog"}
                ),
                "z": ArrayAdapter.from_array(
                    numpy.arange(10), metadata={"cantalope": "orange", "animal": "cat"}
                ),
            },
            metadata={},
        ),
    }
)


@pytest.mark.parametrize(
    "term, expected_keys",
    [
        ("red", ["a"]),
        ("yellow", ["b"]),
        ("orange", ["c"]),
        ("dog", ["a", "b", "nested_with_md"]),
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


def test_nested_key_into_results():
    client = from_tree(tree)
    results = client.search(FullText("dog"))
    assert "nested_with_md" in results
    assert "nested_without_md" not in results

    assert "i" in results["nested_with_md"]
    results["nested_with_md"]["i"]
    results["nested_with_md", "i"]
    with pytest.raises(KeyError):
        results["nested_without_md"]
    with pytest.raises(KeyError):
        results["nested_without_md", "x"]
