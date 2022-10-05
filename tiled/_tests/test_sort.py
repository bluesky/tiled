import random
import string
import uuid

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree

sorted_letters = list(string.ascii_lowercase)[:10]
repeated_letters = ["a", "b"] * 5
sorted_numbers = list(range(10))
letters = sorted_letters.copy()
numbers = sorted_numbers.copy()
seed = random.Random()
seed.shuffle(letters)
seed.shuffle(numbers)

tree = MapAdapter(
    {
        str(uuid.uuid4()): ArrayAdapter.from_array(
            numpy.arange(10),
            metadata={
                "letter": letter,
                "number": number,
                "repeated_letter": repeated_letter,
            },
        )
        for letter, number, repeated_letter in zip(letters, numbers, repeated_letters)
    }
)
client = from_tree(tree)


@pytest.mark.parametrize(
    "key, sorted_list",
    [
        ("letter", sorted_letters),
        ("number", sorted_numbers),
    ],
)
def test_sort(key, sorted_list):
    unsorted = [node.metadata[key] for node in client.values()]
    assert unsorted != sorted_list
    sorted_ascending = [node.metadata[key] for node in client.sort((key, 1)).values()]
    assert sorted_ascending == sorted_list
    sorted_descending = [node.metadata[key] for node in client.sort((key, -1)).values()]
    assert sorted_descending == list(reversed(sorted_list))


def test_sort_two_columns():
    # Sort by (repeated) letter, then by number.
    client_sorted = client.sort(("repeated_letter", 1), ("number", 1))
    letters_ = [node.metadata["repeated_letter"] for node in client_sorted.values()]
    numbers_ = [node.metadata["number"] for node in client_sorted.values()]
    # Letters are sorted.
    assert letters_ == ["a"] * 5 + ["b"] * 5
    # Numbers *within* each block of letters are sorted
    assert sorted(numbers_[:5]) == numbers_[:5]
    assert sorted(numbers_[5:]) == numbers_[5:]
    # but not sorted overall.
    assert not sorted(numbers_) == numbers_


def test_sort_sparse():
    """
    Sort where the key only present on some nodes.
    """
    tree = MapAdapter(
        {
            "yes1": ArrayAdapter.from_array(numpy.arange(10), metadata={"stuff": "a"}),
            "no1": ArrayAdapter.from_array(numpy.arange(10), metadata={}),
            "yes2": ArrayAdapter.from_array(numpy.arange(10), metadata={"stuff": "b"}),
            "no2": ArrayAdapter.from_array(numpy.arange(10), metadata={}),
        }
    )
    client = from_tree(tree)
    client_sorted = client.sort(("stuff", 1))
    assert list(client_sorted)[:2] == ["yes1", "yes2"]


def test_sort_missing():
    """
    Sort where the key not present on any node.
    """
    tree = MapAdapter(
        {
            "no1": ArrayAdapter.from_array(numpy.arange(10), metadata={}),
            "no2": ArrayAdapter.from_array(numpy.arange(10), metadata={}),
        }
    )
    client = from_tree(tree)
    client_sorted = client.sort(("stuff", 1))
    list(client_sorted)  # Just check for no errors.
