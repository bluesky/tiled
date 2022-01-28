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


@pytest.mark.parametrize(
    "key, sorted_list",
    [
        ("letter", sorted_letters),
        ("number", sorted_numbers),
    ],
)
def test_sort(key, sorted_list):
    client = from_tree(tree)
    unsorted = [node.metadata[key] for node in client.values()]
    assert unsorted != sorted_list
    sorted_ascending = [node.metadata[key] for node in client.sort((key, 1)).values()]
    assert sorted_ascending == sorted_list
    sorted_descending = [node.metadata[key] for node in client.sort((key, -1)).values()]
    assert sorted_descending == list(reversed(sorted_list))


def test_sort_two_columns():
    # Sort by (repeated) letter, then by number.
    client = from_tree(tree).sort(("repeated_letter", 1), ("number", 1))
    letters_ = [node.metadata["repeated_letter"] for node in client.values()]
    numbers_ = [node.metadata["number"] for node in client.values()]
    # Letters are sorted.
    assert letters_ == ["a"] * 5 + ["b"] * 5
    # Numbers *within* each block of letters are sorted
    assert sorted(numbers_[:5]) == numbers_[:5]
    assert sorted(numbers_[5:]) == numbers_[5:]
    # but not sorted overall.
    assert not sorted(numbers_) == numbers_
