import string

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree

keys = list(string.ascii_lowercase)
tree = MapAdapter(
    {
        letter: ArrayAdapter.from_array(number * numpy.ones(10))
        for letter, number in zip(keys, range(26))
    }
)


def test_indexers():
    client = from_tree(tree)
    assert client.keys_indexer[:3] == keys[:3] == list("abc")
    assert client.keys_indexer[:1] == keys[:1] == list("a")
    assert client.keys_indexer[1:3] == keys[1:3] == list("bc")
    assert client.keys_indexer[-3::-1] == keys[-3::-1]  # ["x", "w", "v", ...]
    assert client.keys_indexer[-1:-5:-1] == keys[-1:-5:-1] == list("zyxw")
    assert client.keys_indexer[:] == list(client) == keys
    # Test out of bounds
    with pytest.raises(IndexError):
        client.keys_indexer[len(keys)]
    with pytest.raises(IndexError):
        client.keys_indexer[-len(keys) - 1]
    # These should be in bounds and should not raise.
    client.keys_indexer[len(keys) - 1]
    client.keys_indexer[-len(keys)]
