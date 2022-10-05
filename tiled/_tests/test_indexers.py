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
empty_tree = MapAdapter({})
client = from_tree(tree)
empty_client = from_tree(empty_tree)


def test_indexers():
    assert client.keys()[0] == client.keys().first() == keys[0] == "a"
    assert client.keys()[-1] == client.keys().last() == keys[-1] == "z"
    assert client.keys()[:3] == client.keys().head(3) == keys[:3] == list("abc")
    assert client.keys()[:1] == client.keys().head(1) == keys[:1] == list("a")
    assert client.keys()[1:3] == keys[1:3] == list("bc")
    assert client.keys()[-3::-1] == keys[-3::-1]  # ["x", "w", "v", ...]
    assert client.keys()[-1:-5:-1] == keys[-1:-5:-1] == list("zyxw")
    assert client.keys().tail(4) == list(reversed(keys[-1:-5:-1])) == list("wxyz")
    assert client.keys()[:] == list(client) == keys
    # Slice beyond length.
    assert 100 > len(client.keys())
    assert client.keys()[:100] == client.keys().head(100) == keys[:100] == keys[:]
    assert empty_client.keys().head() == []
    assert empty_client.keys().tail() == []
    # Test out of bounds
    with pytest.raises(IndexError):
        client.keys()[len(keys)]
    with pytest.raises(IndexError):
        client.keys()[-len(keys) - 1]
    with pytest.raises(IndexError):
        empty_client.keys().first()
    with pytest.raises(IndexError):
        empty_client.keys().last()
    # These should be in bounds and should not raise.
    client.keys()[len(keys) - 1]
    client.keys()[-len(keys)]

    # smoke test values()
    client.values().first()
    client.values().last()
    client.values().head()
    client.values().tail()
    client.values()[1:3]

    # smoke test items()
    client.items().first()
    client.items().last()
    client.items().head()
    client.items().tail()
    client.items()[1:3]


def test_deprecated_indexer_accessors():
    with pytest.warns(DeprecationWarning):
        assert client.keys_indexer[:3] == keys[:3] == list("abc")
    # smoke test the others
    with pytest.warns(DeprecationWarning):
        client.values_indexer[:3]
    with pytest.warns(DeprecationWarning):
        client.items_indexer[:3]
