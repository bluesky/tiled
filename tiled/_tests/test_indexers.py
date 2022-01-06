import string

import numpy

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree

tree = MapAdapter(
    {
        letter: ArrayAdapter.from_array(number * numpy.ones(10))
        for letter, number in zip(string.ascii_lowercase, range(26))
    }
)


def test_indexers():
    client = from_tree(tree)
    client.keys_indexer[:3] == list("abc")
    client.keys_indexer[:1] == list("a")
    client.keys_indexer[1:3] == list("bc")
    client.keys_indexer[-3::-1] == list("zyx")
    client.keys_indexer[:] == list(client) == string.ascii_lowercase
