import collections

import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.smoke import read
from ..server.app import build_app


def f():
    raise Exception("test!")


def test_smoke_read():
    data = collections.defaultdict(f)
    data["A"] = ArrayAdapter.from_array([1, 2, 3], metadata={"A": "a"})

    tree = MapAdapter(data)
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

    faulty_list = read(client)
    assert len(faulty_list) == 1

    with pytest.raises(Exception):
        data["B"]
