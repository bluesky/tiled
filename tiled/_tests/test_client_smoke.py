import numpy as np
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.smoke import read
from ..server.app import build_app


class Broken(Exception):
    pass


class BrokenArrayAdapter(ArrayAdapter):
    def read(self, *args, **kwargs):
        raise Broken

    def read_block(self, *args, **kwargs):
        raise Broken


@pytest.fixture(scope="module")
def context():
    mapping = {
        "A": ArrayAdapter.from_array(np.array([1, 2, 3]), metadata={"A": "a"}),
        "B": BrokenArrayAdapter.from_array(np.array([4, 5, 6]), metadata={"B": "b"}),
    }

    tree = MapAdapter(mapping)
    with Context.from_app(build_app(tree)) as context:
        yield context


def test_smoke_read_list(context):
    client = from_context(context)

    faulty_list = read(client)
    assert len(faulty_list) == 1


def test_smoke_read_raise(context):
    client = from_context(context)

    with pytest.raises(Broken):
        read(client, strict=True)
