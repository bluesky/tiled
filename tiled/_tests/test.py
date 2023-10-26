import numpy
import pandas
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..catalog import in_memory
from ..client import Context, from_context
from ..queries import FullText
from ..server.app import build_app

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


@pytest.fixture
def map_client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@pytest.fixture
def catalog_client(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        # Upload arrays and metadata from tree into client so that it has matching content.
        for i in range(3):
            data = {"x": i * numpy.ones(10), "y": 2 * i * numpy.ones(10)}
            df = pandas.DataFrame(data)
            metadata = {"number": i, "another": 2 * i}
            client.write_dataframe(df, metadata=metadata)
        yield client


@pytest.fixture(
    scope="module",
    params=("map_client", "catalog_client"),
)
def client(request: pytest.FixtureRequest):
    yield request.getfixturevalue(request.param)


def test_compound_search(client):
    results = client.search(FullText("dog")).search(FullText("yellow"))
    assert list(results) == ["b"]
