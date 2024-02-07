import pytest

from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app


@pytest.fixture
def context(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture
def client(context):
    client = from_context(context)
    yield client


def test_include_data_sources_method_on_self(client):
    "Calling include_data_sources() fetches data sources on self."
    client.write_array([1, 2, 3], key="x")
    with pytest.raises(RuntimeError):
        client["x"].data_sources
    client["x"].include_data_sources().data_sources is not None


def test_include_data_sources_method_affects_children(client):
    "Calling include_data_sources() fetches data sources on children."
    client.create_container("c")
    client["c"].write_array([1, 2, 3], key="x")
    c = client["c"].include_data_sources()
    c["x"].data_sources is not None


def test_include_data_sources_kwarg(context):
    "Passing include_data_sources to constructor includes them by default."
    client = from_context(context, include_data_sources=True)
    client.write_array([1, 2, 3], key="x")
    client["x"].data_sources is not None
    client["x"].include_data_sources() is not None
