import pytest

from ..catalog import in_memory
from ..client import Context, from_context
from ..server.app import build_app


@pytest.fixture
def client(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


def test_with_sources(client):
    x = client.write_array([1, 2, 3], key="x")
    x.data_sources is not None
    client.values().with_data_sources()[0].data_sources is not None
