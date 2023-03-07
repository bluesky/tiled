import pytest

from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, record_history
from ..server.app import build_app


@pytest.fixture(scope="module")
def context():
    tree = MapAdapter({})
    app = build_app(tree)
    with Context.from_app(app) as context:
        yield context


def test_history(context):
    "Very basic exercise of history"
    client = from_context(context)
    with record_history() as history:
        repr(client)  # trigger a request
    assert history.requests
    assert history.responses
