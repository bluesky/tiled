import pytest

from tiled.catalog import in_memory
from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app

N = 10  # number of items generated in sample


@pytest.fixture(scope="module")
def client(tmpdir_module):
    catalog = in_memory(writable_storage=[tmpdir_module])
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        for i in range(N):
            client.create_container(metadata={"num": i})
        yield client


def test_first(client):
    "Fetching the first element requests a page of size 1."
    with record_history() as history:
        item = client.values().first()
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "1"
    assert item.metadata["num"] == 0


def test_last(client):
    "Fetching the last element requests a page of size 1."
    with record_history() as history:
        item = client.values().last()
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "1"
    assert item.metadata["num"] == N - 1


def test_head(client):
    "Fetching the 'head' requests a page of size 5."
    with record_history() as history:
        items = client.values().head(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_tail(client):
    "Fetching the 'tail' requests a page of size 5."
    with record_history() as history:
        items = client.values().tail(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [5, 6, 7, 8, 9]
    assert actual_nums == expected_nums


def test_middle_forward(client):
    "Fetching a slice in the middle requests a page of the correct size."
    with record_history() as history:
        items = client.values()[2:6]
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "4"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [2, 3, 4, 5]
    assert actual_nums == expected_nums


def test_middle_backward(client):
    "Fetching a slice in the middle requests a page of the correct size."
    with record_history() as history:
        items = client.values()[-2:-6:-1]
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "4"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [8, 7, 6, 5]
    assert actual_nums == expected_nums


def test_manual_page_size(client):
    "The page_size method can set the page size manually."
    with record_history() as history:
        items = client.values().page_size(2).head(5)
    assert len(history.requests) == 3
    for request in history.requests:
        assert request.url.params["page[limit]"] == "2"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_manual_page_size_truncated(client):
    "If the manual page size is larger than the result set, it is truncated."
    with record_history() as history:
        items = client.values().page_size(6).head(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums
