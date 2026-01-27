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
            client.create_container(metadata={"num": i}, key=str(i))
        yield client


def test_first_value(client):
    "Fetching the first value requests a page of size 1."
    with record_history() as history:
        item = client.values().first()
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "1"
    assert item.metadata["num"] == 0


def test_first_key(client):
    "Fetching the first key requests a page of size 1."
    with record_history() as history:
        key = client.keys().first()
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "1"
    assert key == "0"


def test_last_value(client):
    "Fetching the last value requests a page of size 1."
    with record_history() as history:
        item = client.values().last()
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "1"
    assert item.metadata["num"] == N - 1


def test_last_key(client):
    "Fetching the last key requests a page of size 1."
    with record_history() as history:
        key = client.keys().last()
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "1"
    assert key == str(N - 1)


def test_head_values(client):
    "Fetching the 'head' values requests a page of size 5."
    with record_history() as history:
        items = client.values().head(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_head_keys(client):
    "Fetching the 'head' keys requests a page of size 5."
    with record_history() as history:
        keys = client.keys().head(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [int(key) for key in keys]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_tail_values(client):
    "Fetching the 'tail' values requests a page of size 5."
    with record_history() as history:
        items = client.values().tail(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [5, 6, 7, 8, 9]
    assert actual_nums == expected_nums


def test_tail_keys(client):
    "Fetching the 'tail' keys requests a page of size 5."
    with record_history() as history:
        keys = client.keys().tail(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [int(key) for key in keys]
    expected_nums = [5, 6, 7, 8, 9]
    assert actual_nums == expected_nums


def test_middle_forward_values(client):
    "Fetching a slice of values in the middle requests a page of the correct size."
    with record_history() as history:
        items = client.values()[2:6]
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "4"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [2, 3, 4, 5]
    assert actual_nums == expected_nums


def test_middle_forward_keys(client):
    "Fetching a slice of keys in the middle requests a page of the correct size."
    with record_history() as history:
        keys = client.keys()[2:6]
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "4"
    actual_nums = [int(key) for key in keys]
    expected_nums = [2, 3, 4, 5]
    assert actual_nums == expected_nums


def test_middle_backward_values(client):
    "Fetching a slice of values in the middle requests a page of the correct size."
    with record_history() as history:
        items = client.values()[-2:-6:-1]
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "4"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [8, 7, 6, 5]
    assert actual_nums == expected_nums


def test_middle_backward_keys(client):
    "Fetching a slice of keys in the middle requests a page of the correct size."
    with record_history() as history:
        keys = client.keys()[-2:-6:-1]
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "4"
    actual_nums = [int(key) for key in keys]
    expected_nums = [8, 7, 6, 5]
    assert actual_nums == expected_nums


def test_manual_page_size_values(client):
    "The page_size method on values() can set the page size manually."
    with record_history() as history:
        items = client.values().page_size(2).head(5)
    assert len(history.requests) == 3
    for request in history.requests:
        assert request.url.params["page[limit]"] == "2"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_manual_page_size_keys(client):
    "The page_size method on keys() can set the page size manually."
    with record_history() as history:
        keys = client.keys().page_size(2).head(5)
    assert len(history.requests) == 3
    for request in history.requests:
        assert request.url.params["page[limit]"] == "2"
    actual_nums = [int(key) for key in keys]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_manual_page_size_truncated_values(client):
    "If the manual page size is larger than the result set, it is truncated."
    with record_history() as history:
        items = client.values().page_size(6).head(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_manual_page_size_truncated_keys(client):
    "If the manual page size is larger than the result set, it is truncated."
    with record_history() as history:
        keys = client.keys().page_size(6).head(5)
    assert len(history.requests) == 1
    assert history.requests[0].url.params["page[limit]"] == "5"
    actual_nums = [int(key) for key in keys]
    expected_nums = [0, 1, 2, 3, 4]
    assert actual_nums == expected_nums


def test_unbounded_values_slice(client):
    "An unbounded slice lets the server set the page size."
    with record_history() as history:
        items = client.values()[3:]
    assert len(history.requests) == 1
    assert "page[limit]" not in history.requests[0].url.params
    actual_nums = [item.metadata["num"] for item in items]
    expected_nums = [3, 4, 5, 6, 7, 8, 9]
    assert actual_nums == expected_nums


def test_unbounded_keys_slice(client):
    "An unbounded slice lets the server set the page size."
    with record_history() as history:
        keys = client.keys()[3:]
    assert len(history.requests) == 1
    assert "page[limit]" not in history.requests[0].url.params
    actual_nums = [int(key) for key in keys]
    expected_nums = [3, 4, 5, 6, 7, 8, 9]
    assert actual_nums == expected_nums
