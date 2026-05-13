import pytest

from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app

N = 10  # number of items generated in sample


@pytest.fixture
def client(catalog_adapter):
    app = build_app(catalog_adapter)
    with Context.from_app(app) as context:
        client = from_context(context)
        for i in range(N):
            client.create_container(metadata={"num": i}, key=str(i))
        yield client


@pytest.fixture
def sorted_client(catalog_adapter):
    """Client pre-populated with 5 items inserted in scrambled order.

    Insertion order: e, b, d, a, c
    Alphabetical (key/id) order: a, b, c, d, e

    The scrambled insertion order means default (time-based) sort != id sort,
    which is required for the cursor vs offset pagination tests to be meaningful.
    """
    app = build_app(catalog_adapter)
    with Context.from_app(app) as context:
        c = from_context(context)
        for letter in ["e", "b", "d", "a", "c"]:
            c.create_container(metadata={"letter": letter}, key=letter)
        yield c


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


def _params_from_next_link(next_link, sort=None, page_size=2):
    """Parse a pagination 'next' link into request params for the following page.

    Handles both cursor-based (?page[cursor]=...) and offset-based
    (?page[offset]=...) next links transparently.
    """
    from urllib.parse import parse_qs, urlparse

    qs = parse_qs(urlparse(next_link).query)
    params = {"page[limit]": page_size}
    if "page[cursor]" in qs:
        params["page[cursor]"] = qs["page[cursor]"][0]
    elif "page[offset]" in qs:
        params["page[offset]"] = qs["page[offset]"][0]
    if sort is not None:
        params["sort"] = sort
    return params


def _paginate_all(client, sort=None, page_size=2):
    """Walk all pages via next links and return the concatenated list of item ids."""
    params = {"page[limit]": page_size}
    if sort is not None:
        params["sort"] = sort
    http_client = client.context.http_client
    all_keys = []
    while True:
        resp = http_client.get(
            client.uri.replace("/api/v1/metadata/", "/api/v1/search/"), params=params
        )
        assert (
            resp.status_code == 200
        ), f"HTTP {resp.status_code}: {resp.json().get('detail')}"
        body = resp.json()
        page_keys = [item["id"] for item in body["data"]]
        all_keys.extend(page_keys)
        next_link = body["links"]["next"]
        if not next_link or not page_keys:
            break
        params = _params_from_next_link(next_link, sort=sort, page_size=page_size)
    return all_keys


def test_cursor_pagination_default_sort(sorted_client):
    """Default-sort first two pages are disjoint and next link is cursor-based."""
    http_client = sorted_client.context.http_client
    search_url = sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/")

    resp1 = http_client.get(search_url, params={"page[limit]": 2})
    assert resp1.status_code == 200
    body1 = resp1.json()
    page1_keys = [item["id"] for item in body1["data"]]
    assert len(page1_keys) == 2

    next_link = body1["links"]["next"]
    assert next_link is not None, "Expected a 'next' link after the first page"
    assert (
        "page[cursor]=" in next_link
    ), "Default sort should produce a cursor-based next link"

    resp2 = http_client.get(search_url, params=_params_from_next_link(next_link))
    assert resp2.status_code == 200
    page2_keys = [item["id"] for item in resp2.json()["data"]]
    assert len(page2_keys) == 2
    assert (
        set(page1_keys) & set(page2_keys) == set()
    ), f"Pages overlap: {page1_keys}, {page2_keys}"


def test_offset_fallback_non_default_sort_second_page(sorted_client):
    """Non-default sort falls back to offset-based next links.

    Insertion order is e,b,d,a,c so default time order != alphabetical key order.
    Sorted by 'id': a,b,c,d,e  →  page 1=[a,b], page 2=[c,d].
    The next link must be offset-based (not cursor-based) for non-default sorts.
    """
    http_client = sorted_client.context.http_client
    search_url = sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/")

    resp1 = http_client.get(search_url, params={"sort": "id", "page[limit]": 2})
    assert resp1.status_code == 200
    page1_keys = [item["id"] for item in resp1.json()["data"]]
    assert page1_keys == ["a", "b"], f"Unexpected page 1: {page1_keys}"

    next_link = resp1.json()["links"]["next"]
    assert next_link is not None, "Expected a 'next' link"
    assert (
        "page[offset]=" in next_link
    ), "Non-default sort should produce an offset-based next link, not a cursor"

    resp2 = http_client.get(
        search_url, params=_params_from_next_link(next_link, sort="id")
    )
    assert resp2.status_code == 200
    page2_keys = [item["id"] for item in resp2.json()["data"]]
    assert page2_keys == ["c", "d"], f"Wrong page 2 for 'id' sort: {page2_keys}"


def test_full_traversal_default_sort(sorted_client):
    """Full traversal with default sort yields all items exactly once."""
    all_keys = _paginate_all(sorted_client)
    assert len(all_keys) == 5
    assert len(set(all_keys)) == 5


def test_full_traversal_asc_sort(sorted_client):
    """Full traversal under ascending 'id' sort yields all items once, in order."""
    all_keys = _paginate_all(sorted_client, sort="id")
    assert all_keys == sorted(all_keys), f"Out of order: {all_keys}"
    assert len(all_keys) == 5
    assert len(set(all_keys)) == 5


def test_full_traversal_desc_sort(sorted_client):
    """Full traversal under descending 'id' sort yields all items once, in reverse order."""
    all_keys = _paginate_all(sorted_client, sort="-id")
    assert all_keys == sorted(all_keys, reverse=True)
    assert len(all_keys) == 5
    assert len(set(all_keys)) == 5


def test_sort_by_dotted_metadata_key_accepted(sorted_client):
    """?sort=metadata.letter must return 200, not 422."""
    resp = sorted_client.context.http_client.get(
        sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/"),
        params={"sort": "metadata.letter", "page[limit]": 5},
    )
    assert resp.status_code == 200


def test_sort_by_dotted_metadata_key_order(sorted_client):
    """?sort=metadata.letter returns items in alphabetical letter order."""
    resp = sorted_client.context.http_client.get(
        sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/"),
        params={"sort": "metadata.letter", "page[limit]": 10},
    )
    assert resp.status_code == 200
    keys = [item["id"] for item in resp.json()["data"]]
    assert keys == sorted(keys)


def test_full_traversal_dotted_metadata_sort(sorted_client):
    """Full traversal with metadata.letter sort yields all items once, in order.

    Exercises both: dotted sort key accepted and correct
    multi-page results for non-default sort via offset fallback.
    """
    all_keys = _paginate_all(sorted_client, sort="metadata.letter")
    assert all_keys == sorted(all_keys)
    assert len(all_keys) == 5
    assert len(set(all_keys)) == 5


def test_plus_prefix_sort_accepted(sorted_client):
    """?sort=+id (explicit ascending prefix) must return 200, not 422."""
    resp = sorted_client.context.http_client.get(
        sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/"),
        params={"sort": "+id", "page[limit]": 5},
    )
    assert (
        resp.status_code == 200
    ), f"HTTP {resp.status_code}: {resp.json().get('detail')}"
    keys = [item["id"] for item in resp.json()["data"]]
    assert keys == sorted(keys)


def test_plus_prefix_reverse_does_not_produce_malformed_sort(sorted_client):
    """Reversing a +id sort must send -id, not -+id, to the server."""
    # Simulate what the client does when reversing a +-prefixed sort field.
    # If the bug were present, reversed_sorting_list would contain "-+id" and
    # the server would reject it with 422.
    resp = sorted_client.context.http_client.get(
        sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/"),
        params={"sort": "-+id", "page[limit]": 5},
    )
    assert resp.status_code == 422, "'-+id' is malformed and should be rejected"

    resp = sorted_client.context.http_client.get(
        sorted_client.uri.replace("/api/v1/metadata/", "/api/v1/search/"),
        params={"sort": "-id", "page[limit]": 5},
    )
    assert resp.status_code == 200, "'-id' (correct reversal of '+id') must be accepted"
