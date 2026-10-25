"""Unit tests for the search protocol layer (`tiled.client._protocol.search`).

These tests exercise the generic paginated-collection builders, the query
parameter encoding, and the container-children specialization as plain
functions and dataclasses, with no server and no I/O.
"""

import pytest

from tiled.client._protocol.search import (
    ChildrenSearchBuilder,
    DeepSearchBuilder,
    PaginatedCollectionBuilder,
)
from tiled.queries import FullText, Key, queries_to_params

LINK = "http://localhost:8000/api/v1/revisions/a/b"
SELF_LINK = "http://localhost:8000/api/v1/metadata/a/b"
SEARCH_LINK = "http://localhost:8000/api/v1/search/a/b"
SEARCH_DEEP_LINK = "http://localhost:8000/api/v1/search-deep/a/b"
MSGPACK_MIMETYPE = "application/x-msgpack"

QUERIES = [FullText("hello")]
SORTING = [("color", 1)]
QUERIES_AS_PARAMS = {"filter[fulltext][condition][text]": ["hello"]}
SORTING_AS_PARAMS = {"sort": ["color"]}


def test_first_page_link():
    builder = PaginatedCollectionBuilder(LINK)
    assert builder.first_page_link(10) == f"{LINK}?page[offset]=10"
    assert builder.first_page_link(10, 5) == f"{LINK}?page[offset]=10&page[limit]=5"
    # A zero limit is a count-only page and must not read as "no limit".
    assert builder.first_page_link(0, 0) == f"{LINK}?page[offset]=0&page[limit]=0"


def test_draft_request_read_page_applies_fixed_params_to_every_page():
    builder = PaginatedCollectionBuilder(LINK, fixed_params={"select_metadata": False})
    # A server-provided next link carries its own pagination query; the
    # collection's fixed parameters are re-applied alongside it.
    next_link = f"{LINK}?page[offset]=100&page[limit]=100"
    draft = builder.draft_request_read_page(next_link)
    assert draft.method == "GET"
    assert draft.params == {
        "page[offset]": ["100"],
        "page[limit]": ["100"],
        "select_metadata": False,
    }
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_draft_request_read_page_extra_params():
    builder = PaginatedCollectionBuilder(LINK, fixed_params={"a": 1})
    draft = builder.draft_request_read_page(LINK, extra_params={"fields": ""})
    assert draft.params == {"a": 1, "fields": ""}


def test_draft_request_count():
    builder = PaginatedCollectionBuilder(LINK, fixed_params={"max_depth": 2})
    draft = builder.draft_request_count()
    assert draft.method == "GET"
    assert draft.url == LINK
    assert draft.params == {"fields": "count", "max_depth": 2}
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_parse_response_count():
    assert PaginatedCollectionBuilder.parse_response_count({"meta": {"count": 7}}) == 7


def test_queries_to_params():
    assert queries_to_params(FullText("hello")) == QUERIES_AS_PARAMS
    assert queries_to_params() == {}


def test_from_queries():
    builder = ChildrenSearchBuilder.from_queries(SEARCH_LINK, QUERIES, SORTING)
    assert builder.link == SEARCH_LINK
    assert builder.fixed_params == QUERIES_AS_PARAMS
    assert builder.sorting_params == SORTING_AS_PARAMS
    assert builder.reversed_sorting_params == {"sort": ["-color"]}
    empty = ChildrenSearchBuilder.from_queries(SEARCH_LINK, [], [])
    assert empty.fixed_params == {}


@pytest.mark.parametrize(
    "sorting, expected, expected_reversed",
    [
        (
            [("color", 1), ("height", -1)],
            {"sort": ["color", "-height"]},
            {"sort": ["-color", "height"]},
        ),
        # An explicit "+" prefix means ascending in the HTTP API, the same
        # as no prefix; it is normalized away in either direction.
        ([("+color", 1)], {"sort": ["color"]}, {"sort": ["-color"]}),
        # The server-default sorting encodes to nothing (no `sort`
        # parameter); its reverse is expressed as "-".
        ([("", 1)], {}, {"sort": ["-"]}),
        ([], {}, {}),
    ],
)
def test_from_queries_sorting_params(sorting, expected, expected_reversed):
    builder = ChildrenSearchBuilder.from_queries(SEARCH_LINK, [], sorting)
    assert builder.sorting_params == expected
    assert builder.reversed_sorting_params == expected_reversed


@pytest.mark.parametrize(
    "contents, queries, sorting, expected",
    [
        # No inlined contents: nothing to serve from.
        (None, [], [("_", 1)], False),
        # Empty inlined contents still serve (the container is empty).
        ({}, [], [("_", 1)], True),
        ({"a": {}}, [], [("_", 1)], True),
        ({"a": {}}, [], [], True),
        # Queries filter, which the inlined contents do not reflect.
        ({"a": {}}, [FullText("x")], [("_", 1)], False),
        # Any sorting other than absent or the server default disqualifies,
        # including the client-side [("", 1)] fallback.
        ({"a": {}}, [], [("", 1)], False),
        ({"a": {}}, [], [("color", 1)], False),
    ],
)
def test_may_serve_from_inlined_contents(contents, queries, sorting, expected):
    assert (
        ChildrenSearchBuilder.may_serve_from_inlined_contents(
            contents, queries, sorting
        )
        is expected
    )


def test_children_draft_request_count():
    draft = ChildrenSearchBuilder.from_queries(
        SEARCH_LINK, QUERIES, SORTING
    ).draft_request_count()
    assert draft.method == "GET"
    assert draft.url == SEARCH_LINK
    assert draft.params == {
        "fields": "count",
        **QUERIES_AS_PARAMS,
        **SORTING_AS_PARAMS,
    }
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_draft_request_read_page_keys_only():
    # A keys-only page requests no attribute fields: only the entry ids.
    builder = ChildrenSearchBuilder.from_queries(SEARCH_LINK, QUERIES, SORTING)
    draft = builder.draft_request_read_page(
        builder.first_page_link(10, 5), keys_only=True
    )
    assert draft.method == "GET"
    # The pagination query baked into the page link is preserved.
    assert draft.params == {
        "page[offset]": ["10"],
        "page[limit]": ["5"],
        "fields": "",
        **QUERIES_AS_PARAMS,
        **SORTING_AS_PARAMS,
    }
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_draft_request_read_page_items():
    # An items page requests full attributes, optionally with data sources.
    builder = ChildrenSearchBuilder.from_queries(SEARCH_LINK, [], [])
    draft = builder.draft_request_read_page(
        builder.first_page_link(0),
        keys_only=False,
        include_data_sources=True,
    )
    assert draft.params == {"page[offset]": ["0"], "include_data_sources": True}


def test_draft_request_read_page_direction():
    # A negative direction iterates backwards: the reversed sorting is sent.
    builder = ChildrenSearchBuilder.from_queries(SEARCH_LINK, QUERIES, SORTING)
    page_link = builder.first_page_link(0)
    forward = builder.draft_request_read_page(page_link, keys_only=True, direction=1)
    backward = builder.draft_request_read_page(page_link, keys_only=True, direction=-1)
    assert forward.params["sort"] == ["color"]
    assert backward.params["sort"] == ["-color"]


def test_draft_request_lookup_key():
    # A lookup under queries is itself a search: the key is expressed as a
    # KeyLookup query alongside the node's own queries.
    draft = ChildrenSearchBuilder.from_queries(
        SEARCH_LINK, QUERIES, SORTING
    ).draft_request_lookup_key("c", include_data_sources=True)
    assert draft.method == "GET"
    assert draft.url == SEARCH_LINK
    assert draft.params == {
        "filter[lookup][condition][key]": ["c"],
        **QUERIES_AS_PARAMS,
        **SORTING_AS_PARAMS,
        "include_data_sources": True,
    }
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_draft_request_read_distinct():
    draft = ChildrenSearchBuilder.from_queries(
        SEARCH_LINK, QUERIES, SORTING
    ).draft_request_read_distinct(SELF_LINK, ("color", "shape"), False, True, True)
    assert draft.method == "GET"
    assert draft.url == SELF_LINK.replace("/metadata", "/distinct", 1)
    assert draft.params == {
        "metadata": ("color", "shape"),
        "structure_families": False,
        "specs": True,
        "counts": True,
        **QUERIES_AS_PARAMS,
    }
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_deep_search_builder_from_queries():
    # The encoded queries, the optional depth limit, and the data-sources
    # flag are fixed at construction and applied to every draft.
    builder = DeepSearchBuilder.from_queries(
        SEARCH_DEEP_LINK,
        [Key("color") == "red"],
        max_depth=2,
        include_data_sources=True,
    )
    draft = builder.draft_request_read_page(builder.first_page_link(0, 10))
    assert draft.params["filter[eq][condition][key]"] == ["color"]
    assert draft.params["max_depth"] == 2
    assert draft.params["include_data_sources"] is True
    # Both options are omitted by default.
    builder = DeepSearchBuilder.from_queries(SEARCH_DEEP_LINK, [])
    assert builder.fixed_params == {}


@pytest.mark.parametrize("param", ["page[offset]", "page[cursor]", "page[limit]"])
def test_explicit_pagination_params_are_rejected(param):
    # Pagination belongs to the page link; an explicitly passed pagination
    # parameter would override the link's and desynchronize the position.
    builder = PaginatedCollectionBuilder(SEARCH_LINK)
    with pytest.raises(ValueError):
        builder.draft_request_read_page(SEARCH_LINK, extra_params={param: 0})
    with pytest.raises(ValueError):
        PaginatedCollectionBuilder(SEARCH_LINK, fixed_params={param: 0})
