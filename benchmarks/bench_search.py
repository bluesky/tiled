"""
Searching containers of SMALL and LARGE children.

Each benchmark fetches the first page of results (the common interactive
case) unless it says otherwise. `Eq` queries use the JSONB containment
index; `Comparison` and `Like` cannot use an index and scan the container.
"""

import pytest

from tiled.queries import Comparison, Contains, FullText, Key, Like

pytestmark = pytest.mark.benchmark(group="search")

SIZES = ["small", "large"]


@pytest.fixture(params=SIZES, scope="module")
def container(request, client):
    return client[request.param]


def first_page(search):
    return search.keys().head(100)


def bench_search_eq(benchmark, container):
    "Equality on a top-level key, matching a third of the nodes."
    benchmark(lambda: first_page(container.search(Key("color") == "red")))


def bench_search_eq_unique(benchmark, container):
    "Equality matching exactly one node."
    benchmark(lambda: first_page(container.search(Key("number") == 42)))


def bench_search_eq_nested(benchmark, container):
    "Equality on a nested key."
    benchmark(lambda: first_page(container.search(Key("nested.depth") == 3)))


def bench_search_comparison(benchmark, container):
    "Range query, matching a tenth of the nodes (not indexable)."
    benchmark(lambda: first_page(container.search(Comparison("lt", "number", 100))))


def bench_search_fulltext(benchmark, container):
    benchmark(lambda: first_page(container.search(FullText("green"))))


def bench_search_like(benchmark, container):
    "Substring match on a metadata value (not indexable)."
    benchmark(lambda: first_page(container.search(Like("label", "%blue%"))))


def bench_search_contains(benchmark, container):
    benchmark(lambda: first_page(container.search(Contains("label", "sample"))))


def bench_search_count(benchmark, container):
    "Exact count of the results of a query matching a third of the nodes."
    benchmark(lambda: len(container.search(Key("color") == "red")))


def bench_search_deep(benchmark, client):
    "Search all 1364 nodes of the tree for the 4 at level 1 with index 0."
    tree = client["tree"]
    benchmark(
        lambda: list(tree.search_deep(Key("level") == 1, Key("index") == 0).keys())
    )


def bench_paginate_all(benchmark, client):
    "Walk every page of the SMALL container's keys, 100 per page."
    benchmark(lambda: list(client["small"].keys().page_size(100)))
