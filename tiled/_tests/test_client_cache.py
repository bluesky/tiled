from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree
from ..client.cache import (
    Cache,
    CacheIsFull,
    NoCache,
    TooLargeForCache,
    WhenFull,
    download,
)
from ..client.utils import logger
from ..queries import FullText
from ..query_registration import register

tree = MapAdapter(
    {"arr": ArrayAdapter.from_array(numpy.arange(10), metadata={"a": 1})},
    metadata={"t": 1},
)


@pytest.fixture(params=["in_memory", "on_disk"])
def cache(request, tmpdir):
    if request.param == "in_memory":
        return Cache.in_memory(2e9)
    if request.param == "on_disk":
        return Cache.on_disk(tmpdir, capacity=2e9)


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_offline(cache, structure_clients):
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    expected_tree_md = client.metadata
    expected_arr = client["arr"][:]
    if structure_clients == "dask":
        expected_arr = expected_arr.compute()
    expected_arr_md = client["arr"].metadata
    client["arr"]  # should be a cache hit

    # Switch this client into offline mode.
    client.offline = True
    actual_tree_md = client.metadata
    actual_arr = client["arr"][:]
    if structure_clients == "dask":
        actual_arr = actual_arr.compute()
    actual_arr_md = client["arr"].metadata
    assert numpy.array_equal(actual_arr, expected_arr)
    assert expected_arr_md == actual_arr_md
    assert expected_tree_md == actual_tree_md

    # Make a fresh client in offline mode from the start.
    client = from_tree(
        tree, cache=cache, structure_clients=structure_clients, offline=True
    )
    actual_tree_md = client.metadata
    actual_arr = client["arr"][:]
    if structure_clients == "dask":
        actual_arr = actual_arr.compute()
    actual_arr_md = client["arr"].metadata
    assert numpy.array_equal(actual_arr, expected_arr)
    assert expected_arr_md == actual_arr_md
    assert expected_tree_md == actual_tree_md

    # Switch online.
    client.offline = False


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_download(cache, structure_clients):
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    download(client)
    assert cache.when_full == WhenFull.ERROR
    client.offline = True
    # smoke test
    client.metadata
    arr = client["arr"][:]
    if structure_clients == "dask":
        arr.compute()
    client["arr"].metadata


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_entries_stale_at(caplog, cache, structure_clients):
    """
    Test that entries_stale_at causes us to rely on the cache for some time.
    """
    logger.setLevel("DEBUG")
    EXPIRES_AFTER = 1  # seconds
    mapping = {"a": ArrayAdapter.from_array(numpy.arange(10), metadata={"a": 1})}
    tree = MapAdapter(
        mapping,
        metadata={"t": 1},
        entries_stale_after=timedelta(seconds=EXPIRES_AFTER),
    )
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    client["a"].download()
    # Entries are stored in cache.
    assert "Cache stored" in caplog.text
    caplog.clear()
    for _ in range(3):
        client["a"].download()
        # Entries are read from cache *without contacting server*
        assert "Cache read" in caplog.text
        assert "Cache stored" not in caplog.text
        assert "<- 304" not in caplog.text
        caplog.clear()
    client["a"].download()
    # By default, we do not refresh stale entries.
    assert "Cache read" in caplog.text
    assert "<- 304" not in caplog.text
    assert "Cache stored" not in caplog.text
    caplog.clear()
    client["a"].refresh()
    # Server is contacted to confirm cache is still valid.
    # The cache is still valid. Entries are read from cache.
    assert "<- 304" in caplog.text
    assert "Cache read" in caplog.text
    assert "Cache stored" not in caplog.text
    caplog.clear()
    # Change the entries...
    mapping["b"] = ArrayAdapter.from_array(2 * numpy.arange(10), metadata={"b": 2})
    client["b"].download()
    assert "Cache stored" in caplog.text


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_content_stale_at(caplog, cache, structure_clients):
    """
    Test that metadata_stale_at causes us to rely on the cache for some time.
    """
    logger.setLevel("DEBUG")
    mapping = {"a": ArrayAdapter.from_array(numpy.arange(10))}
    tree = MapAdapter(
        mapping,
        entries_stale_after=timedelta(seconds=1_000),
    )
    # Monkey-patch this attribute on for now because we aren't decided
    # how to best to expose this when initializing Adapters.
    mapping["a"].content_stale_at = datetime.utcnow() + timedelta(seconds=1_000)
    mapping["a"].metadata_stale_at = datetime.utcnow() + timedelta(seconds=1_000)
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    assert client["a"].read().sum() == 45
    # Content is stored in cache.
    assert "Cache stored" in caplog.text
    assert "Cache read" not in caplog.text
    caplog.clear()
    for _ in range(3):
        assert client["a"].read().sum() == 45
        # Content is read from cache *without contacting server*
        assert "Cache read" in caplog.text
        assert "Cache stored" not in caplog.text
        assert "<- 304" not in caplog.text
        caplog.clear()
    # This refresh will have no effect because nothing is stale.
    client["a"].refresh()
    assert "Cache read" in caplog.text
    assert "Cache stored" not in caplog.text
    assert "<- 304" not in caplog.text
    caplog.clear()
    # A force-refresh will have an effect.
    client["a"].refresh(force=True)
    # Server is contacted to confirm cache is still valid.
    # The cache is still valid. Content is read from cache.
    assert "Cache read" in caplog.text
    assert "Cache stored" not in caplog.text
    assert "<- 304" in caplog.text
    caplog.clear()
    # Change the content...
    mapping["a"] = ArrayAdapter.from_array(2 * numpy.arange(10))
    client["a"].refresh(force=True)
    # Server is contacted to confirm cache is still valid ("renew" it).
    # The cache is NOT still valid. Content is read from server.
    # Content is stored in cache.
    assert "Cache stored" in caplog.text
    assert client["a"].read().sum() == 90


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_metadata_stale_at(caplog, cache, structure_clients):
    """
    Test that metadata_stale_at causes us to rely on the cache for some time.
    """
    logger.setLevel("DEBUG")
    metadata = {"a": 1}
    mapping = {"a": ArrayAdapter.from_array(numpy.arange(10), metadata=metadata)}
    tree = MapAdapter(
        mapping,
        entries_stale_after=timedelta(seconds=1_000),
    )
    # Monkey-patch this attribute on for now because we aren't decided
    # how to best to expose this when initializing Adapters.
    mapping["a"].metadata_stale_at = datetime.utcnow() + timedelta(seconds=1_000)
    mapping["a"].content_stale_at = datetime.utcnow() + timedelta(seconds=1_000)
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    assert client["a"].metadata["a"] == 1
    # Metadata are stored in cache.
    assert "Cache stored" in caplog.text
    assert "Cache read" not in caplog.text
    caplog.clear()
    for _ in range(3):
        assert client["a"].metadata["a"] == 1
        # Metadata are read from cache *without contacting server*
        assert "Cache read" in caplog.text
        assert "Cache stored" not in caplog.text
        caplog.clear()
    assert client["a"].metadata["a"] == 1
    caplog.clear()
    # This refresh will have an effect because we haven't downloaded everything yet.
    client["a"].refresh()
    assert "Cache read" in caplog.text
    assert "Cache stored" in caplog.text
    assert "<- 304" not in caplog.text
    caplog.clear()
    # A force-refresh will also have an effect.
    client["a"].refresh(force=True)
    # Server is contacted to confirm cache is still valid.
    # The cache is still valid. Metadata are read from cache.
    assert "Cache read" in caplog.text
    assert "Cache stored" not in caplog.text
    caplog.clear()
    # Change the metadata...
    metadata["a"] = 2
    client.refresh(force=True)
    # Server is contacted to confirm cache is still valid.
    # The cache is NOT still valid. Metadata are read from server.
    # Metadata are stored in cache.
    assert client["a"].metadata["a"] == 2
    assert "Cache stored" in caplog.text


def test_download_with_no_cache():
    tree = MapAdapter({})
    client = from_tree(tree)
    with pytest.raises(NoCache):
        client.download()


@register("stable")
@dataclass
class StableQuery:
    "A dummy query whose results will not change"
    dummy: str


def test_must_revalidate(caplog, cache):
    """
    By default, search results set Cache-Control: must-revalidate.
    Queries can override this if they expect the results to be very stable.
    (The motivating application is Bluesky scan_id lookup --- not _technically_
    unique but generally expected to be stable and not worth revalidating.)
    """

    def stable_query_search(query, tree):
        result = tree.new_variation(must_revalidate=False)
        return result

    logger.setLevel("DEBUG")
    mapping = {
        "a": ArrayAdapter.from_array(numpy.arange(10), metadata={"color": "red"})
    }
    tree = MapAdapter(
        mapping,
        metadata={"t": 1},
        must_revalidate=True,
    )
    tree.register_query(StableQuery, stable_query_search)
    client = from_tree(tree, cache=cache)
    caplog.clear()
    list(client.search(FullText("red")))
    assert "must-revalidate" in caplog.text

    # The must-revalidate Cache-Control directive forces a refresh.
    caplog.clear()
    list(client.search(FullText("red")))
    assert "<- 304" in caplog.text
    assert "must-revalidate" in caplog.text
    # But the results have not changed.
    assert "<- 200" not in caplog.text

    # The StableQuery does not require revalidation.
    caplog.clear()
    list(client.search(StableQuery(dummy="stuff")))
    assert "must-revalidate" not in caplog.text

    caplog.clear()
    list(client.search(StableQuery(dummy="stuff")))
    assert "must-revalidate" not in caplog.text
    assert "<- 304" not in caplog.text
    assert "<- 200" not in caplog.text


def test_when_full(caplog):
    logger.setLevel("DEBUG")
    arr = numpy.random.random((1000, 1000))
    tree = MapAdapter(
        {
            "a": ArrayAdapter.from_array(arr),
            "b": ArrayAdapter.from_array(arr),
            "c": ArrayAdapter.from_array(arr),
        }
    )

    # error
    cache = Cache.in_memory(1.5 * arr.nbytes)
    cache.when_full = "error"
    c = from_tree(tree, cache=cache)
    c["a"][:]
    with pytest.raises(CacheIsFull):
        c["b"][:]

    # warn
    cache = Cache.in_memory(1.5 * arr.nbytes)
    cache.when_full = "warn"
    c = from_tree(tree, cache=cache)
    c["a"][:]
    with pytest.warns(UserWarning):
        c["b"][:]

    # evict
    caplog.clear()
    cache = Cache.in_memory(1.5 * arr.nbytes)
    assert cache.when_full == WhenFull.EVICT  # default
    c = from_tree(tree, cache=cache)
    c["a"][:]
    assert "stored (8_000_000 B" in caplog.text
    caplog.clear()
    c["b"][:]
    assert "stored (8_000_000 B" in caplog.text
    assert "evicted 8_000_000 B" in caplog.text


def test_too_large(caplog):
    logger.setLevel("DEBUG")
    arr = numpy.random.random((1000, 1000))
    tree = MapAdapter(
        {
            "a": ArrayAdapter.from_array(arr),
        }
    )

    # error
    cache = Cache.in_memory(0.5 * arr.nbytes)
    cache.when_full = "error"
    c = from_tree(tree, cache=cache)
    c["a"]
    with pytest.raises(TooLargeForCache):
        c["a"][:]

    # warn
    cache = Cache.in_memory(0.5 * arr.nbytes)
    cache.when_full = "warn"
    c = from_tree(tree, cache=cache)
    c["a"]
    with pytest.warns(UserWarning):
        c["a"][:]

    # evict
    cache = Cache.in_memory(0.5 * arr.nbytes)
    assert cache.when_full == WhenFull.EVICT  # default
    c = from_tree(tree, cache=cache)
    c["a"]
    c["a"][:]
