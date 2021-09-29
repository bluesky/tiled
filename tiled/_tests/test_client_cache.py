from datetime import datetime, timedelta
import time

import numpy
import pytest

from ..client import from_tree
from ..client.utils import logger
from ..client.cache import Cache, download
from ..readers.array import ArrayAdapter
from ..trees.in_memory import Tree

tree = Tree(
    {"arr": ArrayAdapter.from_array(numpy.arange(10), metadata={"a": 1})},
    metadata={"t": 1},
)


@pytest.fixture(params=["in_memory", "on_disk"])
def cache(request, tmpdir):
    if request.param == "in_memory":
        return Cache.in_memory(2e9)
    if request.param == "on_disk":
        return Cache.on_disk(tmpdir, available_bytes=2e9)


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_integration(cache, structure_clients):
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    expected_tree_md = client.metadata
    expected_arr = client["arr"][:]
    if structure_clients == "dask":
        expected_arr = expected_arr.compute()
    expected_arr_md = client["arr"].metadata
    client["arr"]  # should be a cache hit
    client.offline = True
    actual_tree_md = client.metadata
    actual_arr = client["arr"][:]
    if structure_clients == "dask":
        actual_arr = actual_arr.compute()
    actual_arr_md = client["arr"].metadata
    assert numpy.array_equal(actual_arr, expected_arr)
    assert expected_arr_md == actual_arr_md
    assert expected_tree_md == actual_tree_md


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_download(cache, structure_clients):
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    download(client)
    client.offline = True
    # smoke test
    client.metadata
    arr = client["arr"][:]
    if structure_clients == "dask":
        arr.compute()
    client["arr"].metadata


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_entries_stale_after(caplog, cache, structure_clients):
    """
    Test that entries_stale_after causes us to rely on the cache for some time.
    """
    logger.setLevel("DEBUG")
    EXPIRES_AFTER = 1  # seconds
    mapping = {"a": ArrayAdapter.from_array(numpy.arange(10), metadata={"a": 1})}
    tree = Tree(
        mapping,
        metadata={"t": 1},
        entries_stale_after=timedelta(seconds=EXPIRES_AFTER),
    )
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    assert "'a'" in repr(client)
    # Entries are stored in cache.
    assert "Cache stored" in caplog.text
    assert "Cache read" not in caplog.text
    assert "Cache is stale" not in caplog.text
    assert "Cache renewed" not in caplog.text
    caplog.clear()
    for _ in range(3):
        assert "'a'" in repr(client)
        # Entries are read from cache *without contacting server*
        assert "Cache read" in caplog.text
        assert "Cache stored" not in caplog.text
        assert "Cache is stale" not in caplog.text
        assert "Cache renewed" not in caplog.text
        caplog.clear()
    time.sleep(EXPIRES_AFTER)
    assert "'a'" in repr(client)
    # Server is contacted to confirm cache is still valid ("renew" it).
    # The cache is still valid. Entries are read from cache.
    assert "Cache is stale" in caplog.text
    assert "Cache renewed" in caplog.text
    assert "Cache read" in caplog.text
    assert "Cache stored" not in caplog.text
    caplog.clear()
    # Change the entries...
    mapping["b"] = ArrayAdapter.from_array(2 * numpy.arange(10), metadata={"b": 2})
    time.sleep(EXPIRES_AFTER)
    assert "'b'" in repr(client)
    # Server is contacted to confirm cache is still valid ("renew" it).
    # The cache is NOT still valid. Entries are read from server.
    # Entries are stored in cache.
    assert "Cache is stale" in caplog.text
    assert "Cache stored" in caplog.text
    assert "Cache renewed" not in caplog.text
    assert "Cache read" not in caplog.text


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_metadata_stale_after(caplog, cache, structure_clients):
    """
    Test that metadata_stale_after causes us to rely on the cache for some time.
    """
    logger.setLevel("DEBUG")
    EXPIRES_AFTER = 3  # seconds
    metadata = {"a": 1}
    mapping = {"a": ArrayAdapter.from_array(numpy.arange(10), metadata=metadata)}
    tree = Tree(
        mapping,
        entries_stale_after=timedelta(seconds=1_000),
    )
    mapping["a"].metadata_stale_at = datetime.utcnow() + timedelta(
        seconds=EXPIRES_AFTER
    )
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    assert client["a"].metadata["a"] == 1
    # Metadata are stored in cache.
    assert "Cache stored" in caplog.text
    assert "Cache read" not in caplog.text
    assert "Cache is stale" not in caplog.text
    assert "Cache renewed" not in caplog.text
    caplog.clear()
    for _ in range(3):
        assert client["a"].metadata["a"] == 1
        # Metadata are read from cache *without contacting server*
        assert "Cache read" in caplog.text
        assert "Cache stored" not in caplog.text
        assert "Cache is stale" not in caplog.text
        assert "Cache renewed" not in caplog.text
        caplog.clear()
    time.sleep(EXPIRES_AFTER)
    assert client["a"].metadata["a"] == 1
    # Server is contacted to confirm cache is still valid ("renew" it).
    # The cache is still valid. Metadata are read from cache.
    assert "Cache is stale" in caplog.text
    assert "Cache renewed" in caplog.text
    assert "Cache read" in caplog.text
    assert "Cache stored" not in caplog.text
    caplog.clear()
    # Change the metadata...
    metadata["a"] = 2
    time.sleep(EXPIRES_AFTER)
    assert client["a"].metadata["a"] == 2
    # Server is contacted to confirm cache is still valid ("renew" it).
    # The cache is NOT still valid. Metadata are read from server.
    # Metadata are stored in cache.
    assert "Cache is stale" in caplog.text
    assert "Cache stored" in caplog.text
    assert "Cache renewed" not in caplog.text
    assert "Cache read" not in caplog.text


@pytest.mark.parametrize("structure_clients", ["numpy", "dask"])
def test_no_expires_header(caplog, cache, structure_clients):
    """
    When there is no Expires header, the cache is always stale.
    """
    logger.setLevel("DEBUG")
    mapping = {"a": ArrayAdapter.from_array(numpy.arange(10), metadata={"a": 1})}
    tree = Tree(
        mapping,
        metadata={"t": 1},
        entries_stale_after=None,
    )
    client = from_tree(tree, cache=cache, structure_clients=structure_clients)
    repr(client)
    assert "Cache stored" in caplog.text
    assert "Cache read" not in caplog.text
    assert "Cache is stale" not in caplog.text
    assert "Cache renewed" not in caplog.text
    caplog.clear()

    for _ in range(3):
        repr(client)
        assert "Cache is stale" in caplog.text
        assert "Cache read" in caplog.text
        assert "Cache renewed" not in caplog.text
        assert "Cache stored" not in caplog.text
        caplog.clear()
