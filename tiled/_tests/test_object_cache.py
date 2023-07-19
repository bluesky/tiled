from pathlib import Path

import numpy
import psutil
import pytest

from ..catalog import in_memory
from ..catalog.register import register
from ..client import Context, from_context
from ..server.app import build_app, build_app_from_config
from ..server.object_cache import NO_CACHE, ObjectCache, get_object_cache


def test_tallying_hits_and_misses():
    cache = ObjectCache(1e6)
    assert cache.get("a") is None
    assert cache.misses == 1
    assert cache.hits == 0
    assert cache.get("a") is None
    assert cache.misses == 2
    assert cache.hits == 0
    arr = numpy.ones((5, 5))
    cache.put("a", arr, cost=1)
    assert cache.get("a") is arr
    assert cache.misses == 2
    assert cache.hits == 1
    assert cache.get("a") is arr
    assert cache.misses == 2
    assert cache.hits == 2
    cache.discard("a")
    assert cache.get("a") is None
    assert cache.misses == 3
    assert cache.hits == 2


def test_too_large_item():
    AVAILABLE_BYTES = 10  # very small limit
    cache = ObjectCache(AVAILABLE_BYTES)
    arr = numpy.ones((5, 5))
    assert arr.nbytes > AVAILABLE_BYTES
    cache.put("b", arr, cost=1)
    assert cache.get("b") is None
    # Manually specify the size.
    cache.put("b", arr, cost=1, nbytes=arr.nbytes)
    assert cache.get("b") is None


def test_eviction():
    AVAILABLE_BYTES = 300
    cache = ObjectCache(AVAILABLE_BYTES)
    arr1 = numpy.ones((5, 5))  # 200 bytes
    arr2 = 2 * numpy.ones((5, 5))
    cache.put("arr1", arr1, cost=1)
    assert "arr1" in cache
    # Costly one evicts the previous one.
    cache.put("arr2", arr2, cost=5)
    assert "arr1" not in cache
    assert "arr2" in cache
    # Cheap one does not evict the previous one.
    cache.put("arr1", arr1, cost=1)
    assert "arr1" not in cache
    assert "arr2" in cache


@pytest.mark.asyncio
async def test_object_cache_hit_and_miss(tmpdir):
    with open(Path(tmpdir, "data.csv"), "w") as file:
        file.write(
            """
a,b,c
1,2,3
"""
        )
    adapter = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(adapter)) as context:
        await register(adapter, tmpdir)
        client = from_context(context)
        cache = get_object_cache()
        assert cache.hits == cache.misses == 0
        client["data"].read()
        assert cache.misses == 2  # two dask objects in the cache
        assert cache.hits == 0
        client["data"].read()
        assert cache.misses == 2
        assert cache.hits == 2
        # Simulate eviction.
        cache.clear()
        client["data"].read()
        assert cache.misses == 4
        assert cache.hits == 2
        client["data"].read()
        assert cache.misses == 4
        assert cache.hits == 4


@pytest.mark.asyncio
async def test_object_cache_disabled(tmpdir):
    with open(Path(tmpdir, "data.csv"), "w") as file:
        file.write(
            """
a,b,c
1,2,3
"""
        )
    adapter = in_memory(readable_storage=[tmpdir])
    server_settings = {"object_cache": {"available_bytes": 0}}
    with Context.from_app(
        build_app(adapter, server_settings=server_settings)
    ) as context:
        await register(adapter, tmpdir)
        client = from_context(context)
        cache = get_object_cache()
        assert cache is NO_CACHE
        client["data"]


@pytest.mark.asyncio
async def test_detect_content_changed_or_removed(tmpdir):
    path = Path(tmpdir, "data.csv")
    with open(path, "w") as file:
        file.write(
            """
a,b,c
1,2,3
"""
        )
    adapter = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(adapter)) as context:
        await register(adapter, tmpdir)
        client = from_context(context)
        cache = get_object_cache()
        assert cache.hits == cache.misses == 0
        assert len(client["data"].read()) == 1
        with open(path, "w") as file:
            file.write(
                """
    a,b,c
    1,2,3
    4,5,6
    """
            )
        await register(adapter, tmpdir)
        assert len(client["data"].read()) == 2
        with open(path, "w") as file:
            file.write(
                """
    a,b,c
    1,2,3
    4,5,6
    7,8,9
    """
            )
        await register(adapter, tmpdir)
        assert len(client["data"].read()) == 3
        # Remove file.
        path.unlink()
        await register(adapter, tmpdir)
        assert "data" not in client
        with pytest.raises(KeyError):
            client["data"]


def test_cache_size_absolute(tmpdir):
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {"uri": tmpdir / "catalog.db", "init_if_not_exists": True},
            }
        ],
        "object_cache": {"available_bytes": 1000},
    }
    with Context.from_app(build_app_from_config(config)):
        cache = get_object_cache()
        assert cache.available_bytes == 1000


def test_cache_size_relative(tmpdir):
    # As a fraction of system memory
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {"uri": tmpdir / "catalog.db", "init_if_not_exists": True},
            }
        ],
        "object_cache": {"available_bytes": 0.1},
    }
    with Context.from_app(build_app_from_config(config)):
        cache = get_object_cache()
        actual = cache.available_bytes
        expected = psutil.virtual_memory().total * 0.1
        assert abs(actual - expected) / expected < 0.01  # inexact is OK
