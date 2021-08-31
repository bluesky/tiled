from pathlib import Path

from tiled.client import from_config
from tiled.server.data_cache import get_data_cache, NO_CACHE


def test_data_cache_hit_and_miss(tmpdir):
    with open(Path(tmpdir, "data.csv"), "w") as file:
        file.write(
            """
a,b,c
1,2,3
"""
        )
    config = {
        "trees": [
            {
                "tree": "files",
                "path": "/",
                "args": {"directory": tmpdir},
            },
        ],
    }
    client = from_config(config)
    cache = get_data_cache()
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


def test_data_cache_disabled(tmpdir):
    with open(Path(tmpdir, "data.csv"), "w") as file:
        file.write(
            """
a,b,c
1,2,3
"""
        )
    config = {
        "trees": [
            {
                "tree": "files",
                "path": "/",
                "args": {"directory": tmpdir},
            },
        ],
        "data_cache": {"available_bytes": 0},
    }
    client = from_config(config)
    cache = get_data_cache()
    assert cache is NO_CACHE
    client["data"]
