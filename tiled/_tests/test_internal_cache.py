from pathlib import Path

from tiled.client import from_config
from tiled.server.internal_cache import get_internal_cache


def test_internal_cache_hit_and_miss(tmpdir):
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
    cache = get_internal_cache()
    assert cache.hits == cache.misses == 0
    client["data"]
    # The read_csv classmethod constructor greedily loads data into cache,
    # and then it is read out.
    assert cache.misses == 0
    assert cache.hits == 1
    client["data"]
    assert cache.misses == 0
    assert cache.hits == 2
    cache.clear()
    client["data"]
    # The read_csv classmethod constructor observes a cache miss, but then
    # re-loads the data into the cache. The data is handed directly off,
    # so there is no additional cache hit here.
    assert cache.misses == 1
    assert cache.hits == 2
    client["data"]
    # Now we get a hit.
    assert cache.misses == 1
    assert cache.hits == 3


def test_internal_cache_disabled(tmpdir):
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
        "internal_cache": {"available_bytes": 0},
    }
    client = from_config(config)
    cache = get_internal_cache()
    assert cache is None
    client["data"]
