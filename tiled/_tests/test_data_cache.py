from pathlib import Path
import time

from tiled.client import from_config
from tiled.server.data_cache import get_data_cache, NO_CACHE
from tiled.trees.files import DEFAULT_POLL_INTERVAL


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


def test_detect_content_changed(tmpdir):
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
    assert len(client["data"].read()) == 1
    with open(Path(tmpdir, "data.csv"), "w") as file:
        file.write(
            """
a,b,c
1,2,3
4,5,6
"""
        )
    time.sleep(4 * DEFAULT_POLL_INTERVAL)
    assert len(client["data"].read()) == 2
    with open(Path(tmpdir, "data.csv"), "w") as file:
        file.write(
            """
a,b,c
1,2,3
4,5,6
7,8,9
"""
        )
    time.sleep(4 * DEFAULT_POLL_INTERVAL)
    assert len(client["data"].read()) == 3
