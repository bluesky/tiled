import os
from typing import Any, Callable, Optional

import cachetools

# Cached items will be evicted if not used for a specified time interval
# ("time to use"). If the cache size reaches its max size, the least recently
# used cache item will be evicted.
DEFAULT_MAX_SIZE = int(os.getenv("TILED_RESOURCE_CACHE_MAX_SIZE", "1024"))
DEFAULT_TIME_TO_USE_SECONDS = float(os.getenv("TILED_RESOURCE_CACHE_TTU", "60."))


def get_resource_cache() -> cachetools.Cache:
    return _cache


def set_resource_cache(cache: cachetools.Cache) -> None:
    global _cache
    _cache = cache


def default_ttu(_key: str, value: Any, now: float):
    """
    Retain cached items for at most 60 seconds.
    """
    return DEFAULT_TIME_TO_USE_SECONDS + now


def default_resource_cache():
    return cachetools.TLRUCache(DEFAULT_MAX_SIZE, default_ttu)


def with_resource_cache(
    cache_key: str,
    factory: Callable,
    *args,
    _resource_cache: Optional[cachetools.Cache] = None,
    **kwargs,
):
    """
    Use value from cache or, if not present, call factory(*args, **kwargs) and cache result.

    This uses a globally configured resource cache by default.
    For testing and debugging, a cache may be passed to the
    parameter _resource_cache.
    """
    if _resource_cache is None:
        cache = get_resource_cache()
    else:
        cache = _resource_cache
    # Return cached value if found.
    value = cache.get(cache_key)
    if value is not None:
        return value
    # Generate value and offer it to the cache.
    value = factory(*args, **kwargs)
    if cache.maxsize:  # handle size 0 cache
        cache[cache_key] = value
    return value


_cache: cachetools.Cache = default_resource_cache()
