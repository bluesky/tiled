from typing import Callable, Optional

import cachetools

# Cached items will be evicted if not used for a specified time interval
# ("time to use"). If the cache size reaches its max size, the least recently
# used cache item will be evicted.
DEFAULT_MAX_SIZE = 1024
DEFAULT_TIME_TO_USE_SECONDS = 60

_cache = None


def get_resource_cache():
    global _cache
    if _cache is None:
        cache = default_resource_cache()
        set_resource_cache(cache)
    return _cache


def set_resource_cache(cache):
    global _cache
    _cache = cache


def default_ttu(_key, value, now):
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
    cache[cache_key] = value
    return value
