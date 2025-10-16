import os
from pathlib import Path
from typing import Any, Callable, Optional, Tuple

import cachetools

# When items are evicted from the cache, a hard reference is dropped, freeing
# the resource to be closed by the garbage collector if there are no other
# extant hard references. Items are evicted if:
#
# - They have been in the cache for a _total_ of more than a given time.
#   (Accessing an item does not reset this time.)
# - The cache is at capacity and this item is the least recently used item.
#
# The "size" is measured in cached items; that is, each item in the cache has
# size 1.
DEFAULT_MAX_SIZE = int(os.getenv("TILED_RESOURCE_CACHE_MAX_SIZE", "1024"))
DEFAULT_TIME_TO_USE_SECONDS = float(os.getenv("TILED_RESOURCE_CACHE_TTU", "60."))

AnyCache = cachetools.Cache[Tuple[Any, Path], Any]


def get_resource_cache() -> AnyCache:
    "Return resource cache, a process-global Cache."
    return _cache


def set_resource_cache(cache: AnyCache) -> None:
    """
    Set the resource cache, a process-global Cache.
    Parameters
    ----------
    cache :

    Returns
    -------

    """
    global _cache
    _cache = cache


def default_ttu(_key: str, value: Any, now: float) -> float:
    """
    Retain cached items for at most `DEFAULT_TIME_TO_USE_SECONDS` seconds (60s, by default).

    Parameters
    ----------
    _key :
    value :
    now :

    Returns
    -------

    """
    return DEFAULT_TIME_TO_USE_SECONDS + now


def default_resource_cache() -> cachetools.TLRUCache[Any, Any]:
    "Create a new instance of the default resource cache."
    return cachetools.TLRUCache(DEFAULT_MAX_SIZE, default_ttu)


def with_resource_cache(
    cache_key: Tuple[Any, Path],
    factory: Callable[..., Any],
    *args: Any,
    _resource_cache: Optional[AnyCache] = None,
    **kwargs: Any,
) -> Any:
    """
    Use value from cache or, if not present, call `factory(*args, **kwargs)` and cache result.

    This uses a globally configured resource cache by default. For testing and
    debugging, a cache may be passed to the parameter _resource_cache.

    Parameters
    ----------
    cache_key :
    factory :
    args :
    _resource_cache :
    kwargs :

    Returns
    -------

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


_cache: AnyCache = default_resource_cache()
