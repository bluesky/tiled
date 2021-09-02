"""
The 'internal' cache is available to all Adapters to cache their internal
resources, such as parsed file contents. It is a process-global singleton.
"""
import contextlib
from timeit import default_timer

import cachey
from dask.callbacks import Callback


if __debug__:
    import logging

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setLevel("DEBUG")
    logger.addHandler(handler)


class _NO_CACHE_SENTINEL:
    def __init__(self):
        self.dask_context = contextlib.nullcontext()

    def __repr__(self):
        return "NO_CACHE"


NO_CACHE = _NO_CACHE_SENTINEL()
_data_cache = NO_CACHE


def set_data_cache(cache):
    """
    Set the process-global icache.
    """
    global _data_cache
    _data_cache = cache


def get_data_cache():
    """
    Set the process-global icache.
    """
    return _data_cache


class CacheInProcessMemory:
    def __init__(self, available_bytes):
        self.misses = 0
        self.hits = 0

        def miss(key):
            self.misses += 1
            if __debug__:
                logger.info("Internal cache miss %r", key)

        def hit(key, value):
            self.hits += 1
            if __debug__:
                logger.info("Internal cache hit %r", key)

        self._cache = cachey.Cache(available_bytes, 0, miss=miss, hit=hit)
        self.dask_context = DaskCache(self._cache)

    def get(self, key):
        value = self._cache.get(key)
        return value

    def put(self, key, value, cost):
        self._cache.put(key, value, cost)

    def retire(self, key):
        return self._cache.retire(key)

    def clear(self):
        return self._cache.clear()

    def __contains__(self, key):
        return key in self._cache


class DaskCache(Callback):
    """
    Adapted from dask.cache

    Changes:
    - Scope keys under prefix 'dask-' to avoid collisions with non-dask cache usage
    - Use a simpler cost computation: duration (in seconds)
    """

    def __init__(self, cache):
        self._nbytes = cachey.nbytes
        self.cache = cache
        self.starttimes = dict()

    def _start(self, dsk):
        "Patched as noted in comment below"
        self.durations = dict()
        for key in dsk:
            # Use 'get', not cache.data[key] as upstream does,
            # in order to trip 'hit' and 'miss' callbacks.
            cached_result = self.cache.get(f"dask-{key}")
            if cached_result is not None:
                dsk[key] = cached_result

    def _pretask(self, key, dsk, state):
        self.starttimes[key] = default_timer()

    def _posttask(self, key, value, dsk, state, id):
        duration = default_timer() - self.starttimes[key]
        deps = state["dependencies"][key]
        if deps:
            duration += max(self.durations.get(k, 0) for k in deps)
        self.durations[key] = duration
        nb = self._nbytes(value)
        self.cache.put(f"dask-{'-'.join(key)}", value, cost=duration, nbytes=nb)

    def _finish(self, dsk, state, errored):
        self.starttimes.clear()
        self.durations.clear()
