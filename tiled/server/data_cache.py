"""
The 'data' cache is available to all Adapters to cache chunks of data.

This is integrated with dask's callback mechanism for simple caching of dask
chunks. It is also available for usage outside of dask.

The cache is a process-global singleton.
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
    handler.setFormatter(logging.Formatter("DATA CACHE: %(message)s"))
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
                logger.debug("Miss %r", key)

        def hit(key, value):
            self.hits += 1
            if __debug__:
                logger.debug("Hit %r", key)

        self._cache = cachey.Cache(available_bytes, 0, miss=miss, hit=hit)
        self._dask_context = DaskCache(self)

    @property
    def dask_context(self):
        """
        Within this context, get and store dask tasks with the data cache.
        """
        return self._dask_context

    @property
    def available_bytes(self):
        """
        Maximum size in bytes
        """
        return self._cache.available_bytes

    def get(self, key):
        """
        Get cache item.
        """
        value = self._cache.get(key)
        return value

    def put(self, key, value, cost, nbytes=None):
        """
        Put cache item.

        Parameters
        ----------
        key : uniquely identifies content
        value : object
            May be any Python object. For future-proofing, the object should be
            pickle-able, as an _external_ data cache will be added in the future.
        cost : float
            Time in seconds that this value cost to obtain.
        nbytes : bytesize, optional
            Computed (with best effort) if not provided.
        """
        if nbytes is None:
            nbytes = self._cache.get_nbytes(value)
        logger.debug("Store %r (cost=%.3f, nbytes=%d)", key, cost, nbytes)
        self._cache.put(key, value, cost, nbytes=nbytes)

    def discard(self, *keys):
        """
        Discard one or more items from the cache if present.
        """
        for key in keys:
            # Cachey has no API specifically for this, but we can do it ourselves
            # but modifying only public state.
            value = self._cache.data.pop(key, None)
            if value is not None:
                self._cache.total_bytes -= self._cache.nbytes.pop(key)

    def discard_dask(self, *keys):
        """
        Discard one or more dask tasks from the cache, if present.

        Internally, cached dask keys are prefixed to avoid collisions.
        That is why it has a method separate from discard().
        """
        self.discard(("dask", key) for key in keys)

    def clear(self):
        """
        Empty the cache.
        """
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
            cached_result = self.cache.get(("dask", *key))
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
        self.cache.put(("dask", *key), value, cost=duration, nbytes=nb)

    def _finish(self, dsk, state, errored):
        for key in dsk:
            self.starttimes.pop(key, None)
            self.durations.pop(key, None)
