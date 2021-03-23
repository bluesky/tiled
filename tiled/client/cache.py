"""
This module includes objects inspired by https://github.com/dask/cachey/

We opted for an independent implementation because reusing cachey would have required:

* An invasive subclass that could be a bit fragile
* And also composition in order to get the public API we want
* Carrying around some complexity/features that we do not use here

The original cachey license (which, like Tiled's, is 3-clause BSD) is included in
the same source directory as this module.
"""
from collections import defaultdict
from math import log
from threading import RLock
import time
import urllib.parse

from heapdict import heapdict


class Cache:
    """
    A client-side cache of data from the server.

    The __init__ is to be used internally and by authors of custom caches.
    See Cache.in_memory() and Cache.on_disk() for user-facing methods..
    """

    @classmethod
    def in_memory(cls, available_bytes, scorer=None):
        "An in-memory cache of data from the server"
        return cls(available_bytes, scorer, {}, {})

    @classmethod
    def on_disk(cls, available_bytes, path, scorer=None):
        "An on-disk cache of data from the server"
        raise NotImplementedError("Work in progress...")
        # TODO Make file-backed mutable mappings.
        # Consider using zict.File.
        # Consider using locket for file-based locking.
        # Record the client library version somewhere so we can
        # deal with migrating caches across upgrades if needed.
        return cls(available_bytes, scorer, ..., ...)

    def __init__(
        self, available_bytes, url_to_etag_cache, etag_to_content_cache, scorer=None
    ):
        """
        Parameters
        ----------

        available_bytes: int
            The number of bytes of data to keep in the cache
        url_to_etag_cache : MutableMapping
            Dict-like object to use for cache
        etag_to_content_cache : MutableMapping
            Dict-like object to use for cache
        scorer: Scorer, optional
            A Scorer object that controls how we decide what to retire when space
            is low.
        """

        if scorer is None:
            scorer = Scorer(halflife=1000)
        self.scorer = scorer
        self.available_bytes = available_bytes
        self.heap = heapdict()
        self.nbytes = dict()
        self.total_bytes = 0
        self.url_to_etag_cache = url_to_etag_cache
        self.etag_to_content_cache = etag_to_content_cache
        self.etag_refcount = defaultdict(lambda: 0)
        self.etag_lock = defaultdict(RLock)

    def get_etag_for_url(self, url):
        return self.url_to_etag_cache.get(tokenize_url(url))
    
    def get_content_for_etag(self, etag):
        return self.etag_to_content_cache.get(etag)

    def put(self, key, value, cost):
        nbytes = len(value)
        if cost >= self.limit and nbytes < self.available_bytes:
            score = self.scorer.touch(key, cost)
            if (
                nbytes + self.total_bytes < self.available_bytes
                or not self.heap
                or score > self.heap.peekitem()[1]
            ):
                self.data[key] = value
                self.heap[key] = score
                self.nbytes[key] = nbytes
                self.total_bytes += nbytes
                self.shrink()

    def get(self, key, default=None):
        """Get value associated with key.  Returns None if not present

        >>> c = Cache(1e9, 10)
        >>> c.put('x', 10, cost=50)
        >>> c.get('x')
        10
        """
        score = self.scorer.touch(key)
        if key in self.data:
            value = self.data[key]
            if self.hit is not None:
                self.hit(key, value)
            self.heap[key] = score
            return value
        else:
            if self.miss is not None:
                self.miss(key)
            return default

    def retire(self, key):
        """Retire/remove a key from the cache

        See Also:
            shrink
        """
        with self.etag_lock[key]:
            val = self.data.pop(key)
            self.total_bytes -= self.nbytes.pop(key)

    def _shrink_one(self):
        try:
            key, score = self.heap.popitem()
        except IndexError:
            return
        self.retire(key)

    def resize(self, available_bytes):
        """Resize the cache.

        Will fit the cache into available_bytes by calling `shrink()`.
        """
        self.available_bytes = available_bytes
        self.shrink()

    def shrink(self):
        """Retire keys from the cache until we're under bytes budget

        See Also:
            retire
        """
        if self.total_bytes <= self.available_bytes:
            return

        while self.total_bytes > self.available_bytes:
            self._shrink_one()

    def clear(self):
        while self.data:
            self._shrink_one()


class Scorer:
    """
    Object to track scores of cache

    Prefers items that have the following properties:

    1.  Expensive to download (bytes)
    3.  Frequently used
    4.  Recently used

    This object tracks both stated costs of keys and a separate score related
    to how frequently/recently they have been accessed.  It uses these to to
    provide a score for the key used by the ``Cache`` object, which is the main
    usable object.

    Example
    -------
    >>> s = Scorer(halflife=10)
    >>> s.touch('x', cost=2)  # score is similar to cost
    2
    >>> s.touch('x')  # scores increase on every touch
    4.138629436111989
    """

    def __init__(self, halflife):
        self.cost = dict()
        self.time = defaultdict(lambda: 0)

        self._base_multiplier = 1 + log(2) / float(halflife)
        self.tick = 1
        self._base = 1

    def touch(self, key, cost=None):
        """Update score for key
        Provide a cost the first time and optionally thereafter.
        """
        time = self._base

        if cost is not None:
            self.cost[key] = cost
            self.time[key] += self._base
            time = self.time[key]
        else:
            try:
                cost = self.cost[key]
                self.time[key] += self._base
                time = self.time[key]
            except KeyError:
                return

        self._base *= self._base_multiplier
        return cost * time


def tokenize_url(url):
    # TODO This should probably return a tuple of paths (dropping the scheme)
    # which can be used by the on-disk cache to create subdirectories.
    return urllib.parse.quote_plus(term)
