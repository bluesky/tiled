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
import urllib.parse

from heapdict import heapdict


class ReadOnlyCache:
    @classmethod
    def on_disk(cls, path):
        "An on-disk cache of data from the server"
        raise NotImplementedError("Work in progress...")
        return cls(..., ...)

    def __init__(self, url_to_etag_cache, etag_to_content_cache):
        """
        Parameters
        ----------

        url_to_etag_cache : MutableMapping
            Dict-like object to use for cache
        etag_to_content_cache : MutableMapping
            Dict-like object to use for cache
        """

        self.url_to_etag_cache = url_to_etag_cache
        self.etag_to_content_cache = etag_to_content_cache

    def get_etag_for_url(self, url, default=None):
        # Return (etag, None) so that a writable cache's return value,
        # which is (etag, RLock), is a drop-in replacement.
        return self.url_to_etag_cache.get(tokenize_url(url), default), None

    def get_content_for_etag(self, etag, default=None):
        return self.etag_to_content_cache.get(etag, default)


class Cache:
    """
    A client-side cache of data from the server.

    The __init__ is to be used internally and by authors of custom caches.
    See Cache.in_memory() and Cache.on_disk() for user-facing methods..
    """

    @classmethod
    def in_memory(cls, available_bytes, scorer=None):
        "An in-memory cache of data from the server"
        return cls(available_bytes, {}, {}, scorer)

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

    def read_only(self):
        return ReadOnlyCache(self.url_to_etag_cache, self.etag_to_content_cache)

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
        self.url_to_etag_lock = RLock()

    def put_etag_for_url(self, url, etag):
        key = tokenize_url(url)
        previous_etag = self.url_to_etag_cache.get(key)
        if previous_etag:
            self.etag_refcount[previous_etag] -= 1
            if self.etag_refcount[previous_etag] == 0:
                # All URLs that referred to this content have since
                # changed their ETags, so we can forget about this content.
                self.retire(previous_etag)
        with self.url_to_etag_lock:
            self.url_to_etag_cache[key] = etag

    def put_content(self, etag, content):
        nbytes = len(content)
        if nbytes < self.available_bytes:
            score = self.scorer.touch(etag, nbytes)
            if (
                nbytes + self.total_bytes < self.available_bytes
                or not self.heap
                or score > self.heap.peekitem()[1]
            ):
                self.etag_to_content_cache[etag] = content
                self.heap[etag] = score
                self.nbytes[etag] = nbytes
                self.total_bytes += nbytes
                # TODO We should actually shrink *first* to stay below the available_bytes.
                self.shrink()

    def get_etag_for_url(self, url, default=None):
        # Hold the global lock.
        with self.url_to_etag_lock:
            etag = self.url_to_etag_cache.get(tokenize_url(url), default)
            # Acquire a targeted lock, and then release and the global lock.
            lock = self.etag_lock[etag]
            lock.acquire()
        return etag, lock

    def get_content_for_etag(self, etag, default=None):
        # Access this item increases its score.
        score = self.scorer.touch(etag)
        if etag in self.etag_to_content_cache:
            value = self.etag_to_content_cache[etag]
            self.heap[etag] = score
            return value
        else:
            return default

    def retire(self, etag):
        """Retire/remove a etag from the cache

        See Also:
            shrink
        """
        lock = self.etag_lock[etag]
        with lock:
            self.etag_to_content_cache.pop(etag)
            self.total_bytes -= self.nbytes.pop(etag)
            self.etag_lock.pop(etag)

    def _shrink_one(self):
        if self.heap.heap:
            # Retire the lowest-priority item that isn't locked.
            for score, etag, _ in self.heap.heap:
                if self.etag_lock[etag].acquire(blocking=False):
                    self.heap.pop(etag)
                    self.retire(etag)
                    break

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
        while self.etag_to_content_cache:
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
    """
    >>> tokenize_url((b"https", b"localhost", 8000, b"/metadata/"))
    (b"https", b"localhost", b"8000", b"%2Fmetadata%2F")
    """
    return (
        url[:2]
        + (str(url[2]).encode(),)
        + tuple(urllib.parse.quote_plus(segment) for segment in url[3:])
    )
