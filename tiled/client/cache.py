"""
This module includes objects inspired by https://github.com/dask/cachey/

We opted for an independent implementation because reusing cachey would have required:

* An invasive subclass that could be a bit fragile
* And also composition in order to get the public API we want
* Carrying around some complexity/features that we do not use here

The original cachey license (which, like Tiled's, is 3-clause BSD) is included in
the same source directory as this module.
"""
import collections.abc
import functools
import hashlib
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from math import log
from pathlib import Path

from heapdict import heapdict
from httpx import Headers

if __debug__:
    from .utils import logger


class UrlItem(
    collections.namedtuple(
        "UrlItem",
        [
            "pinned",
            "size",
            "media_type",
            "encoding",
            "etag",
            "must_revalidate",
            "expires",
        ],
    )
):
    """
    An item in the cache mapping URLs to ETags (with other Headers info)
    """

    @classmethod
    def from_headers(cls, headers):
        expires_str = headers.get("expires")
        if expires_str is not None:
            expires = datetime.strptime(expires_str, HTTP_EXPIRES_HEADER_FORMAT)
        else:
            expires = None
        return cls(
            pinned=bool(int(headers.pop("__tiled_client_pinned__", "0"))),
            size=int(headers["content-length"]),
            media_type=headers["content-type"],
            encoding=headers.get("content-encoding"),
            etag=headers["etag"],
            must_revalidate="must-revalidate" in headers.get("cache-control", ""),
            expires=expires,
        )

    @classmethod
    def from_text(cls, text):
        headers = Headers()
        for line in text.splitlines():
            k, v = line.split(": ", 1)
            headers[k] = v
        return cls.from_headers(headers)

    def to_text(self):
        headers = {
            "__tiled_client_pinned__": str(
                int(self.pinned)
            ),  # "0" or "1" for False or True
            "content-length": str(self.size),
            "content-type": self.media_type,
            "etag": self.etag,
        }
        if self.must_revalidate:
            headers["cache-control"] = "must-revalidate"
        if self.expires is not None:
            headers["expires"] = self.expires.strftime(HTTP_EXPIRES_HEADER_FORMAT)
        if self.encoding is not None:
            headers["content-encoding"] = self.encoding
        return "\n".join(f"{k}: {v}" for k, v in headers.items())


def download(*entries):
    """
    Download a local cache for Tiled so access is fast and can work offline.

    Parameters
    ----------
    *entries : Node(s) or structure client(s)

    Examples
    --------

    Connect a tree and download it in its entirety.

    >>> from tiled.client import from_uri
    >>> from tiled.client.cache import download, Cache
    >>> client = from_uri("http://...", cache=Cache.on_disk("path/to/directory"))
    >>> download(client)

    Alternatively ,this can be done from the commandline via the tiled CLI:

    $ tiled download "http://..." my_cache_direcotry

    Use the local copy for faster data access. Tiled will connect to server just
    to verify that the local copy is current, and only download data if there have
    been changes to the copy of the server.

    >>> from tiled.client import from_uri
    >>> from tiled.client.cache import Cache
    >>> client = from_uri("http://...")
    >>> client = from_uri("http://...", cache=Cache.on_disk("my_cache_directory"))

    If network is unavailable or very slow, rely on the local copy entirely. Tiled
    will not connect to the server. (Note that you still need to provide a URL,
    but it is only used to contruct the names of files in the local directory.)

    >>> from tiled.client import from_uri
    >>> from tiled.client.cache import Cache
    >>> client = from_uri("http://...", cache=Cache.on_disk("my_cache_directory"), offline=True)
    """
    # TODO Use multiple processes to ensure we are saturating our network connection.
    for entry in entries:
        entry.touch()


# This is a silly time format, but it is the HTTP standard.
HTTP_EXPIRES_HEADER_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
UNIT_SECOND = timedelta(seconds=1)
ZERO_SECONDS = timedelta(seconds=0)


def _round_seconds(dt):
    return round(dt / UNIT_SECOND)


class Reservation:
    """
    This represents a reservation on a cached piece of content.

    The content will not be evicted from the cache or updated
    until the content is loaded using `load_content()` or released
    using `ensure_released()`.
    """

    def __init__(self, url, item, renew, lock, load_content):
        self.url = url
        self.item = item
        self._renew = renew
        self._lock = lock
        self._lock_held = True
        self._load_content = load_content
        lock.acquire()

    @property
    def etag(self):
        # This is a relic of a refactor. Might be able to remove this indirection.
        return self.item.etag

    @property
    def expires(self):
        # This is a relic of a refactor. Might be able to remove this indirection.
        return self.item.expires

    def load_content(self):
        "Return the content and release the reservation."
        start = time.perf_counter()
        content = self._load_content()
        duration = 1000 * (time.perf_counter() - start)  # units: ms
        self._lock.release()
        if __debug__:
            # Use _ for thousands separator in bytes.
            logger.debug(
                "Cache read (%s B in %.1f ms) %s",
                f"{len(content):_}",
                duration,
                self.url,
            )
        return content

    def is_stale(self):
        if self.expires is None:
            logger.debug(
                "Cache is stale (no expiration was set) %s",
                self.url,
            )
            return True

        time_remaining = datetime.utcnow() - self.expires
        stale = time_remaining > ZERO_SECONDS
        if __debug__:
            if stale:
                logger.debug(
                    "Cache is stale (%d secs ago) %s",
                    _round_seconds(time_remaining),
                    self.url,
                )
        return stale

    def renew(self, expires):
        self._renew(expires=expires)

    def ensure_released(self):
        "Release the reservation. This is idempotent."
        if self._lock_held:
            try:
                self._lock.release()
                # TODO Investigate why this is sometimes released twice.
            except (AttributeError, RuntimeError):
                pass
                self._lock_held = False


class Cache:
    """
    A client-side cache of data from the server.

    The __init__ is to be used internally and by authors of custom caches.
    See ``Cache.in_memory()`` and ``Cache.on_disk()`` for user-facing methods.

    This is used by the function ``tiled.client.utils.get_content_with_cache``.
    """

    @classmethod
    def in_memory(cls, available_bytes, *, scorer=None):
        """
        An in-memory cache of data from the server

        This is useful to ensure that data is not downloaded repeatedly
        unless it has been updated since the last download.

        Because it is in memory, it only applies to a given Python process,
        i.e. a given working session. See ``Cache.on_disk()`` for a
        cache that can be shared across process and persistent for future
        sessions.

        Parameters
        ----------
        available_bytes : integer
            e.g. 2e9 to use up to 2 GB of RAM
        scorer : Scorer
            Determines which items to evict from the cache when it grows full.
            See tiled.client.cache.Scorer for example.
        """
        return cls(
            available_bytes,
            url_to_headers_cache={},
            etag_to_content_cache={},
            global_lock=threading.Lock(),
            lock_factory=lambda etag: threading.Lock(),
            scorer=scorer,
        )

    @classmethod
    def on_disk(
        cls,
        path,
        available_bytes=None,
        *,
        cull_on_startup=False,
        scorer=None,
    ):
        """
        An on-disk cache of data from the server

        This is useful to ensure that data is not downloaded repeatedly
        unless it has been updated since the last download.

        This uses file-based locking to ensure consistency when the cache
        is shared by multiple processes.

        Parameters
        ----------
        path : Path or str
            A directory will be created at this path if it does not yet exist.
            It is safe to reuse an existing cache directory and to share a cache
            directory between multiple processes.
        available_bytes : integer, optional
            e.g. 2e9 to use up to 2 GB of disk space. If None, this will consume
            up to (X - 1 GB) where X is the free space remaining on the volume
            containing `path`.
        cull_on_startup : boolean, optional
            If reusing an existing cache directory which is already larger than the
            available_bytes, an error is raised. Set this to True to delete
            items from the cache until it fits in available_bytes. False by default.
        scorer : Scorer
            Determines which items to evict from the cache when it grows full.
            See tiled.client.cache.Scorer for example.
        """
        import shutil

        import locket

        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        if available_bytes is None:
            # By default, use (X - 1 GB) where X is the current free space
            # on the volume containing `path`.
            available_bytes = shutil.disk_usage(path).free - 1e9
        etag_to_content_cache = FileBasedCache(path / "etag_to_content_cache")
        instance = cls(
            available_bytes,
            url_to_headers_cache=FileBasedUrlCache(path / "url_to_headers_cache"),
            etag_to_content_cache=etag_to_content_cache,
            global_lock=locket.lock_file(path / "global.lock"),
            lock_factory=lambda etag: locket.lock_file(
                path / "etag_to_content_cache" / f"{etag}.lock"
            ),
            scorer=scorer,
        )
        # Ensure we fit in available_bytes.
        instance.shrink()
        return instance

    def __init__(
        self,
        available_bytes,
        *,
        url_to_headers_cache,
        etag_to_content_cache,
        global_lock,
        lock_factory,
        scorer=None,
    ):
        """
        Parameters
        ----------

        available_bytes : int
            The number of bytes of data to keep in the cache
        url_to_headers_cache : MutableMapping
            Dict-like object to use for cache
        etag_to_content_cache : MutableMapping
            Dict-like object to use for cache
        global_lock : Lock
            A lock used for the url_to_headers_cache
        lock_factory : callable
            Expected signature: ``f(etag) -> Lock``
        scorer: Scorer, optional
            A Scorer object that controls how we decide what to retire when space
            is low.
        """

        if scorer is None:
            scorer = Scorer(halflife=1000)
        self.scorer = scorer
        self.available_bytes = available_bytes
        self.pinned_bytes = 0
        self.heap = heapdict()
        self.nbytes = dict()
        self.total_bytes = 0
        self.url_to_headers_cache = url_to_headers_cache
        self.etag_to_content_cache = etag_to_content_cache
        self.etag_refcount = defaultdict(lambda: 0)
        self.etag_lock = LockDict.from_lock_factory(lock_factory)
        self.url_to_headers_lock = global_lock
        # If the cache has data in it, initialize the internal caches.
        for etag in etag_to_content_cache:
            # This tells us the content size without actually reading in the data.
            nbytes = etag_to_content_cache.sizeof(etag)
            score = self.scorer.touch(etag, nbytes)
            self.heap[etag] = score
            self.nbytes[etag] = nbytes
            self.total_bytes += nbytes

    def renew(self, url, etag, expires):
        cache_key = tokenize_url(url)
        if expires is None:
            # Do not renew.
            return
        with self.url_to_headers_lock:
            item = self.url_to_headers_cache[cache_key]
            assert item.etag == etag
            # TO DO We end up going str -> datetime -> str here.
            # It may be worth adding a fast path.
            expires_dt = datetime.strptime(expires, HTTP_EXPIRES_HEADER_FORMAT)
            updated_item = item._replace(expires=expires_dt)
            self.url_to_headers_cache[cache_key] = updated_item
        if __debug__:
            logger.debug(
                "Cache renewed %s for %d secs.",
                url,
                _round_seconds(expires_dt - datetime.utcnow()),
            )

    def put(self, url, headers, content):
        cache_key = tokenize_url(url)
        cached = self.url_to_headers_cache.get(cache_key)
        if cached:
            previous_etag = cached.etag
            self.etag_refcount[previous_etag] -= 1
            if self.etag_refcount[previous_etag] == 0:
                # All URLs that referred to this content have since
                # changed their ETags, so we can forget about this content.
                self.retire(previous_etag)
        item = UrlItem.from_headers(headers)
        start = time.perf_counter()
        with self.url_to_headers_lock:
            self.url_to_headers_cache[cache_key] = item
        nbytes = self._put_content(item.etag, content)
        duration = 1000 * (time.perf_counter() - start)  # units: ms
        if __debug__:
            if nbytes:
                if item.expires is not None:
                    logger.debug(
                        "Cache stored (%s B in %.1f ms) %s. Renew after %d secs.",
                        f"{nbytes:_}",  # Use _ for thousands separator.
                        duration,
                        url,
                        _round_seconds(item.expires - datetime.utcnow()),
                    )
                else:
                    logger.debug(
                        "Cache stored (%s B in %.1f ms) %s. Renew on next access.",
                        f"{nbytes:_}",  # Use _ for thousands separator.
                        duration,
                        url,
                    )
            else:
                logger.debug(
                    "Cache delined to store %s given its cost/benefit score.", url
                )

    def _put_content(self, etag, content):
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
                return nbytes

    def get_reservation(self, url):
        # Hold the global lock.
        with self.url_to_headers_lock:
            cached = self.url_to_headers_cache.get(tokenize_url(url))
            if cached is None:
                # We have nothing for this URL.
                return None
            # Acquire a targeted lock, and then release and the global lock.
            lock = self.etag_lock[cached.etag]
        return Reservation(
            url,
            cached,
            functools.partial(self.renew, url, cached.etag),
            lock,
            functools.partial(self._get_content_for_etag, cached.etag),
        )

    def _get_content_for_etag(self, etag):
        try:
            content = self.etag_to_content_cache[etag]
            # Access this item increases its score.
            score = self.scorer.touch(etag, len(content))
            self.heap[etag] = score
            return content
        except KeyError:
            return None

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
                lock = self.etag_lock[etag]
                if lock.acquire(blocking=False):
                    try:
                        self.heap.pop(etag)
                        self.etag_to_content_cache.pop(etag)
                        self.total_bytes -= self.nbytes.pop(etag)
                        self.etag_lock.pop(etag)
                    finally:
                        lock.release()
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

        if self._error_if_full:
            raise CacheIsFull(
                f"""All {self.available_bytes} are used. Options:
1. Set larger available_bytes (and if necessary a different storage volume with more room).
2. Choose a smaller set of entries to cache.
3. Allow the cache to evict items that do not fit by setting error_if_full=False.
"""
            )

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

    Examples
    --------

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
    "some unique hash"
    """
    url_as_tuple = url.raw
    return hashlib.md5(
        b"".join(
            [
                url_as_tuple[0],
                url_as_tuple[1],
                f":{url_as_tuple[2]}".encode(),  # e.g. 8000 -> b'8000'
                *url_as_tuple[3:],
            ]
        )
    ).hexdigest()


class FileBasedCache(collections.abc.MutableMapping):
    "Locking is handled in the other layer, by Cache."

    def __init__(self, directory, mode="w"):
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        if mode == "w":
            # Record the version which may be useful to future code that needs
            # to deal with a change in internal format or layout.
            with open(self._directory / "client_library_version", "wt") as file:
                from tiled import __version__

                file.write(__version__)

    def __repr__(self):
        return repr(dict(self))

    @property
    def directory(self):
        return self._directory

    def sizeof(self, key):
        path = Path(self._directory, *_normalize(key))
        return path.stat().st_size

    def __getitem__(self, key):
        path = Path(self._directory, *_normalize(key))
        if not path.is_file():
            raise KeyError(key)
        with open(path, "rb") as file:
            return file.read()

    def __setitem__(self, key, value):
        path = Path(self._directory, *_normalize(key))
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as file:
            file.write(value)

    def __delitem__(self, key):
        path = Path(self._directory, *_normalize(key))
        path.unlink()

    def __len__(self):
        return len(list(self._directory.iterdir()))

    def __iter__(self):
        for path in self._directory.iterdir():
            parts = path.relative_to(self._directory).parts
            if len(parts) == 1:
                # top-level metadata like "client_library_version"
                continue
            yield _unnormalize(parts)

    def __contains__(self, key):
        path = Path(self._directory, *_normalize(key))
        return path.is_file()


def _normalize(*key):
    # To avoid an overly large directory (which leads to slow performance)
    # divide into subdirectories beginning with the first two characters of
    # the contents' name.
    return (key[0][:2],) + key


def _unnormalize(key):
    return [key][1]


class LockDict(dict):
    @classmethod
    def from_lock_factory(cls, lock_factory):
        instance = cls()
        instance._lock_factory = lock_factory
        return instance

    def __missing__(self, key):
        value = self._lock_factory(key)
        self[key] = value
        return value


class FileBasedUrlCache(FileBasedCache):
    def __getitem__(self, key):
        data = super().__getitem__(key)
        return UrlItem.from_text(data.decode())

    def __setitem__(self, key, value):
        data = value.to_text().encode()
        super().__setitem__(key, data)


class AlreadyTooLarge(Exception):
    pass


class CacheIsFull(Exception):
    pass
