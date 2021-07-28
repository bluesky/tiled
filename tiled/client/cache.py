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
import collections.abc
import functools
import hashlib
from math import log
from pathlib import Path
import threading

from heapdict import heapdict


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


class Reservation:
    """
    This represents a reservation on a cached piece of content.

    The content will not be evicted from the cache or updated
    until the content is loaded using `load_content()` or released
    using `ensure_released()`.
    """

    def __init__(self, url, etag, lock, load_content):
        self.url = url
        self.etag = etag
        self._lock = lock
        self._lock_held = True
        self._load_content = load_content
        lock.acquire()

    def load_content(self):
        "Return the content and release the reservation."
        content = self._load_content()
        self._lock.release()
        return content

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
    def in_memory(cls, available_bytes, *, error_if_full=False, scorer=None):
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
        error_if_full : boolean, optional
            By default, the cache starts evicting the least-used items when
            it fills up. This is generally fine when working with a
            connection to the server. But if the goal is to cache for
            *offline* use, it is better to be notified by and error that the
            cache is full. Then the user can respond by increasing
            available_bytes, using a different storage volume for the cache,
            or choosing to a different (smaller) set of entries to download.
        scorer : Scorer
            Determines which items to evict from the cache when it grows full.
            See tiled.client.cache.Scorer for example.
        """
        return cls(
            available_bytes,
            url_to_etag_cache={},
            etag_to_content_cache={},
            sizes={},
            global_lock=threading.Lock(),
            lock_factory=lambda etag: threading.Lock(),
            error_if_full=error_if_full,
            scorer=scorer,
        )

    @classmethod
    def on_disk(
        cls,
        path,
        available_bytes=None,
        *,
        cull_on_startup=False,
        error_if_full=False,
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
        error_if_full : boolean, optional
            By default, the cache starts evicting the least-used items when
            it fills up. This is generally fine when working with a
            connection to the server. But if the goal is to cache for
            *offline* use, it is better to be notified by and error that the
            cache is full. Then the user can respond by increasing
            available_bytes, using a different storage volume for the cache,
            or choosing to a different (smaller) set of entries to download.
        scorer : Scorer
            Determines which items to evict from the cache when it grows full.
            See tiled.client.cache.Scorer for example.
        """
        import locket
        import shutil

        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        if available_bytes is None:
            # By default, use (X - 1 GB) where X is the current free space
            # on the volume containing `path`.
            available_bytes = shutil.disk_usage(path).free - 1e9
        # Get the nbytes for each object in the cache if it is not empty.
        # The FileBasedCache provides a `sizes` property that computes this
        # using stat(). That is, we do not actually read the content into
        # memory at this point.
        etag_to_content_cache = FileBasedCache(path / "etag_to_content_cache")
        sizes = etag_to_content_cache.sizes
        total_size = sum(sizes.values())
        if total_size > available_bytes:
            if not cull_on_startup:
                raise AlreadyTooLarge(
                    f"The cache directory is already {total_size} bytes which is greater "
                    f"that the specified available_bytes {available_bytes}. To delete items "
                    "from the cache until it fits, set cull_on_startup=True."
                )
        instance = cls(
            available_bytes,
            url_to_etag_cache=FileBasedCache(path / "url_to_etag_cache"),
            etag_to_content_cache=etag_to_content_cache,
            sizes=sizes,
            global_lock=locket.lock_file(path / "global.lock"),
            lock_factory=lambda etag: locket.lock_file(
                path / "etag_to_content_cache" / f"{etag}.lock"
            ),
            error_if_full=error_if_full,
            scorer=scorer,
        )
        # Ensure we fit in available_bytes.
        instance.shrink()
        return instance

    def __init__(
        self,
        available_bytes,
        *,
        url_to_etag_cache,
        etag_to_content_cache,
        sizes,
        global_lock,
        lock_factory,
        error_if_full=False,
        scorer=None,
    ):
        """
        Parameters
        ----------

        available_bytes : int
            The number of bytes of data to keep in the cache
        url_to_etag_cache : MutableMapping
            Dict-like object to use for cache
        etag_to_content_cache : MutableMapping
            Dict-like object to use for cache
        sizes : dict
            Byte size of each item in the etag_to_content_cache.
        global_lock : Lock
            A lock used for the url_to_etag_cache
        lock_factory : callable
            Expected signature: ``f(etag) -> Lock``
        error_if_full : boolean, optional
            By default, the cache starts evicting the least-used items when
            it fills up. This is generally fine when working with a
            connection to the server. But if the goal is to cache for
            *offline* use, it is better to be notified by and error that the
            cache is full. Then the user can respond by increasing
            available_bytes, using a different storage volume for the cache,
            or choosing to a different (smaller) set of entries to download.
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
        self.etag_lock = LockDict.from_lock_factory(lock_factory)
        self.url_to_etag_lock = global_lock
        self.error_if_full = error_if_full
        # If the cache has data in it, initialize the internal caches.
        for etag in etag_to_content_cache:
            nbytes = sizes[etag]
            score = self.scorer.touch(etag, nbytes)
            self.heap[etag] = score
            self.nbytes[etag] = nbytes
            self.total_bytes += nbytes

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
            self.url_to_etag_cache[key] = etag.encode()

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

    def get_reservation(self, url):
        # Hold the global lock.
        with self.url_to_etag_lock:
            etag_bytes = self.url_to_etag_cache.get(tokenize_url(url))
            if etag_bytes is None:
                # We have nothing for this URL.
                return None
            etag = etag_bytes.decode()
            # Acquire a targeted lock, and then release and the global lock.
            lock = self.etag_lock[etag]
        return Reservation(
            url, etag, lock, functools.partial(self._get_content_for_etag, etag)
        )

    def _get_content_for_etag(self, etag):
        if etag in self.etag_to_content_cache:
            value = self.etag_to_content_cache[etag]
            # Access this item increases its score.
            nbytes = len(value)
            score = self.scorer.touch(etag, nbytes)
            self.heap[etag] = score
            return value

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
    return hashlib.md5(
        b"".join(
            [
                url[0],
                url[1],
                f":{url[2]}".encode(),  # e.g. 8000 -> b'8000'
                *url[3:],
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

    @property
    def sizes(self):
        return {
            path.relative_to(self._directory).parts: path.stat().st_size
            for path in self._directory.iterdir()
        }

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
            yield path.relative_to(self._directory).parts

    def __contains__(self, key):
        path = Path(self._directory, *_normalize(key))
        return path.is_file()


def _normalize(key):
    if isinstance(key, str):
        return (key,)
    return key


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


class AlreadyTooLarge(Exception):
    pass


class CacheIsFull(Exception):
    pass
