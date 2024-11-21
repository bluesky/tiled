import enum
import json
import os
import sqlite3
import threading
import typing as tp
from contextlib import closing
from datetime import datetime
from functools import wraps
from pathlib import Path

import httpx
import platformdirs

from .utils import SerializableLock, TiledResponse

CACHE_DATABASE_SCHEMA_VERSION = 1


class CachedResponse(TiledResponse):
    pass


def get_cache_key(request: httpx.Request) -> str:
    """Get the cache key from a request.

    The cache key is the str request url.

    Args:
        request: httpx.Request

    Returns:
        str: httpx.Request.url
    """
    return str(request.url)


def get_size(response, content=None):
    # get content or stream_content
    if hasattr(response, "_content"):
        size = len(response.content)
    elif content is None:
        raise httpx.ResponseNotRead()
    else:
        size = len(content)
    return size


def dump(response, content=None):
    # get content or stream_content
    if hasattr(response, "_content"):
        body = response.content
        is_stream = False
    elif content is None:
        raise httpx.ResponseNotRead()
    else:
        body = content
        is_stream = True

    return (
        response.status_code,
        json.dumps(response.headers.multi_items()),
        body,
        is_stream,
        response.encoding or None,
        len(body),
        datetime.now().timestamp(),
        0,  # never yet accessed
    )


def load(row, request=None):
    status_code, headers_json, body, is_stream, encoding = row
    headers = json.loads(headers_json)
    if is_stream:
        content = None
        stream = httpx.ByteStream(body)
    else:
        content = body
        stream = None
    response = CachedResponse(
        status_code, headers=headers, content=content, stream=stream
    )
    if encoding is not None:
        response.encoding = encoding

    if request is not None:
        response.request = request
    return response


def _create_tables(conn):
    with closing(conn.cursor()) as cur:
        cur.execute(
            """CREATE TABLE responses (
cache_key TEXT PRIMARY KEY,
status_code INTEGER,
headers JSON,
body BLOB,
is_stream INTEGER,
encoding TEXT,
size INTEGER,
time_created REAL,
time_last_accessed REAL
)"""
        )
        cur.execute("CREATE TABLE tiled_http_response_cache_version (version INTEGER)")
        cur.execute(
            "INSERT INTO tiled_http_response_cache_version (version) VALUES (?)",
            (CACHE_DATABASE_SCHEMA_VERSION,),
        )
        conn.commit()


def _prepare_database(filepath, readonly):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    if readonly:
        # The methods in Cache will not try to write when in readonly mode.
        # For extra safety we open a readonly connection to the database, so
        # that SQLite itself will prohibit writing.
        conn = sqlite3.connect(
            f"file:{filepath}?mode=ro", uri=True, check_same_thread=False
        )
    else:
        conn = sqlite3.connect(filepath, check_same_thread=False)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    if not tables:
        # We have an empty database.
        _create_tables(conn)
    elif "tiled_http_response_cache_version" not in tables:
        # We have a nonempty database that we do not recognize.
        raise RuntimeError(
            f"Database at {filepath} is not empty and not recognized as a tiled HTTP response cache."
        )
    else:
        # We have a nonempty database that we recognize.
        cursor = conn.execute("SELECT * FROM tiled_http_response_cache_version;")
        (version,) = cursor.fetchone()
        if version != CACHE_DATABASE_SCHEMA_VERSION:
            # It is likely that this cache database will be very stable; we
            # *may* never need to change the schema. But if we do, we will
            # not bother with migrations. The cache is highly disposable.
            # Just silently blow it away and start over.
            Path(filepath).unlink()
            conn = sqlite3.connect(filepath, check_same_thread=False)
            _create_tables(conn)
    return conn


def with_thread_lock(fn):
    """Makes sure the wrapper isn't accessed concurrently."""

    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        obj._lock.acquire()
        try:
            result = fn(obj, *args, **kwargs)
        finally:
            obj._lock.release()
        return result

    return wrapper


class ThreadingMode(enum.IntEnum):
    """Threading mode used in the sqlite3 package.

    https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety

    """

    SINGLE_THREAD = 0
    MULTI_THREAD = 1
    SERIALIZED = 3


class Cache:
    def __init__(
        self,
        filepath=None,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ):
        if filepath is None:
            # Resolve this here, not at module scope, because the test suite
            # injects TILED_CACHE_DIR env var to use a temporary directory.
            TILED_CACHE_DIR = Path(
                os.getenv("TILED_CACHE_DIR", platformdirs.user_cache_dir("tiled"))
            )
            # TODO Detect filesystem of TILED_CACHE_DIR. If it is a networked filesystem
            # use a temporary database instead.
            filepath = TILED_CACHE_DIR / "http_response_cache.db"
        if capacity <= max_item_size:
            raise ValueError("capacity must be greater than max_item_size")
        self._capacity = capacity
        self._max_item_size = max_item_size
        self._readonly = readonly
        self._filepath = filepath
        self._owner_thread = threading.current_thread().ident
        self._conn = _prepare_database(filepath, readonly)
        self._lock = SerializableLock()

    def __repr__(self):
        return f"<{type(self).__name__} {str(self._filepath)!r}>"

    def write_safe(self):
        """Check that it is safe to write.

        SQLite is not threadsafe for concurrent _writes_ unless the
        underlying sqlite library was built with thread safety
        enabled. Even still, it may be a good idea to use a thread
        lock (``@with_thread_lock``) to prevent parallel writes.

        """
        is_main_thread = threading.current_thread().ident == self._owner_thread
        sqlite_is_safe = sqlite3.threadsafety == ThreadingMode.SERIALIZED
        return is_main_thread or sqlite_is_safe

    def __getstate__(self):
        return (
            self.filepath,
            self.capacity,
            self.max_item_size,
            self._readonly,
            self._lock,
        )

    def __setstate__(self, state):
        (filepath, capacity, max_item_size, readonly, lock) = state
        self._capacity = capacity
        self._max_item_size = max_item_size
        self._readonly = readonly
        self._filepath = filepath
        self._owner_thread = threading.current_thread().ident
        self._conn = _prepare_database(filepath, readonly)
        self._lock = lock

    @property
    def readonly(self):
        "If True, cache be read but not updated."
        return self._readonly

    @property
    def filepath(self):
        "Filepath of SQLite database storing cache data"
        return self._filepath

    @property
    def capacity(self):
        "Max capacity in bytes. Includes response bodies only."
        return self._capacity

    @capacity.setter
    def capacity(self, capacity):
        self._capacity = capacity

    @property
    def max_item_size(self):
        "Max size of a response body eligible for caching."
        return self._max_item_size

    @max_item_size.setter
    def max_item_size(self, max_item_size):
        self._max_item_size = max_item_size

    @with_thread_lock
    def clear(self):
        """
        Drop all entries from HTTP response cache.
        """
        if self.readonly:
            raise RuntimeError("Cannot clear read-only cache")
        if not self.write_safe():
            raise RuntimeError(
                "Cannot clear cache from a different thread than the one it was created on"
            )
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM responses")
            self._conn.commit()

    @with_thread_lock
    def get(self, request: httpx.Request) -> tp.Optional[httpx.Response]:
        """Get cached response from Cache.

        We use the httpx.Request.url as key.

        Args:
            request: httpx.Request

        Returns:
            tp.Optional[httpx.Response]
        """
        with closing(self._conn.cursor()) as cur:
            cache_key = get_cache_key(request)
            row = cur.execute(
                """SELECT
status_code, headers, body, is_stream, encoding
FROM
responses
WHERE cache_key = ?""",
                (cache_key,),
            ).fetchone()
            if row is None:
                return None
            if (not self.readonly) and self.write_safe():
                cur.execute(
                    """UPDATE responses
    SET time_last_accessed = ?
    WHERE cache_key = ?""",
                    (datetime.now().timestamp(), cache_key),
                )
            self._conn.commit()

        return load(row, request)

    @with_thread_lock
    def set(
        self,
        *,
        request: httpx.Request,
        response: httpx.Response,
        content: tp.Optional[bytes] = None,
    ) -> None:
        """Set new response entry in cache.

        In case the response does not yet have a '_content' property, content should
        be provided in the optional 'content' kwarg (usually using a callback)

        Parameters
        ----------
        request: httpx.Request
        response: httpx.Response, to cache
        content (bytes, optional): Defaults to None, should be provided in case
            response that not have yet content.
        """
        if self.readonly:
            raise RuntimeError("Cache is readonly")
        if not self.write_safe():
            raise RuntimeError("Write is not safe from another thread")
        incoming_size = get_size(response, content)
        if incoming_size > self.max_item_size:
            # Decline to store.
            return False
        with closing(self._conn.cursor()) as cur:
            (total_size,) = cur.execute("SELECT SUM(size) FROM responses").fetchone()
            total_size = total_size or 0  # If empty, total_size is None.
            while (incoming_size + total_size) > self.capacity:
                # Cull to make space.
                (cache_key, size) = cur.execute(
                    """SELECT
cache_key, size
FROM responses
ORDER BY time_last_accessed ASC"""
                ).fetchone()
                cur.execute("DELETE FROM responses WHERE cache_key = ?", (cache_key,))
                total_size -= size
            cur.execute(
                """INSERT OR REPLACE INTO responses
(cache_key, status_code, headers, body, is_stream, encoding, size, time_created, time_last_accessed)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (get_cache_key(request),) + dump(response, content),
            )
            self._conn.commit()
        return True

    @with_thread_lock
    def delete(self, request: httpx.Request) -> None:
        """Delete an entry from cache.

        Args:
            request: httpx.Request
        """
        if self.readonly:
            raise RuntimeError("Cache is readonly")
        if not self.write_safe():
            raise RuntimeError("Write is not safe from another thread")
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "DELETE FROM responses WHERE cache_key=?", (get_cache_key(request),)
            )
            self._conn.commit()

    def size(self):
        "Size of response bodies in bytes (does not count headers and other auxiliary info)"
        with closing(self._conn.cursor()) as cur:
            (total_size,) = cur.execute("SELECT SUM(size) FROM responses").fetchone()
        return total_size or 0  # If emtpy, total_size is None.

    def count(self):
        "Number of responses cached"
        with closing(self._conn.cursor()) as cur:
            (count,) = cur.execute("SELECT COUNT(*) FROM responses").fetchone()
        return count or 0  # If empty, count is None.

    def close(self) -> None:
        """Close cache."""
        self._conn.close()
