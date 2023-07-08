import json
import sqlite3
import threading
import typing as tp
from contextlib import closing
from datetime import datetime
from pathlib import Path

import httpx

from .utils import TiledResponse

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


def _prepare_database(filepath):
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


class Cache:
    def __init__(
        self,
        filepath,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ):
        if readonly:
            raise NotImplementedError(
                "readonly cache is planned but not yet implemented"
            )
        if capacity <= max_item_size:
            raise ValueError("capacity must be greater than max_item_size")
        self.capacity = capacity
        self.max_item_size = max_item_size
        self._readonly = readonly
        self._filepath = filepath
        self._owner_thread = threading.current_thread().ident
        self._conn = _prepare_database(filepath)

    def write_safe(self):
        """
        Check that it is safe to write.

        SQLite is not threadsafe for concurrent _writes_.
        """
        return threading.current_thread().ident == self._owner_thread

    def __getstate__(self):
        return (self.filepath, self.capacity, self.max_item_size, self._readonly)

    def __setstate__(self, state):
        (filepath, capacity, max_item_size, readonly) = state
        self.capacity = capacity
        self.max_item_size = max_item_size
        self._readonly = readonly
        self._filepath = filepath
        self._owner_thread = threading.current_thread().ident
        self._conn = _prepare_database(filepath)

    @property
    def readonly(self):
        return self._readonly

    @property
    def filepath(self):
        return self._filepath

    def clear(self):
        """
        Drop all entries from HTTP response cache.
        """
        if self.readonly:
            raise RuntimeError("Cannot clear read-only cache")
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM responses")
            self._conn.commit()

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

        Args:
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
            total_size = total_size or 0  # may be None
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

    def close(self) -> None:
        """Close cache."""
        self._conn.close()

    # async def aget(self, request: httpx.Request) -> tp.Optional[httpx.Response]:
    #     """(Async) Get cached response from Cache.

    #     We use the httpx.Request.url as key.

    #     Args:
    #         request: httpx.Request

    #     Returns:
    #         tp.Optional[httpx.Response]
    #     """

    # async def aset(
    #     self,
    #     *,
    #     request: httpx.Request,
    #     response: httpx.Response,
    #     content: tp.Optional[bytes] = None,
    # ) -> None:
    #     """(Async) Set new response entry in cache.

    #     In case the response does not yet have a '_content' property, content should
    #     be provided in the optional 'content' kwarg (usually using a callback)

    #     Args:
    #         request: httpx.Request
    #         response: httpx.Response, to cache
    #         content (bytes, optional): Defaults to None, should be provided in case
    #             response that not have yet content.
    #     """

    # async def adelete(self, request: httpx.Request) -> None:
    #     """(Async) Delete an entry from cache.

    #     Args:
    #         request: httpx.Request
    #     """
    # async def aclose(self) -> None:
    #     """(Async) Close cache."""
