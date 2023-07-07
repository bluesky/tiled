import json
import sqlite3
import typing as tp
import urllib
from contextlib import closing
from datetime import datetime
from pathlib import Path

import httpx

from . import context

VERSION = 1


def default_cache_filepath(api_uri):
    "Return default cache filepath for this API."
    # TO DO: If TILED_CACHE_DIR is on NFS, use a sqlite temporary database instead.

    # We access TILED_CACHE_DIR as context.TILED_CACHE_DIR rather than
    # importing it as its own object. This is important because the test suite
    # monkey-patches context.TILED_CACHE_DIR to an isolated temporary directory
    # for each test, and we want that to apply here too.
    return Path(
        context.TILED_CACHE_DIR,
        "http_response_cache",
        urllib.parse.quote_plus(str(api_uri)),
    )


def get_cache_key(request: httpx.Request) -> str:
    """Get the cache key from a request.

    The cache key is the str request url.

    Args:
        request: httpx.Request

    Returns:
        str: httpx.Request.url
    """
    return str(request.url)


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
    response = httpx.Response(
        status_code, headers=headers, content=content, stream=stream
    )
    if encoding is not None:
        response.encoding = encoding

    if request is not None:
        response.request = request
    return response


class Cache:
    def __init__(
        self,
        filepath,
        total_capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ):
        if readonly:
            raise NotImplementedError(
                "readonly cache is planned but not yet implemented"
            )
        self.total_capacity = total_capacity
        self.max_item_size = max_item_size
        self._readonly = readonly
        self._filepath = filepath
        self._db = sqlite3.connect(filepath)
        cursor = self._db.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        if not tables:
            # We have an empty database.
            self._create_tables()
        elif "tiled_http_response_cache_version" not in tables:
            # We have a nonempty database that we do not recognize.
            print(tables)
            raise RuntimeError(
                f"Database at {filepath} is not empty and not recognized as a tiled HTTP response cache."
            )
        else:
            # We have a nonempty database that we recognize.
            cursor = self._db.execute(
                "SELECT * FROM tiled_http_response_cache_version;"
            )
            (version,) = cursor.fetchone()
            if version != VERSION:
                # It is likely that this cache database will be very stable; we
                # *may* never need to change the schema. But if we do, we will
                # not bother with migrations. The cache is highly disposable.
                # Just silently blow it away and start over.
                Path(filepath).unlink()
                self._db = sqlite3.connect(filepath)
                self._create_tables()

    def _create_tables(self):
        with closing(self._db.cursor()) as cur:
            cur.execute(
                """CREATE TABLE responses (
cache_key TEXT,
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
            cur.execute(
                "CREATE TABLE tiled_http_response_cache_version (version INTEGER)"
            )
            cur.execute(
                "INSERT INTO tiled_http_response_cache_version (version) VALUES (?)",
                (VERSION,),
            )
            self._db.commit()

    @property
    def readonly(self):
        return self._readonly

    @property
    def filepath(self):
        return self._filepath

    def get(self, request: httpx.Request) -> tp.Optional[httpx.Response]:
        """Get cached response from Cache.

        We use the httpx.Request.url as key.

        Args:
            request: httpx.Request

        Returns:
            tp.Optional[httpx.Response]
        """
        with closing(self._db.cursor()) as cur:
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
            return load(row)

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
        with closing(self._db.cursor()) as cur:
            cur.execute(
                """INSERT INTO responses
(cache_key, status_code, headers, body, is_stream, encoding, size, time_created, time_last_accessed)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (get_cache_key(request),) + dump(response, content),
            )
            self._db.commit()

    def delete(self, request: httpx.Request) -> None:
        """Delete an entry from cache.

        Args:
            request: httpx.Request
        """
        with closing(self._db.cursor()) as cur:
            cur.execute(
                "DELETE FROM responses WHERE cache_key=?", (get_cache_key(request),)
            )
            self._db.commit()

    def close(self) -> None:
        """Close cache."""
        self._db.close()

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
