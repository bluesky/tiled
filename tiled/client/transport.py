"""
Adapted from https://raw.githubusercontent.com/obendidi/httpx-cache/main/httpx_cache/transport.py
in accordance with its BSD-3 license
"""
import collections
import contextlib
import os
import typing as tp

import httpx

from .cache import Cache
from .cache_control import ByteStreamWrapper, CacheControl
from .utils import TiledResponse

# Logging calls are not run if Python is run as python -O (with optimizations).
if __debug__:
    import logging

    # By default, the token in the authentication header is redacted from the logs.
    # Set thie env var to 1 to show it for debugging purposes.
    TILED_LOG_AUTH_TOKEN = int(os.getenv("TILED_LOG_AUTH_TOKEN", False))

    class ClientLogRecord(logging.LogRecord):
        def getMessage(self):
            if hasattr(self, "request"):
                request = self.request
                message = f"-> {request.method} '{request.url}' " + " ".join(
                    f"'{k}:{v}'"
                    for k, v in request.headers.items()
                    if k != "authorization"
                )
                # Handle the authorization header specially.
                # For debugging, it can be useful to show it so that the log message
                # can be copy/pasted and passed to httpie in a shell.
                # But for screen-sharing demos, it should be redacted.
                if TILED_LOG_AUTH_TOKEN:
                    if "authorization" in request.headers:
                        message += (
                            f" 'authorization:{request.headers['authorization']}'"
                        )
                else:
                    if "authorization" in request.headers:
                        scheme, _, param = request.headers["authorization"].partition(
                            " "
                        )
                        message += f" 'authorization:{scheme} [redacted]'"
            elif hasattr(self, "response"):
                response = self.response
                request = response.request
                message = f"<- {response.status_code} " + " ".join(
                    f"{k}:{v}" for k, v in response.headers.items()
                )
            else:
                message = super().getMessage()
            return message

    def patched_make_record(
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        rv = ClientLogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        if extra is not None:
            for key in extra:
                if (key in ["message", "asctime"]) or (key in rv.__dict__):
                    raise KeyError("Attempt to overwrite %r in LogRecord" % key)
                rv.__dict__[key] = extra[key]
        return rv

    logger = logging.getLogger("tiled.client")
    # Monkey-patch our logger!
    # The logging framework provides no way to look a custom record factory into
    # the global logging manager. I tried several ways to avoid monkey-patching
    # and this is the least bad. Notice that it only downloades the 'tiled.client'
    # logger and will not affect the behavior of other loggers.
    logger.makeRecord = patched_make_record
    handler = logging.StreamHandler()
    log_format = "%(asctime)s.%(msecs)03d %(message)s"

    handler.setFormatter(logging.Formatter(log_format, datefmt="%H:%M:%S"))

    def log_request(request):
        logger.debug("", extra={"request": request})

    def log_response(response):
        logger.debug("", extra={"response": response})

    def collect_request(request):
        if _history is not None:
            _history.requests.append(request)

    def collect_response(response):
        if _history is not None:
            _history.responses.append(response)


def show_logs():
    """
    Log network traffic and interactions with the cache.

    This is just a convenience function that makes some Python logging configuration calls.
    """
    logger.setLevel("DEBUG")
    logger.addHandler(handler)


def hide_logs():
    """
    Undo show_logs().
    """
    logger.setLevel("WARNING")
    if handler in logger.handlers:
        logger.removeHandler(handler)


History = collections.namedtuple("History", "requests responses")
_history = None


@contextlib.contextmanager
def record_history():
    """
    Collect requests and responses.

    >>> with history() as t:
    ...     ...
    ...

    >>> t.requests
    [...]

    >>> t.responses
    [...]
    """
    global _history

    responses = []
    requests = []
    history = History(requests, responses)
    _history = history
    yield history
    _history = None


class Transport(httpx.BaseTransport):
    """Custom transport, implementing caching and custom compression encodings.

    Args:
        transport (optional): an existing httpx transport, if no transport
            is given, defaults to an httpx.HTTPTransport with default args.
        cache (optional): cache to use with this transport, defaults to
            httpx_cache.DictCache
        cacheable_methods: methods that are allowed to be cached, defaults to ['GET']
        cacheable_status_codes: status codes that are allowed to be cached,
            defaults to: (200, 203, 300, 301, 308)
    """

    def __init__(
        self,
        *,
        transport: tp.Optional[httpx.BaseTransport] = None,
        cache: tp.Optional[Cache] = None,
        cacheable_methods: tp.Tuple[str, ...] = ("GET",),
        cacheable_status_codes: tp.Tuple[int, ...] = (200, 203, 300, 301, 308),
        always_cache: bool = False,
    ):
        self.controller = CacheControl(
            cacheable_methods=cacheable_methods,
            cacheable_status_codes=cacheable_status_codes,
            always_cache=always_cache,
        )
        self.transport = transport or httpx.HTTPTransport()
        self.cache = cache

    def close(self) -> None:
        self.cache.close()
        self.transport.close()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        # check if request is cacheable
        if (self.cache is not None) and self.controller.is_request_cacheable(request):
            if __debug__:
                logger.debug(f"Checking cache for: {request}")
            cached_response = self.cache.get(request)
            if cached_response is not None:
                if __debug__:
                    logger.debug(f"Found cached response for: {request}")
                if self.controller.is_response_fresh(
                    request=request, response=cached_response
                ):
                    if not self.controller.needs_revalidation(
                        request=request, response=cached_response
                    ):
                        if __debug__:
                            logger.debug("Using cached response without revalidation")
                            log_request(request)
                            collect_request(request)
                        return cached_response
                    request.headers["If-None-Match"] = cached_response.headers["ETag"]
                else:
                    if __debug__:
                        logger.debug(f"Cached response is stale, deleting: {request}")
                    self.cache.delete(request)
            else:
                if __debug__:
                    logger.debug("No valid cached response found in cache")

        # Call original transport
        if __debug__:
            log_request(request)
            collect_request(request)
        response = self.transport.handle_request(request)
        response.__class__ = TiledResponse
        response.request = request
        if __debug__:
            # Log the actual server traffic, not the cached response.
            log_response(response)
            # But, below _collect_ the response with the content in it.

        if self.cache is not None:
            if response.status_code == 304:
                if __debug__:
                    logger.debug(f"Server validated fresh cached entry for: {request}")
                    collect_response(cached_response)
                return cached_response
            if __debug__:
                logger.debug(f"Server invalidated stale cached entry for: {request}")
                collect_response(response)

            if self.controller.is_response_cacheable(
                request=request, response=response
            ):
                if hasattr(response, "_content"):
                    if __debug__:
                        logger.debug(f"Caching response for: {request}")
                    self.cache.set(request=request, response=response)
                else:
                    # Wrap the response with cache callback:
                    def _callback(content: bytes) -> None:
                        if __debug__:
                            logger.debug(f"Caching response for: {request}")
                        self.cache.set(
                            request=request, response=response, content=content
                        )

                    response.stream = ByteStreamWrapper(
                        stream=response.stream, callback=_callback  # type: ignore
                    )
        return response


# For when we implement an Async client
#
# class AsyncCacheControlTransport(httpx.AsyncBaseTransport):
#     """Async CacheControl transport for httpx_cache.
#
#     Args:
#         transport (optional): an existing httpx async-transport, if no transport
#             is given, defaults to an httpx.AsyncHTTPTransport with default args.
#         cache (optional): cache to use with this transport, defaults to
#             httpx_cache.DictCache
#         cacheable_methods: methods that are allowed to be cached, defaults to ['GET']
#         cacheable_status_codes: status codes that are allowed to be cached,
#             defaults to: (200, 203, 300, 301, 308)
#     """
#
#     def __init__(
#         self,
#         *,
#         transport: tp.Optional[httpx.AsyncBaseTransport] = None,
#         cache: tp.Optional[BaseCache] = None,
#         cacheable_methods: tp.Tuple[str, ...] = ("GET",),
#         cacheable_status_codes: tp.Tuple[int, ...] = (200, 203, 300, 301, 308),
#         always_cache: bool = False,
#     ):
#         self.controller = CacheControl(
#             cacheable_methods=cacheable_methods,
#             cacheable_status_codes=cacheable_status_codes,
#             always_cache=always_cache,
#         )
#         self.transport = transport or httpx.AsyncHTTPTransport()
#         self.cache = cache or DictCache()
#
#     async def aclose(self) -> None:
#         await self.cache.aclose()
#         await self.transport.aclose()
#
#     async def handle_async_request(self, request: httpx.Request) -> TiledResponse:
#         # check if request is cacheable
#         if self.controller.is_request_cacheable(request):
#             logger.debug(f"Checking cache for: {request}")
#             cached_response = await self.cache.aget(request)
#             if cached_response is not None:
#                 logger.debug(f"Found cached response for: {request}")
#                 if self.controller.is_response_fresh(
#                     request=request, response=cached_response
#                 ):
#                     setattr(cached_response, "from_cache", True)
#                     return cached_response
#                 else:
#                     logger.debug(f"Cached response is stale, deleting: {request}")
#                     await self.cache.adelete(request)
#
#         # Request is not in cache, call original transport
#         response = await self.transport.handle_async_request(request)
#
#         if self.controller.is_response_cacheable(request=request, response=response):
#             if hasattr(response, "_content"):
#                 logger.debug(f"Caching response for: {request}")
#                 await self.cache.aset(request=request, response=response)
#             else:
#                 # Wrap the response with cache callback:
#                 async def _callback(content: bytes) -> None:
#                     logger.debug(f"Caching response for: {request}")
#                     await self.cache.aset(
#                         request=request, response=response, content=content
#                     )
#
#                 response.stream = ByteStreamWrapper(
#                     stream=response.stream, callback=_callback  # type: ignore
#                 )
#         setattr(response, "from_cache", False)
#         return response
