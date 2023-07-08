"""
Adapted from https://raw.githubusercontent.com/obendidi/httpx-cache/main/httpx_cache/transport.py
in accordance with its BSD-3 license
"""
import logging
import typing as tp

import httpx

from .cache import Cache
from .cache_control import ByteStreamWrapper, CacheControl
from .utils import TiledResponse

logger = logging.getLogger(__name__)


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
            logger.debug(f"Checking cache for: {request}")
            cached_response = self.cache.get(request)
            if cached_response is not None:
                logger.debug(f"Found cached response for: {request}")
                if self.controller.is_response_fresh(
                    request=request, response=cached_response
                ):
                    if not self.controller.needs_revalidation(
                        request=request, response=cached_response
                    ):
                        logger.debug("Using cached response without revalidation")
                        return cached_response
                    request.headers["If-None-Match"] = cached_response.headers["ETag"]
                else:
                    logger.debug(f"Cached response is stale, deleting: {request}")
                    self.cache.delete(request)
            else:
                logger.debug("No valid cached response found in cache")

        # Call original transport
        response = self.transport.handle_request(request)

        if response.status_code == 304:
            logger.debug(f"Server revalidated fresh entry for: {request}")
            # TODO Update headers?
            return cached_response
        logger.debug(f"Server invalidated stale entry for: {request}")

        if (self.cache is not None) and self.controller.is_response_cacheable(
            request=request, response=response
        ):
            if hasattr(response, "_content"):
                logger.debug(f"Caching response for: {request}")
                self.cache.set(request=request, response=response)
            else:
                # Wrap the response with cache callback:
                def _callback(content: bytes) -> None:
                    logger.debug(f"Caching response for: {request}")
                    self.cache.set(request=request, response=response, content=content)

                response.stream = ByteStreamWrapper(
                    stream=response.stream, callback=_callback  # type: ignore
                )
        response.__class__ = TiledResponse
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
