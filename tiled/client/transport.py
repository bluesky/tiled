"""
Adapted from https://raw.githubusercontent.com/obendidi/httpx-cache/main/httpx_cache/transport.py
in accordance with its BSD-3 license
"""
import typing as tp

import httpx

from .cache import Cache
from .cache_control import ByteStreamWrapper, CacheControl
from .logger import collect_request, collect_response, log_request, log_response, logger
from .utils import TiledResponse


def _proxy_mounts_from_env(
    transport_kwargs: tp.Dict[str, tp.Any],
) -> tp.Dict[tp.Any, tp.Optional[httpx.BaseTransport]]:
    """Build proxy transports from the standard proxy environment variables.

    Reads HTTP_PROXY / HTTPS_PROXY / ALL_PROXY / NO_PROXY (via httpx's own
    resolution helper) and returns a mapping of URLPattern -> transport,
    sorted by pattern priority so the most specific match wins. A value of
    None indicates a NO_PROXY entry (connect directly).

    This reproduces the behavior httpx.Client applies to the transport it
    builds itself, which is otherwise skipped when a custom transport is
    supplied.
    """
    from httpx._utils import URLPattern, get_environment_proxies

    mounts: tp.Dict[tp.Any, tp.Optional[httpx.BaseTransport]] = {}
    for key, value in get_environment_proxies().items():
        mounts[URLPattern(key)] = (
            None
            if value is None
            else httpx.HTTPTransport(proxy=value, **transport_kwargs)
        )
    return dict(sorted(mounts.items(), key=lambda item: item[0].priority))


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
        limits: tp.Optional[httpx.Limits] = None,
        verify: tp.Union[bool, str] = True,
        trust_env: bool = True,
        cacheable_methods: tp.Tuple[str, ...] = ("GET",),
        cacheable_status_codes: tp.Tuple[int, ...] = (
            httpx.codes.OK,
            httpx.codes.NON_AUTHORITATIVE_INFORMATION,
            httpx.codes.MULTIPLE_CHOICES,
            httpx.codes.MOVED_PERMANENTLY,
            httpx.codes.PERMANENT_REDIRECT,
        ),
        always_cache: bool = False,
    ):
        self.controller = CacheControl(
            cacheable_methods=cacheable_methods,
            cacheable_status_codes=cacheable_status_codes,
            always_cache=always_cache,
        )
        # Mapping of URLPattern -> transport for requests that must be routed
        # through a proxy (or, when the value is None, connected to directly).
        self._mounts: tp.Dict[tp.Any, tp.Optional[httpx.BaseTransport]] = {}
        if transport is not None:
            # An explicit transport was provided (e.g. the in-process ASGI
            # transport used for TestClient). Use it as-is; proxy/verify
            # handling does not apply.
            self.transport = transport
        else:
            transport_kwargs: tp.Dict[str, tp.Any] = {"verify": verify}
            if limits is not None:
                transport_kwargs["limits"] = limits
            self.transport = httpx.HTTPTransport(**transport_kwargs)
            if trust_env:
                # httpx.Client honors HTTP_PROXY/HTTPS_PROXY/NO_PROXY env vars,
                # but only for the transport it builds itself. Because Tiled
                # supplies a custom transport (to enable response caching),
                # that logic is bypassed. Replicate it here so proxy env vars
                # are respected.
                self._mounts = _proxy_mounts_from_env(transport_kwargs)
        self.cache = cache

    def _transport_for_url(self, url: httpx.URL) -> httpx.BaseTransport:
        """Select the transport for a URL, honoring proxy mounts.

        Mirrors httpx.Client._transport_for_url: the most specific matching
        pattern wins; a value of None means "connect directly" (e.g. NO_PROXY).
        """
        for pattern, transport in self._mounts.items():
            if pattern.matches(url):
                return transport or self.transport
        return self.transport

    def close(self) -> None:
        self.transport.close()
        for transport in self._mounts.values():
            if transport is not None:
                transport.close()
        if self.cache is not None:
            self.cache.close()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        # check if request is cacheable
        if (self.cache is not None) and self.controller.is_request_cacheable(request):
            if __debug__:
                logger.debug("Checking cache for: %s", request)
            cached_response = self.cache.get(request)
            if cached_response is not None:
                if self.controller.is_response_fresh(
                    request=request, response=cached_response
                ):
                    if not self.controller.needs_revalidation(
                        request=request, response=cached_response
                    ):
                        if __debug__:
                            logger.debug("Using cached response for: %s", request)
                            log_request(request)
                            collect_request(request)
                        return cached_response
                    if __debug__:
                        logger.debug("Revalidating cached response for: %s", request)
                    request.headers["If-None-Match"] = cached_response.headers["ETag"]
                else:
                    if __debug__:
                        logger.debug("Cached response is stale, deleting: %s", request)
                    self.cache.delete(request)
            else:
                if __debug__:
                    logger.debug(
                        "No valid cached response found in cache for: %s", request
                    )

        # Call original transport
        if __debug__:
            log_request(request)
            collect_request(request)
        response = self._transport_for_url(request.url).handle_request(request)
        response.__class__ = TiledResponse
        response.request = request
        if __debug__:
            # Log the actual server traffic, not the cached response.
            log_response(response)
            # But, below _collect_ the response with the content in it.

        if self.cache is not None:
            if response.status_code == httpx.codes.NOT_MODIFIED:
                if __debug__:
                    logger.debug(
                        "Server validated as fresh cached entry for: %s", request
                    )
                    collect_response(cached_response)
                return cached_response

            if self.controller.is_response_cacheable(
                request=request, response=response
            ):
                if self.cache.readonly:
                    if __debug__:
                        logger.debug("Cache is read-only; will not store")
                elif not self.cache.write_safe():
                    if __debug__:
                        logger.debug(
                            "Cannot write to cache from another thread; will not store"
                        )
                else:
                    if hasattr(response, "_content"):
                        is_stored = self.cache.set(request=request, response=response)
                        if __debug__:
                            if is_stored:
                                logger.debug("Caching response for: %s", request)
                            else:
                                logger.debug(
                                    "Declined to store large response for: %s", request
                                )
                    else:
                        # Wrap the response with cache callback:
                        def _callback(content: bytes) -> None:
                            is_stored = self.cache.set(
                                request=request, response=response, content=content
                            )
                            if __debug__:
                                if is_stored:
                                    logger.debug("Caching response for: %s", request)
                                else:
                                    logger.debug(
                                        "Declined to store large response for: %s",
                                        request,
                                    )

                        response.stream = ByteStreamWrapper(
                            stream=response.stream, callback=_callback  # type: ignore
                        )
        if __debug__:
            collect_response(response)
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
#             logger.debug(f"Checking cache for: %s", request)
#             cached_response = await self.cache.aget(request)
#             if cached_response is not None:
#                 logger.debug(f"Found cached response for: %s", request)
#                 if self.controller.is_response_fresh(
#                     request=request, response=cached_response
#                 ):
#                     setattr(cached_response, "from_cache", True)
#                     return cached_response
#                 else:
#                     logger.debug(f"Cached response is stale, deleting: %s", request)
#                     await self.cache.adelete(request)
#
#         # Request is not in cache, call original transport
#         response = await self.transport.handle_async_request(request)
#
#         if self.controller.is_response_cacheable(request=request, response=response):
#             if hasattr(response, "_content"):
#                 logger.debug(f"Caching response for: %s", request)
#                 await self.cache.aset(request=request, response=response)
#             else:
#                 # Wrap the response with cache callback:
#                 async def _callback(content: bytes) -> None:
#                     logger.debug(f"Caching response for: %s", request)
#                     await self.cache.aset(
#                         request=request, response=response, content=content
#                     )
#
#                 response.stream = ByteStreamWrapper(
#                     stream=response.stream, callback=_callback  # type: ignore
#                 )
#         setattr(response, "from_cache", False)
#         return response
