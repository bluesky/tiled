"""
Adapted from https://raw.githubusercontent.com/obendidi/httpx-cache/main/httpx_cache/transport.py
in accordance with its BSD-3 license
"""
import logging
import typing as tp
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import attr
import httpx

logger = logging.getLogger(__name__)

_PERMANENT_REDIRECT_STATUSES = (301, 308)


def parse_headers_date(headers_date: tp.Optional[str]) -> tp.Optional[datetime]:
    """Parse a 'Date' header and return it as an optional datetime object.

    If the 'Date' does not exist return None
    If there is an error using parsing return None

    Args:
        headers: httpx.Headers

    Returns:
        Optional[datetime]
    """
    if not isinstance(headers_date, str):
        return None

    try:
        return parsedate_to_datetime(headers_date)
    except (ValueError, TypeError) as error:
        logger.error(error)
        return None


def parse_cache_control_headers(
    headers: httpx.Headers,
) -> tp.Dict[str, tp.Optional[int]]:
    """Parse cache-control headers.

    Args:
        headers: An instance of httpx headers.

    Returns:
        parsed cache-control headers as dict.
    """

    cache_control: tp.Dict[str, tp.Optional[int]] = {}
    directives = headers.get_list("cache-control", split_commas=True)
    for directive in directives:
        if "=" in directive:
            name, value = directive.split("=", maxsplit=1)
            if value.isdigit():
                cache_control[name] = int(value)
            else:
                cache_control[name] = None
        else:
            cache_control[directive] = None
    return cache_control


@attr.s
class ByteStreamWrapper(httpx.ByteStream):
    """Wrapper around the stream object of an httpx.Response."""

    stream: httpx.ByteStream = attr.ib(kw_only=True)
    callback: tp.Callable[[bytes], tp.Any] = attr.ib(kw_only=True)
    content: bytearray = attr.ib(factory=bytearray, init=False)

    def close(self) -> None:
        """Close stream."""
        self.stream.close()

    async def aclose(self) -> None:
        """Close async stream."""
        await self.stream.aclose()

    def __iter__(self) -> tp.Iterator[bytes]:
        """Iterate over the stream object and store it's chunks in a content.

        After the stream is completed call the callback with content as argument.
        """
        for chunk in self.stream:
            self.content.extend(chunk)
            yield chunk
        self.callback(bytes(self.content))

    async def __aiter__(self) -> tp.AsyncIterator[bytes]:
        """Iterate over the async stream object and store it's chunks in a content.

        After the stream is completed call the async callback with content as argument.
        """
        async for chunk in self.stream:
            self.content.extend(chunk)
            yield chunk
        await self.callback(bytes(self.content))


class CacheControl:
    """Cache controller for httpx-cache.

    Uses 'cache-contol' header direcrives for using/skipping cache.

    If no cache-control directive is set, the cache is used by default (except if there
    is an expires header in the response.)
    """

    def __init__(
        self,
        *,
        cacheable_methods: tp.Tuple[str, ...] = ("GET",),
        cacheable_status_codes: tp.Tuple[int, ...] = (
            httpx.codes.OK,
            httpx.codes.NON_AUTHORITATIVE_INFORMATION,
            httpx.codes.MULTIPLE_CHOICES,
            httpx.codes.MOVED_PERMANENTLY,
            httpx.codes.PERMANENT_REDIRECT,
        ),
        always_cache: bool = False,
    ) -> None:
        self.cacheable_methods = cacheable_methods
        self.cacheable_status_codes = cacheable_status_codes
        self.always_cache = always_cache

    def is_request_cacheable(self, request: httpx.Request) -> bool:
        """Checks if an httpx request has the necessary requirement to support caching.

        A request is cacheable if:

            - url is absolute
            - method is defined as cacheable (by default only GET methods are cached)
            - request has no 'no-cache' cache-control header directive
            - request has no 'max-age=0' cache-control header directive

        Args:
            request: httpx.Request

        Returns:
            True if request cacheable else False
        """
        if request.url.is_relative_url:
            # logger.debug(
            #     f"Only absolute urls are supported, got '{request.url}'. "
            #     "Request is not cacheable!"
            # )
            return False
        if request.method not in self.cacheable_methods:
            # logger.debug(
            #     f"Request method '{request.method}' is not supported, only "
            #     f"'{self.cacheable_methods}' are supported. Request is not cacheable!"
            # )
            return False
        cc = parse_cache_control_headers(request.headers)
        if "no-cache" in cc or cc.get("max-age") == 0:
            # logger.debug(
            #     "Request cache-control headers has a 'no-cache' directive. "
            #     "Request is not cacheable!"
            # )
            return False
        return True

    def is_response_fresh(
        self, *, request: httpx.Request, response: httpx.Response
    ) -> bool:
        """Checks whether a cached response is fresh or not.

        Args:
            request: httpx.Request
            response: httpx.Response

        Returns:
            True if request is fresh else False
        """

        # check if response is a permanent redirect
        if response.status_code in _PERMANENT_REDIRECT_STATUSES:
            # logger.debug(
            #     "Cached response with permanent redirect status "
            #     f"'{response.status_code}' is always fresh."
            # )
            return True

        # check that we do have a response Date header
        response_date = parse_headers_date(response.headers.get("date"))

        # extract cache_control for both request and response
        request_cc = parse_cache_control_headers(request.headers)
        response_cc = parse_cache_control_headers(response.headers)

        # get all values we need for freshness eval
        resp_max_age = response_cc.get("max-age")
        req_min_fresh = request_cc.get("min-fresh")
        req_max_age = request_cc.get("max-age")

        # check max-age in response
        if isinstance(req_max_age, int):
            max_freshness_age = timedelta(seconds=req_max_age)
            # logger.debug(
            #     "Evaluating response freshness from request cache-control "
            #     "'max-age' header directive."
            # )

        elif isinstance(resp_max_age, int):
            max_freshness_age = timedelta(seconds=resp_max_age)
            # logger.debug(
            #     "Evaluating response freshness from response cache-control "
            #     "'max-age' header directive."
            # )
        elif "expires" in response.headers and response_date is None:
            # logger.warning(
            #     "Response is missing a valid 'Date' header, couldn't evaluate "
            #     "response freshness. Response is not fresh!"
            # )
            return False
        elif "expires" in response.headers:
            resp_expires = parse_headers_date(response.headers.get("expires"))
            if resp_expires is None:
                # logger.warning(
                #     "Response has an invalid 'Expires' header, couldn't evaluate "
                #     "response freshness. Response is not fresh!"
                # )
                return False

            max_freshness_age = resp_expires - response_date  # type: ignore
            # logger.debug(
            #     "Evaluating response freshness from response 'expires' header."
            # )

        else:
            # logger.debug(
            #     "Request/Response pair has no cache-control headers. Assuming "
            #     "response is fresh!"
            # )
            return True

        if response_date is None:
            # logger.warning(
            #     "Response is missing a valid 'Date' header, couldn't evaluate "
            #     "response freshness. Response is not fresh!"
            # )
            return False

        # get response age (timedelta)
        now = datetime.now(tz=timezone.utc)
        response_age = now - response_date
        if isinstance(req_min_fresh, int):
            # logger.debug(
            #     f"Adjusting response age ({response_age}) using request cache-control "
            #     "'min-fresh' header directive."
            # )
            response_age += timedelta(seconds=req_min_fresh)

        # logger.debug(f"Response age is: {response_age}")
        # logger.debug(f"Response allowed max-age is: {max_freshness_age}")

        if response_age > max_freshness_age:
            # logger.debug("Response is not fresh!")
            return False

        # logger.debug("Response is fresh.")
        return True

    def is_response_cacheable(
        self, *, request: httpx.Request, response: httpx.Response
    ) -> bool:
        """Check if an httpx response is cacheable.

        A response is cacheable if:

            - response status_code is cacheable
            - request method is cacheable
            - One of:
                - always_cache is True
            OR:
                - Response has no 'no-store' cache-control header
                - Request has no 'no-store' cache-control header

        Args:
            request: httpx.Request
            response: httpx.Response

        Returns:
            whether response is cacheable or not.
        """
        if request.url.is_relative_url:
            # logger.debug(
            #     f"Only absolute urls are supported, got '{request.url}'. "
            #     "Request is not cacheable!"
            # )
            return False

        if request.method not in self.cacheable_methods:
            # logger.debug(
            #     f"Request method '{request.method}' is not supported, only "
            #     f"'{self.cacheable_methods}' are supported. Request is not cacheable!"
            # )
            return False

        if response.status_code not in self.cacheable_status_codes:
            # logger.debug(
            #     f"Response status_code '{response.status_code}' is not cacheable, only "
            #     f"'{self.cacheable_status_codes}' are cacheable. Response is not "
            #     "cacheable!"
            # )
            return False

        # always cache request, event if 'no-store' is set as header
        if self.always_cache:
            # logger.debug("Caching Response because 'always_cache' is set to True.'")
            return True

        # extract cache_control for both request and response
        request_cc = parse_cache_control_headers(request.headers)
        response_cc = parse_cache_control_headers(response.headers)

        if "no-store" in request_cc or "no-store" in response_cc:
            # logger.debug(
            #     "Request/Response cache-control headers has a 'no-store' directive. "
            #     "Response is not cacheable!"
            # )
            return False

        return True

    def needs_revalidation(
        self, *, request: httpx.Request, response: httpx.Response
    ) -> bool:
        return "ETag" in response.headers
