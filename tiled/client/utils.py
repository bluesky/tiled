import asyncio
from inspect import iscoroutine

import httpx
import msgpack

from ..utils import Sentinel


class UNSET(Sentinel):
    pass


def get_content_with_cache(
    cache, offline, client, path, accept=None, timeout=UNSET, **kwargs
):
    request = client.build_request("GET", path, **kwargs)
    if accept:
        request.headers["Accept"] = accept
    url = request.url.raw  # URL as tuple
    if offline:
        # We must rely on the cache alone.
        reservation = cache.get_reservation(url)
        if reservation is None:
            raise NotAvailableOffline(url)
        content = reservation.load_content()
        if content is None:
            # TODO Do we ever get here?
            raise NotAvailableOffline(url)
        return content
    if cache is None:
        # No cache, so we can use the client straightforwardly.
        response = _send(client, request, timeout=timeout)
        handle_error(response)
        return response.content
    # If we get this far, we have an online client and a cache.
    reservation = cache.get_reservation(url)
    try:
        if reservation is not None:
            request.headers["If-None-Match"] = reservation.etag
        response = _send(client, request, timeout=timeout)
        handle_error(response)
        if response.status_code == 304:  # HTTP 304 Not Modified
            # Read from the cache
            content = reservation.load_content()
        elif response.status_code == 200:
            etag = response.headers.get("ETag")
            content = response.content
            # TODO Respect Cache-control headers (e.g. "no-store")
            if etag is not None:
                # Write to cache.
                cache.put_etag_for_url(url, etag)
                cache.put_content(etag, content)
        else:
            raise NotImplementedError(f"Unexpected status_code {response.status_code}")
    finally:
        if reservation is not None:
            reservation.ensure_released()
    return content


def get_json_with_cache(cache, offline, client, path, **kwargs):
    return msgpack.unpackb(
        get_content_with_cache(
            cache, offline, client, path, accept="application/x-msgpack", **kwargs
        )
    )


def _send(client, request, timeout):
    """
    EXPERIMENTAL: Tolerate sync httpx.Client or httpx.AsyncClient.

    The AsyncClient is interesting because it can interface directly with FastAPI app
    in the same process via ASGI.
    """
    if timeout is UNSET:
        result = client.send(request)
    else:
        result = client.send(request, timeout=timeout)
    if iscoroutine(result):
        return asyncio.run(result)
    return result


def handle_error(response):
    try:
        response.raise_for_status()
    except httpx.RequestError:
        raise  # Nothing to add in this case; just raise it.
    except httpx.HTTPStatusError as exc:
        if response.status_code < 500:
            # Include more detail that httpx does by default.
            message = (
                f"{exc.response.status_code}: "
                f"{exc.response.json()['detail']} "
                f"{exc.request.url}"
            )
            raise ClientError(message, exc.request, exc.response) from exc
        else:
            raise


class ClientError(httpx.HTTPStatusError):
    def __init__(self, message, request, response):
        super().__init__(message=message, request=request, response=response)


class NotAvailableOffline(Exception):
    "Item looked for in offline cache was not found."
