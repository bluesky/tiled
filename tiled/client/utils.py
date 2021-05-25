import urllib.parse

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
    Handle httpx's timeout API, which uses a special internal sentinel to mean
    "no timeout" and therefore must not be passed any value (including None)
    if we want no timeout.
    """
    if timeout is UNSET:
        return client.send(request)
    else:
        return client.send(request, timeout=timeout)


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


def client_from_catalog(catalog, authenticator, allow_anonymous_access, secret_keys):
    from ..server.app import serve_catalog

    app = serve_catalog(catalog, authenticator, allow_anonymous_access, secret_keys)

    # Only an AsyncClient can be used over ASGI.
    # We wrap all the async methods in a call to asyncio.run(...).
    # Someday we should explore asynchronous Tiled Client objects.
    from ._async_bridge import AsyncClientBridge

    async def startup():
        # Note: This is important. The Tiled server routes are defined lazily on
        # startup.
        await app.router.startup()

    client = AsyncClientBridge(
        base_url="http://local-tiled-app",
        app=app,
        _startup_hook=startup,
    )
    # TODO How to close the httpx.AsyncClient more cleanly?
    import atexit

    atexit.register(client.close)


def client_and_path_from_uri(uri):
    headers = {}
    url = httpx.URL(uri)
    parsed_query = urllib.parse.parse_qs(url.query.decode())
    api_key_list = parsed_query.pop("api_key", None)
    if api_key_list is not None:
        if len(api_key_list) != 1:
            raise ValueError("Cannot handle two api_key query parameters")
        (api_key,) = api_key_list
        headers["X-TILED-API-KEY"] = api_key
    query = urllib.parse.urlencode(parsed_query, doseq=True)
    path = [segment for segment in url.path.rstrip("/").split("/") if segment]
    if path:
        if path[0] != "metadata":
            raise ValueError(
                "When the URI has a path, the path expected to begin with '/metadata/'"
            )
        path.pop(0)  # ["metadata", "stuff", "things"] -> ["stuff", "things"]
    base_url = urllib.parse.urlunsplit(
        (url.scheme, url.netloc, "", query, url.fragment)
    )
    client = httpx.Client(base_url=base_url, headers=headers)
    return client, path
