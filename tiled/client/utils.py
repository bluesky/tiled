import os
import secrets
import urllib.parse

import httpx

from ..utils import Sentinel


UNSET = Sentinel("UNSET")
NEEDS_INITIALIZATION = Sentinel("NEEDS_INITIALIZATION")


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


def client_from_catalog(catalog, authentication, server_settings):
    from ..server.app import serve_catalog

    params = {}
    if (authentication.get("authenticator") is None) and (
        authentication.get("single_user_api_key") is None
    ):
        # Generate the key here instead of letting serve_catalog do it for us,
        # so that we can give it to the client below.
        single_user_api_key = os.getenv(
            "TILED_SINGLE_USER_API_KEY", secrets.token_hex(32)
        )
        authentication["single_user_api_key"] = single_user_api_key
        params["api_key"] = single_user_api_key
    app = serve_catalog(catalog, authentication, server_settings)

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
        params=params,
        app=app,
        _startup_hook=startup,
    )
    # TODO How to close the httpx.AsyncClient more cleanly?
    import atexit

    atexit.register(client.close)
    return client


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
