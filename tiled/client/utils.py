import os
from pathlib import Path, PurePosixPath
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
    # The uri is expected to reach the root or /metadata route.
    url = httpx.URL(uri)

    # If ?api_key=... is present, move it from the query into a header.
    parsed_query = urllib.parse.parse_qs(url.query.decode())
    api_key_list = parsed_query.pop("api_key", None)
    if api_key_list is not None:
        if len(api_key_list) != 1:
            raise ValueError("Cannot handle two api_key query parameters")
        (api_key,) = api_key_list
        headers["X-TILED-API-KEY"] = api_key
    params = urllib.parse.urlencode(parsed_query, doseq=True)

    # Construct the URL *without* the params, which we will pass in separately.
    handshake_url = urllib.parse.urlunsplit(
        (url.scheme, url.netloc, url.path, {}, url.fragment)
    )

    # First, ask the server what its root_path is.
    client = httpx.Client(headers=headers, params=params)
    # This is the only place where we use client.get *directly*, circumventing
    # the usual "get with cache" logic.
    response = client.get(handshake_url, params={"root_path": None})
    handle_error(response)
    data = response.json()
    base_path = data["meta"]["root_path"]
    base_url = urllib.parse.urlunsplit(
        (url.scheme, url.netloc, base_path, {}, url.fragment)
    )
    client.base_url = base_url
    path_parts = list(PurePosixPath(url.path).relative_to(base_path).parts)
    if path_parts:
        # Strip "/metadata"
        path_parts.pop(0)
    return client, path_parts


def export_util(file, format, get, link, params):
    """
    Download client data in some format and write to a file.

    This is used by the export method on clients. It intended for internal use.

    Parameters
    ----------
    file: str or buffer
        Filepath or writeable buffer.
    format : str, optional
        If format is None and `file` is a filepath, the format is inferred
        from the name, like 'table.csv' implies format="text/csv". The format
        may be given as a file extension ("csv") or a media type ("text/csv").
    get : callable
        Client's internal GET method
    link: str
        URL to download full data
    params : dict
        Additional parameters for the request, which may be used to subselect
        or slice, for example.
    """

    # The server accpets a media type like "text/csv" or a file extension like
    # "csv" (no dot) as a "format".
    if "format" in params:
        raise ValueError("params may not include 'format'. Use the format parameter.")
    if isinstance(format, str) and format.startswith("."):
        format = format[1:]  # e.g. ".csv" -> "csv"
    if isinstance(file, str):
        # Infer that `file` is a filepath.
        if format is None:
            format = ".".join(
                suffix[1:] for suffix in Path(file).suffixes
            )  # e.g. "csv"
        content = get(link, params={"format": format, **params})
        with open(file, "wb") as file:
            file.write(content)
    else:
        # Infer that `file` is a writeable buffer.
        if format is None:
            # We have no filepath to infer to format from.
            raise ValueError("format must be specified when file is writeable buffer")
        content = get(link, params={"format": format, **params})
        file.write(content)
