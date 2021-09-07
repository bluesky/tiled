import getpass
import os
from pathlib import Path, PurePosixPath
import secrets
import urllib.parse

import appdirs
import httpx
import msgpack

from .utils import (
    ASYNC_EVENT_HOOKS,
    DEFAULT_ACCEPTED_ENCODINGS,
    EVENT_HOOKS,
    handle_error,
    NotAvailableOffline,
    UNSET,
)


DEFAULT_TOKEN_CACHE = os.getenv(
    "TILED_TOKEN_CACHE", os.path.join(appdirs.user_config_dir("tiled"), "tokens")
)


def _token_directory(token_cache, netloc, username):
    return Path(
        token_cache,
        urllib.parse.quote_plus(
            netloc.decode()
        ),  # Make a valid filename out of hostname:port.
        username,
    )


def login(
    uri_or_profile,
    username=None,
    authentication_uri=None,
    verify=True,
    *,
    token_cache=DEFAULT_TOKEN_CACHE,
):
    context = _context_from_uri_or_profile(
        uri_or_profile, username, authentication_uri, token_cache, verify
    )
    # This has a side effect of storing the refresh token in the token_cache, if set.
    return context.authenticate()


def _context_from_uri_or_profile(
    uri_or_profile,
    username,
    authentication_uri,
    token_cache,
    verify,
    headers=None,
):
    headers = headers or {}
    headers.setdefault("accept-encoding", ",".join(DEFAULT_ACCEPTED_ENCODINGS))
    if uri_or_profile.startswith("http://") or uri_or_profile.startswith("https://"):
        # This looks like a URI.
        uri = uri_or_profile
        client = httpx.Client(
            base_url=uri,
            verify=verify,
            event_hooks=EVENT_HOOKS,
            headers=headers,
            timeout=httpx.Timeout(5.0, read=20.0),
        )
        context = Context(
            client,
            username=username,
            authentication_uri=authentication_uri,
            token_cache=token_cache,
        )
    else:
        from ..profiles import load_profiles

        # Is this a profile name?
        profiles = load_profiles()
        if uri_or_profile in profiles:
            profile_name = uri_or_profile
            filepath, profile_content = profiles[profile_name]
            if "uri" in profile_content:
                uri = profile_content["uri"]
                verify = profile_content.get("verify", True)
                headers.update(profile_content.get("headers", {}))
                client = httpx.Client(
                    base_url=uri,
                    verify=verify,
                    event_hooks=EVENT_HOOKS,
                    headers=headers,
                    timeout=httpx.Timeout(5.0, read=20.0),
                )
                context = Context(
                    client,
                    username=profile_content.get("username"),
                    authentication_uri=profile_content.get("authentication_uri"),
                    cache=profile_content.get("cache"),
                    offline=profile_content.get("offline", False),
                    token_cache=profile_content.get("token_cache", DEFAULT_TOKEN_CACHE),
                )
            elif "direct" in profile_content:
                # The profiles specifies that there is no server. We should create
                # an app ourselves and use it directly via ASGI.
                from ..config import construct_serve_tree_kwargs

                serve_tree_kwargs = construct_serve_tree_kwargs(
                    profile_content.pop("direct", None), source_filepath=filepath
                )
                context = context_from_tree(**serve_tree_kwargs, **profile_content)
            else:
                raise ValueError("Invalid profile content")
        else:
            raise TreeValueError(
                f"Not sure what to do with tree {uri_or_profile!r}. "
                "It does not look like a URI (it does not start with http[s]://) "
                "and it does not match any profiles."
            )
    return context


class TreeValueError(ValueError):
    pass


class CannotRefreshAuthentication(Exception):
    pass


class Context:
    """
    Wrap an httpx.Client with an optional cache and authentication functionality.
    """

    def __init__(
        self,
        client,
        authentication_uri=None,
        username=None,
        cache=None,
        offline=False,
        token_cache=DEFAULT_TOKEN_CACHE,
        app=None,
    ):
        authentication_uri = authentication_uri or "/"
        if not authentication_uri.endswith("/"):
            authentication_uri += "/"
        self._client = client
        self._authentication_uri = authentication_uri
        self._cache = cache
        self._username = username
        self._offline = offline
        if (username is not None) and isinstance(token_cache, (str, Path)):
            directory = _token_directory(
                token_cache, self._client.base_url.netloc, username
            )
            token_cache = TokenCache(directory)
        self._token_cache = token_cache
        self._app = app

        # Authenticate. If a valid refresh_token is available in the token_cache,
        # it will be used. Otherwise, this will prompt for a password.
        if (username is not None) and not offline:
            tokens = self.reauthenticate()
            access_token = tokens["access_token"]
            client.headers["Authorization"] = f"Bearer {access_token}"

        # Ask the server what its root_path is.
        handshake_request = self._client.build_request(
            "GET", "/", params={"root_path": None}
        )
        handshake_response = self._client.send(handshake_request)
        handle_error(handshake_response)
        data = handshake_response.json()
        base_path = data["meta"]["root_path"]
        url = httpx.URL(self._client.base_url)
        base_url = urllib.parse.urlunsplit(
            (url.scheme, url.netloc.decode(), base_path, {}, url.fragment)
        )
        client.base_url = base_url
        client.headers["x-base-url"] = base_url
        path_parts = list(PurePosixPath(url.path).relative_to(base_path).parts)
        if path_parts:
            # Strip "/metadata"
            path_parts.pop(0)
        self._path_parts = path_parts

    @property
    def offline(self):
        return self._offline

    @property
    def app(self):
        return self._app

    @offline.setter
    def offline(self, value):
        self._offline = bool(value)

    @property
    def path_parts(self):
        return self._path_parts

    @property
    def base_url(self):
        return self._client.base_url

    @property
    def event_hooks(self):
        "httpx.Client event hooks. This is exposed for testing."
        return self._client.event_hooks

    def get_content(self, path, accept=None, timeout=UNSET, stream=False, **kwargs):
        request = self._client.build_request("GET", path, **kwargs)
        if accept:
            request.headers["Accept"] = accept
        url = request.url.raw  # URL as tuple
        if self._offline:
            # We must rely on the cache alone.
            reservation = self._cache.get_reservation(url)
            if reservation is None:
                raise NotAvailableOffline(url)
            content = reservation.load_content()
            if content is None:
                # TODO Do we ever get here?
                raise NotAvailableOffline(url)
            return content
        if self._cache is None:
            # No cache, so we can use the client straightforwardly.
            response = self._send(request, stream=stream, timeout=timeout)
            handle_error(response)
            if response.headers.get("content-encoding") == "blosc":
                import blosc

                return blosc.decompress(response.content)
            return response.content
        # If we get this far, we have an online client and a cache.
        reservation = self._cache.get_reservation(url)
        try:
            if reservation is not None:
                request.headers["If-None-Match"] = reservation.etag
            response = self._send(request, stream=stream, timeout=timeout)
            handle_error(response)
            if response.status_code == 304:  # HTTP 304 Not Modified
                # Read from the cache
                content = reservation.load_content()
            elif response.status_code == 200:
                etag = response.headers.get("ETag")
                content = response.content
                if response.headers.get("content-encoding") == "blosc":
                    import blosc

                    content = blosc.decompress(content)
                # TODO Respect Cache-control headers (e.g. "no-store")
                if etag is not None:
                    # Write to cache.
                    self._cache.put_etag_for_url(url, etag)
                    self._cache.put_content(etag, content)
            else:
                raise NotImplementedError(
                    f"Unexpected status_code {response.status_code}"
                )
        finally:
            if reservation is not None:
                reservation.ensure_released()
        return content

    def get_json(self, path, stream=False, **kwargs):
        return msgpack.unpackb(
            self.get_content(
                path, accept="application/x-msgpack", stream=stream, **kwargs
            ),
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def _send(self, request, timeout=UNSET, stream=False, attempts=0):
        """
        Handle httpx's timeout API, which uses a special internal sentinel to mean
        "no timeout" and therefore must not be passed any value (including None)
        if we want no timeout.
        """
        if timeout is UNSET:
            response = self._client.send(request, stream=stream)
        else:
            response = self._client.send(request, stream=stream, timeout=timeout)
        if (response.status_code == 401) and (attempts == 0):
            # Try refreshing the token.
            # TODO Use a more targeted signal to know that refreshing the token will help.
            # Parse the error message? Add a special header from the server?
            if self._username is not None:
                tokens = self.reauthenticate()
                access_token = tokens["access_token"]
                auth_header = f"Bearer {access_token}"
                # Patch in the Authorization header for this request...
                request.headers["authorization"] = auth_header
                # And update the default headers for future requests.
                self._client.headers["Authorization"] = auth_header
                return self._send(request, timeout, stream=stream, attempts=1)
        return response

    def authenticate(self):
        # Make an initial "safe" request to let the server set the CSRF cookie.
        # TODO: Skip this if we already have a valid CSRF cookie for the authentication domain.
        # TODO: The server should support HEAD requests so we can do this more cheaply.
        handshake_request = self._client.build_request("GET", self._authentication_uri)
        # If an Authorization header is set, that's for the Resource server.
        # Do not include it in the request to the Authentication server.
        handshake_request.headers.pop("Authorization", None)
        handshake_response = self._send(handshake_request)
        handle_error(handshake_response)
        username = self._username or input("Username: ")
        password = getpass.getpass()
        form_data = {
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        token_request = self._client.build_request(
            "POST", f"{self._authentication_uri}token", data=form_data, headers={}
        )
        token_request.headers.pop("Authorization", None)
        token_response = self._client.send(token_request)
        handle_error(token_response)
        tokens = token_response.json()
        if self._token_cache is not None:
            # We are using a token cache. Store the new refresh token.
            self._token_cache["refresh_token"] = tokens["refresh_token"]
        return tokens

    def reauthenticate(self, prompt_on_failure=True):
        try:
            return self._refresh()
        except CannotRefreshAuthentication:
            if prompt_on_failure:
                return self.authenticate()
            raise

    def _refresh(self):
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        # Make an initial "safe" request to let the server set the CSRF cookie.
        # TODO: Skip this if we already have a valid CSRF cookie for the authentication domain.
        # TODO: The server should support HEAD requests so we can do this more cheaply.
        handshake_request = self._client.build_request("GET", self._authentication_uri)
        # If an Authorization header is set, that's for the Resource server.
        # Do not include it in the request to the Authentication server.
        handshake_request.headers.pop("Authorization", None)
        handshake_response = self._client.send(handshake_request)
        handle_error(handshake_response)
        if self._token_cache is None:
            # We are not using a token cache.
            raise CannotRefreshAuthentication("No token cache was given")
        # We are using a token_cache.
        try:
            refresh_token = self._token_cache["refresh_token"]
        except KeyError:
            raise CannotRefreshAuthentication(
                "No refresh token was found in token cache"
            )
        # There is a refresh token in the cache.
        token_request = self._client.build_request(
            "POST",
            f"{self._authentication_uri}token/refresh",
            json={"refresh_token": refresh_token},
            headers={"x-csrf": self._client.cookies["tiled_csrf"]},
        )
        token_request.headers.pop("Authorization", None)
        token_response = self._client.send(token_request)
        if token_response.status_code == 401:
            # Refreshing the token failed.
            # Discard the expired (or otherwise invalid) refresh_token file.
            self._token_cache.pop("refresh_token", None)
            raise CannotRefreshAuthentication(
                "Server rejected attempt to refresh token"
            )
        handle_error(token_response)
        tokens = token_response.json()
        # If we get this far, reauthentication worked.
        # Store the new refresh token.
        self._token_cache["refresh_token"] = tokens["refresh_token"]
        return tokens


def context_from_tree(
    tree,
    authentication,
    server_settings,
    *,
    query_registry=None,
    serialization_registry=None,
    compression_registry=None,
    cache=None,
    offline=False,
    token_cache=DEFAULT_TOKEN_CACHE,
    username=None,
    headers=None,
):
    from ..server.app import serve_tree

    authentication = authentication or {}
    server_settings = server_settings or {}
    params = {}
    headers = headers or {}
    headers.setdefault("accept-encoding", ",".join(DEFAULT_ACCEPTED_ENCODINGS))
    if (authentication.get("authenticator") is None) and (
        authentication.get("single_user_api_key") is None
    ):
        # Generate the key here instead of letting serve_tree do it for us,
        # so that we can give it to the client below.
        single_user_api_key = os.getenv(
            "TILED_SINGLE_USER_API_KEY", secrets.token_hex(32)
        )
        authentication["single_user_api_key"] = single_user_api_key
        params["api_key"] = single_user_api_key
    app = serve_tree(
        tree,
        authentication,
        server_settings,
        query_registry=query_registry,
        serialization_registry=serialization_registry,
        compression_registry=compression_registry,
    )

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
        event_hooks=ASYNC_EVENT_HOOKS,
        headers=headers,
        timeout=httpx.Timeout(5.0, read=20.0),
    )
    # Block for application startup.
    try:
        client.wait_until_ready(10)
    except TimeoutError:
        raise TimeoutError("Application startup has timed out.")
    # TODO How to close the httpx.AsyncClient more cleanly?
    import atexit

    atexit.register(client.close)
    return Context(
        client,
        cache=cache,
        offline=offline,
        token_cache=token_cache,
        username=username,
        app=app,
    )


class TokenCache:
    "A (partial) dict interface backed by files with restrictive permissions"

    def __init__(self, directory):
        self._directory = Path(directory)
        self._directory.mkdir(exist_ok=True, parents=True)

    def __getitem__(self, key):
        filepath = self._directory / key
        try:
            with open(filepath, "r") as file:
                return file.read()
        except FileNotFoundError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        if not isinstance(value, str):
            raise ValueError("Expected string value, got {value!r}")
        filepath = self._directory / key
        filepath.touch(mode=0o600)  # Set permissions.
        with open(filepath, "w") as file:
            file.write(value)

    def __delitem__(self, key):
        filepath = self._directory / key
        filepath.unlink(missing_ok=False)

    def pop(self, key, fallback=None):
        filepath = self._directory / key
        try:
            with open(filepath, "r") as file:
                content = file.read()
        except FileNotFoundError:
            content = fallback
        filepath.unlink(missing_ok=True)
        return content
