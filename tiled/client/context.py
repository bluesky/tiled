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
    handle_error,
    NotAvailableOffline,
)
from ..utils import DictView


DEFAULT_TOKEN_CACHE = os.getenv(
    "TILED_TOKEN_CACHE", os.path.join(appdirs.user_config_dir("tiled"), "tokens")
)


def _token_directory(token_cache, netloc):
    return Path(
        token_cache,
        urllib.parse.quote_plus(
            netloc.decode()
        ),  # Make a valid filename out of hostname:port.
    )


def logout(
    uri_or_profile,
    *,
    token_cache=DEFAULT_TOKEN_CACHE,
):
    """
    Logout of a given session.

    If not logged in, calling this function has no effect.

    Parameters
    ----------
    uri_or_profile : str
    token_directory : str or Path, optional

    Returns
    -------
    netloc : str
    """
    if isinstance(token_cache, (str, Path)):
        netloc = _netloc_from_uri_or_profile(uri_or_profile)
        directory = _token_directory(
            token_cache,
            netloc,
        )
    else:
        netloc = None  # unknowable
    token_cache = TokenCache(directory)
    token_cache.pop("refresh_token", None)
    return netloc


def sessions(token_directory=DEFAULT_TOKEN_CACHE):
    """
    List all sessions.

    Note that this may include expired sessions. It does not confirm that
    any cached tokens are still valid.

    Parameters
    ----------
    token_directory : str or Path, optional

    Returns
    -------
    tokens : dict
        Maps netloc to refresh_token
    """
    tokens = {}
    for directory in Path(token_directory).iterdir():
        if not directory.is_dir():
            # Some stray file. Ignore it.
            continue
        refresh_token_file = directory / "refresh_token"
        netloc = directory.name
        if refresh_token_file.is_file():
            with open(refresh_token_file) as file:
                token = file.read()
            tokens[netloc] = token
    return tokens


def logout_all(token_directory=DEFAULT_TOKEN_CACHE):
    """
    Logout of a all sessions.

    If not logged in to any sessions, calling this function has no effect.

    Parameters
    ----------
    token_directory : str or Path, optional

    Returns
    -------
    logged_out_from : list
        List of netloc of logged-out sessions
    """
    logged_out_from = []
    for directory in Path(token_directory).iterdir():
        if not directory.is_dir():
            # Some stray file. Ignore it.
            continue
        refresh_token_file = directory / "refresh_token"
        if refresh_token_file.is_file():
            refresh_token_file.unlink()
            netloc = directory.name
            logged_out_from.append(netloc)
    return logged_out_from


def _netloc_from_uri_or_profile(uri_or_profile):
    if uri_or_profile.startswith("http://") or uri_or_profile.startswith("https://"):
        # This looks like a URI.
        uri = uri_or_profile
    else:
        # Is this a profile name?
        from ..profiles import load_profiles

        profiles = load_profiles()
        if uri_or_profile in profiles:
            profile_name = uri_or_profile
            _, profile_content = profiles[profile_name]
            if "uri" in profile_content:
                uri = profile_content["uri"]
            else:
                raise ValueError(
                    "Logout does not apply to profiles with inline ('direct') "
                    "server configuration."
                )
        else:
            raise ValueError(
                f"Not sure what to do with tree {uri_or_profile!r}. "
                "It does not look like a URI (it does not start with http[s]://) "
                "and it does not match any profiles."
            )
    return httpx.URL(uri).netloc


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
        self._token_cache_or_root_directory = token_cache
        if isinstance(token_cache, (str, Path)):
            directory = _token_directory(
                token_cache,
                self._client.base_url.netloc,
            )
            token_cache = TokenCache(directory)
        self._token_cache = token_cache
        # The token *cache* is optional. The tokens attrbiute is always present,
        # and it isn't actually used for anything internally. It's just a view
        # of the current tokens.
        self._tokens = {}
        self._app = app

        # Make an initial "safe" request to let the server set the CSRF cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        self._handshake_data = self.get_json(self._authentication_uri)

        # Ask the server what its root_path is.
        if (not offline) and (
            self._handshake_data["authentication"]["required"] or (username is not None)
        ):
            if self._handshake_data["authentication"]["type"] in (
                "password",
                "external",
            ):
                # Authenticate. If a valid refresh_token is available in the token_cache,
                # it will be used. Otherwise, this will prompt for input from the stdin.
                tokens = self.reauthenticate()
                access_token = tokens["access_token"]
                client.headers["Authorization"] = f"Bearer {access_token}"
        base_path = self._handshake_data["meta"]["root_path"]
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
    def tokens(self):
        "A view of the current access and refresh tokens."
        return DictView(self._tokens)

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

    def get_content(self, path, accept=None, stream=False, **kwargs):
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
            response = self._send(request, stream=stream)
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
            response = self._send(request, stream=stream)
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

    def _send(self, request, stream=False, attempts=0):
        """
        If sending results in an authentication error, reauthenticate.
        """
        response = self._client.send(request, stream=stream)
        if (response.status_code == 401) and (attempts == 0):
            # Try refreshing the token.
            tokens = self.reauthenticate()
            # The line above updated self._client.headers["authorization"]
            # so we will have a fresh token for the next call to
            # client.build_request(...), but we need to retroactively patch the
            # authorization header for this request and then re-send.
            access_token = tokens["access_token"]
            auth_header = f"Bearer {access_token}"
            request.headers["authorization"] = auth_header
            return self._send(request, stream=stream, attempts=1)
        return response

    def authenticate(self):
        "Authenticate. Prompt for password or access code (refresh token)."
        auth_type = self._handshake_data["authentication"]["type"]
        if auth_type == "password":
            username = self._username or input("Username: ")
            password = getpass.getpass()
            form_data = {
                "grant_type": "password",
                "username": username,
                "password": password,
            }
            token_request = self._client.build_request(
                "POST",
                f"{self._authentication_uri}auth/token",
                data=form_data,
                headers={},
            )
            token_request.headers.pop("Authorization", None)
            token_response = self._client.send(token_request)
            handle_error(token_response)
            tokens = token_response.json()
            refresh_token = tokens["refresh_token"]
        elif auth_type == "external":
            endpoint = self._handshake_data["authentication"]["endpoint"]
            print(
                f"""
Navigate web browser to this address to obtain access code:

{endpoint}

"""
            )
            while True:
                # The proper term for this is 'refresh token' but that may be
                # confusing jargon to the end user, so we say "access code".
                raw_refresh_token = getpass.getpass("Access code (quotes optional): ")
                if not raw_refresh_token:
                    print("No access token given. Failed.")
                    break
                # Remove any accidentally-included quotes.
                refresh_token = raw_refresh_token.replace('"', "")
                # Immediately refresh to (1) check that the copy/paste worked and
                # (2) obtain an access token as well.
                try:
                    tokens = self._refresh(refresh_token=refresh_token)
                except CannotRefreshAuthentication:
                    print(
                        "That didn't work. Try pasting the access code again, or press Enter to escape."
                    )
                else:
                    break
            confirmation_message = self._handshake_data["authentication"][
                "confirmation_message"
            ]
            if confirmation_message:
                username = username = self.whoami()
                print(confirmation_message.format(username=username))
        elif auth_type == "api_key":
            raise ValueError(
                "authenticate() method is not applicable to API key authentication"
            )
        else:
            raise ValueError(f"Server has unknown authentication type {auth_type!r}")
        if self._token_cache is not None:
            # We are using a token cache. Store the new refresh token.
            self._token_cache["refresh_token"] = refresh_token
        self._tokens.update(
            refresh_token=tokens["refresh_token"], access_token=tokens["access_token"]
        )
        return tokens

    def reauthenticate(self, prompt_on_failure=True):
        "Refresh authentication. Prompt if refresh fails."
        try:
            return self._refresh()
        except CannotRefreshAuthentication:
            if prompt_on_failure:
                return self.authenticate()
            raise

    def whoami(self):
        "Return username."
        request = self._client.build_request(
            "GET",
            f"{self._authentication_uri}auth/whoami",
        )
        response = self._client.send(request)
        handle_error(response)
        return response.json()["username"]

    def logout(self):
        """
        Clear the access token and the cached refresh token.

        This method is idempotent.
        """
        self._client.headers.pop("Authorization", None)
        if self._token_cache is not None:
            self._token_cache.pop("refresh_token", None)
        self._tokens.clear()

    def _refresh(self, refresh_token=None):
        if refresh_token is None:
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
        token_request = self._client.build_request(
            "POST",
            f"{self._authentication_uri}auth/token/refresh",
            json={"refresh_token": refresh_token},
            # Submit CSRF token in both header and cookie.
            # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
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
        # Update the client's Authentication header.
        access_token = tokens["access_token"]
        auth_header = f"Bearer {access_token}"
        self._client.headers["authorization"] = auth_header
        self._tokens.update(
            refresh_token=tokens["refresh_token"], access_token=tokens["access_token"]
        )
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

    # By default make it "public" because there is no way to
    # secure access from inside the same process anyway.
    authentication = authentication or {"allow_anonymous_access": True}
    server_settings = server_settings or {}
    params = {}
    headers = headers or {}
    headers.setdefault("accept-encoding", ",".join(DEFAULT_ACCEPTED_ENCODINGS))
    # If a single-user API key will be used, generate the key here instead of
    # letting serve_tree do it for us, so that we can give it to the client
    # below.
    if (
        (authentication.get("authenticator") is None)
        and (not authentication.get("allow_anonymous_access", False))
        and (authentication.get("single_user_api_key") is None)
    ):
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
