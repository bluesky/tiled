import getpass
import os
from pathlib import Path, PurePosixPath
import urllib.parse

import appdirs
import httpx
import msgpack

from .utils import (
    client_from_tree,
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
    uri_or_profile, username=None, authentication_uri=None, *, token_cache=DEFAULT_TOKEN_CACHE
):
    client, _uri = _client_and_uri_from_uri_or_profile(tree)
    # This has a side effect of storing the refresh token in the token_cache, if set.
    return authenticate(client, username, authentication_uri, token_cache=token_cache)


def client_and_path_from_uri(
    uri, username=None, authentication_uri=None, token_cache=None, **kwargs
):
    headers = kwargs.get("headers", {})
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
    params = kwargs.get("params", {})
    params.update(urllib.parse.urlencode(parsed_query, doseq=True))

    # Construct the URL *without* the params, which we will pass in separately.
    handshake_url = urllib.parse.urlunsplit(
        (url.scheme, url.netloc.decode(), url.path, {}, url.fragment)
    )

    client = httpx.Client(headers=headers, params=params, **kwargs)
    if (username is not None) and (authentication_uri is not None):
        tokens = reauthenticate(
            client, username, authentication_uri, token_cache=token_cache
        )
        access_token = tokens["access_token"]
        client.headers["Authorization"] = f"Bearer {access_token}"
    # First, ask the server what its root_path is.
    # This is the only place where we use client.get *directly*, circumventing
    # the usual "get with cache" logic.
    response = client.get(handshake_url, params={"root_path": None})
    handle_error(response)
    data = response.json()
    base_path = data["meta"]["root_path"]
    base_url = urllib.parse.urlunsplit(
        (url.scheme, url.netloc.decode(), base_path, {}, url.fragment)
    )
    client.base_url = base_url
    path_parts = list(PurePosixPath(url.path).relative_to(base_path).parts)
    if path_parts:
        # Strip "/metadata"
        path_parts.pop(0)
    return client, path_parts


def _client_and_path_from_uri_or_profile(uri_or_profile):
    if uri_or_profile.startswith("http://") or uri_or_profile.startswith("https://"):
        # This looks like a URI.
        uri = uri_or_profile
        return client_and_path_from_uri(uri)
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
                client, _ = client_and_path_from_uri(uri, verify=verify)
                return client, uri
            elif "direct" in profile_content:
                # The profiles specifies that there is no server. We should create
                # an app ourselves and use it directly via ASGI.
                from ..config import construct_serve_tree_kwargs

                serve_tree_kwargs = construct_serve_tree_kwargs(
                    profile_content.pop("direct", None), source_filepath=filepath
                )
                client = client_from_tree(**serve_tree_kwargs)
                return client, "/"
            else:
                raise ValueError("Invalid profile content")

    raise TreeValueError(
        f"Not sure what to do with tree {uri_or_profile!r}. "
        "It does not look like a URI (it does not start with http[s]://) "
        "and it does not match any profiles."
    )


class TreeValueError(ValueError):
    pass


class CannotRefreshAuthentication(Exception):
    pass


class CoreClient:
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
    ):
        authentication_uri = authentication_uri or "/"
        if not authentication_uri.endswith("/"):
            authentication_uri += "/"
        self._client = client
        self._authentication_uri = authentication_uri
        self._cache = cache
        self._username = username
        self._offline = offline
        self._token_cache = token_cache
        if username is not None:
            tokens = reauthenticate(
                self, username, authentication_uri, token_cache=token_cache
            )
            access_token = tokens["access_token"]
            client.headers["Authorization"] = f"Bearer {access_token}"

    def get_content(self, path, accept=None, timeout=UNSET, **kwargs):
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
            response = self._send(request, timeout=timeout)
            handle_error(response)
            return response.content
        # If we get this far, we have an online client and a cache.
        reservation = self._cache.get_reservation(url)
        try:
            if reservation is not None:
                request.headers["If-None-Match"] = reservation.etag
            response = self._send(request, timeout=timeout)
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

    def get_json(self, path, **kwargs):
        return msgpack.unpackb(
            self.get_content(path, accept="application/x-msgpack", **kwargs),
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def _send(self, request, timeout, attempts=0):
        """
        Handle httpx's timeout API, which uses a special internal sentinel to mean
        "no timeout" and therefore must not be passed any value (including None)
        if we want no timeout.
        """
        if timeout is UNSET:
            response = self._client.send(request)
        else:
            response = self._client.send(request, timeout=timeout)
        if (response.status_code == 401) and (attempts == 0):
            # Try refreshing the token.
            # TODO Use a more targeted signal to know that refreshing the token will help.
            # Parse the error message? Add a special header from the server?
            if self._username is not None:
                tokens = reauthenticate(
                    self._client,
                    self._username,
                    self._authentication_uri,
                    token_cache=self._token_cache,
                )
                access_token = tokens["access_token"]
                auth_header = f"Bearer {access_token}"
                # Patch in the Authorization header for this request...
                request.headers["authorization"] = auth_header
                # And update the default headers for future requests.
                self._client.headers["Authorization"] = auth_header
                return self._send(request, timeout, attempts=1)
        return response

    def authenticate(self):
        if self._offline:
            return
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
        if self._token_cache:
            # We are using a token cache. Store the new refresh token.
            directory = _token_directory(
                self._token_cache, self._client.base_url.netloc, username
            )
            directory.mkdir(exist_ok=True, parents=True)
            filepath = directory / "refresh_token"
            filepath.touch(mode=0o600)  # Set permissions.
            with open(filepath, "w") as file:
                file.write(tokens["refresh_token"])
        return tokens

    def reauthenticate(self, prompt_on_failure=True):
        if self._offline:
            return
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
        if not self._token_cache:
            # We are not using a token cache.
            raise CannotRefreshAuthentication("No token cache was given")
        # We are using a token_cache.
        directory = _token_directory(
            self._token_cache, self._client.base_url.netloc, self._username
        )
        filepath = directory / "refresh_token"
        if filepath.is_file():
            # There is a token file.
            with open(filepath, "r") as file:
                refresh_token = file.read()
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
                filepath.unlink(missing_ok=True)
                raise CannotRefreshAuthentication(
                    "Server rejected attempt to refresh token"
                )
        else:
            raise CannotRefreshAuthentication(
                "No refresh token was found in token cache"
            )
        handle_error(token_response)
        tokens = token_response.json()
        # If we get this far, reauthentication worked.
        # Store the new refresh token.
        filepath.touch(mode=0o600)  # Set permissions.
        with open(filepath, "w") as file:
            file.write(tokens["refresh_token"])
        return tokens
