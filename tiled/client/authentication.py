import getpass
import os
from pathlib import Path
import urllib.parse

import appdirs

from .utils import (
    client_and_path_from_uri,
    client_from_catalog,
    handle_error,
)


DEFAULT_TOKEN_CACHE = os.getenv(
    "TILED_TOKEN_CACHE", os.path.join(appdirs.user_config_dir("tiled"), "tokens")
)


def _token_directory(token_cache, netloc, username):
    return Path(
        token_cache,
        urllib.parse.quote_plus(netloc),  # Make a valid filename out of hostname:port.
        username,
    )


def login(catalog, username=None, *, token_cache=DEFAULT_TOKEN_CACHE):
    client, uri = _client_and_uri_from_uri_or_profile(catalog)
    authenticate_client(client, username, token_cache=token_cache)


def authenticate_client(client, username, *, token_cache=DEFAULT_TOKEN_CACHE):
    if "tiled_csrf" not in client.cookies:
        # Make an initial "safe" request to let the server set the CSRF cookie.
        # We could also use this to check the API version.
        handshake_request = client.build_request("GET", "/")
        handshake_response = client.send(handshake_request)
        handle_error(handshake_response)
    username = username or input("Username: ")
    password = getpass.getpass()
    form_data = {"grant_type": "password", "username": username, "password": password}
    token_request = client.build_request("POST", "/token", data=form_data)
    token_response = client.send(token_request)
    handle_error(token_response)
    data = token_response.json()
    if token_cache:
        # We are using a token cache. Store the new refresh token.
        directory = _token_directory(token_cache, client.base_url.netloc, username)
        directory.mkdir(exist_ok=True, parents=True)
        filepath = directory / "refresh_token"
        filepath.touch(mode=0o600)  # Set permissions.
        with open(filepath, "w") as file:
            file.write(data["refresh_token"])
    access_token = token_response.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {access_token}"


def reauthenticate_client(
    client, username, *, token_cache=DEFAULT_TOKEN_CACHE, prompt_on_failure=True
):
    try:
        _reauthenticate_client(client, username, token_cache=token_cache)
    except CannotRefreshAuthentication:
        if prompt_on_failure:
            return authenticate_client(client, username, token_cache=token_cache)
        else:
            raise


def _reauthenticate_client(client, username, *, token_cache=DEFAULT_TOKEN_CACHE):
    # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
    if "tiled_csrf" not in client.cookies:
        # Make an initial "safe" request to let the server set the CSRF cookie.
        # We could also use this to check the API version.
        handshake_request = client.build_request("GET", "/")
        handshake_response = client.send(handshake_request)
        handle_error(handshake_response)
    if token_cache:
        # We are using a token_cache.
        directory = _token_directory(token_cache, client.base_url.netloc, username)
        filepath = directory / "refresh_token"
        if filepath.is_file():
            # There is a token file.
            with open(filepath, "r") as file:
                refresh_token = file.read()
            token_request = client.build_request(
                "POST",
                "/token/refresh",
                json={"refresh_token": refresh_token},
                headers={"x-csrf": client.cookies["tiled_csrf"]},
            )
            token_response = client.send(token_request)
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

    else:
        # We are not using a token cache.
        raise CannotRefreshAuthentication("No token cache was given")
    handle_error(token_response)
    access_token = token_response.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {access_token}"


def _client_and_uri_from_uri_or_profile(uri_or_profile):
    if uri_or_profile.startswith("http://") or uri_or_profile.startswith("https://"):
        # This looks like a URI.
        uri = uri_or_profile
        client, _ = client_and_path_from_uri(uri)
        return client, uri
    else:
        from ..profiles import load_profiles

        # Is this a profile name?
        profiles = load_profiles()
        if uri_or_profile in profiles:
            profile_name = uri_or_profile
            filepath, profile_content = profiles[profile_name]
            if "uri" in profile_content:
                uri = profile_content["uri"]
                client, _ = client_and_path_from_uri(uri)
                return client, uri
            elif "direct" in profile_content:
                # The profiles specifies that there is no server. We should create
                # an app ourselves and use it directly via ASGI.
                from ..config import construct_serve_catalog_kwargs

                serve_catalog_kwargs = construct_serve_catalog_kwargs(
                    profile_content.pop("direct", None), source_filepath=filepath
                )
                client = client_from_catalog(**serve_catalog_kwargs)
                PLACEHOLDER = "__process_local_app__"
                return client, PLACEHOLDER
            else:
                raise ValueError("Invalid profile content")

    raise CatalogValueError(
        f"Not sure what to do with catalog {uri_or_profile!r}. "
        "It does not look like a URI (it does not start with http[s]://) "
        "and it does not match any profiles."
    )


class CatalogValueError(ValueError):
    pass


class CannotRefreshAuthentication(Exception):
    pass
