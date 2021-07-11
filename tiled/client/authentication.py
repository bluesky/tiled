import getpass
import os
from pathlib import Path
import urllib.parse

import appdirs

from .utils import (
    client_and_path_from_uri,
    client_from_tree,
    handle_error,
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
    tree, username=None, authentication_uri=None, *, token_cache=DEFAULT_TOKEN_CACHE
):
    client, _uri = _client_and_uri_from_uri_or_profile(tree)
    # This has a side effect of storing the refresh token in the token_cache, if set.
    return authenticate(client, username, authentication_uri, token_cache=token_cache)


def authenticate(
    client, username, authentication_uri=None, *, token_cache=DEFAULT_TOKEN_CACHE
):
    authentication_uri = authentication_uri or "/"
    if not authentication_uri.endswith("/"):
        authentication_uri += "/"
    # Make an initial "safe" request to let the server set the CSRF cookie.
    # TODO: Skip this if we already have a valid CSRF cookie for the authentication domain.
    # TODO: The server should support HEAD requests so we can do this more cheaply.
    handshake_request = client.build_request("GET", authentication_uri)
    # If an Authorization header is set, that's for the Resource server.
    # Do not include it in the request to the Authentication server.
    handshake_request.headers.pop("Authorization", None)
    handshake_response = client.send(handshake_request)
    handle_error(handshake_response)
    username = username or input("Username: ")
    password = getpass.getpass()
    form_data = {"grant_type": "password", "username": username, "password": password}
    token_request = client.build_request(
        "POST", f"{authentication_uri}token", data=form_data, headers={}
    )
    token_request.headers.pop("Authorization", None)
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
    return data
    access_token = token_response.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {access_token}"


def reauthenticate(
    client,
    username,
    authentication_uri=None,
    *,
    token_cache=DEFAULT_TOKEN_CACHE,
    prompt_on_failure=True,
):
    try:
        _refresh(client, username, authentication_uri, token_cache=token_cache)
    except CannotRefreshAuthentication:
        if prompt_on_failure:
            return authenticate(
                client, username, authentication_uri, token_cache=token_cache
            )
        else:
            raise


def _refresh(client, username, authentication_uri, *, token_cache=DEFAULT_TOKEN_CACHE):
    # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
    authentication_uri = authentication_uri or "/"
    if not authentication_uri.endswith("/"):
        authentication_uri += "/"
    # Make an initial "safe" request to let the server set the CSRF cookie.
    # TODO: Skip this if we already have a valid CSRF cookie for the authentication domain.
    # TODO: The server should support HEAD requests so we can do this more cheaply.
    handshake_request = client.build_request("GET", authentication_uri)
    # If an Authorization header is set, that's for the Resource server.
    # Do not include it in the request to the Authentication server.
    handshake_request.headers.pop("Authorization", None)
    handshake_response = client.send(handshake_request)
    handle_error(handshake_response)
    if not token_cache:
        # We are not using a token cache.
        raise CannotRefreshAuthentication("No token cache was given")
    # We are using a token_cache.
    directory = _token_directory(token_cache, client.base_url.netloc, username)
    filepath = directory / "refresh_token"
    if filepath.is_file():
        # There is a token file.
        with open(filepath, "r") as file:
            refresh_token = file.read()
        token_request = client.build_request(
            "POST",
            f"{authentication_uri}token/refresh",
            json={"refresh_token": refresh_token},
            headers={"x-csrf": client.cookies["tiled_csrf"]},
        )
        token_request.headers.pop("Authorization", None)
        token_response = client.send(token_request)
        if token_response.status_code == 401:
            # Refreshing the token failed.
            # Discard the expired (or otherwise invalid) refresh_token file.
            filepath.unlink(missing_ok=True)
            raise CannotRefreshAuthentication(
                "Server rejected attempt to refresh token"
            )
    else:
        raise CannotRefreshAuthentication("No refresh token was found in token cache")
    handle_error(token_response)
    tokens = token_response.json()
    # If we get this far, reauthentication worked.
    # Store the new refresh token.
    filepath.touch(mode=0o600)  # Set permissions.
    with open(filepath, "w") as file:
        file.write(tokens["refresh_token"])
    return tokens


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
                PLACEHOLDER = "__process_local_app__"
                return client, PLACEHOLDER
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
