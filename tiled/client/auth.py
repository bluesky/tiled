import enum
import os
import threading
import urllib.parse
from pathlib import Path

import appdirs
import httpx

from .utils import handle_error


class CannotRefreshAuthentication(Exception):
    pass


class PromptForReauthentication(enum.Enum):
    AT_INIT = "at_init"
    NEVER = "never"
    ALWAYS = "always"


DEFAULT_TOKEN_CACHE = os.getenv(
    "TILED_TOKEN_CACHE", os.path.join(appdirs.user_config_dir("tiled"), "tokens")
)


def logout(uri_or_profile, *, token_cache=DEFAULT_TOKEN_CACHE):
    """
    Logout of a given session.

    If not logged in, calling this function has no effect.

    Parameters
    ----------
    uri_or_profile : str
    token_cache : str or Path, optional

    Returns
    -------
    netloc : str
    """
    netloc = _netloc_from_uri_or_profile(uri_or_profile)
    # Find the directory associated with this specific Tiled server.
    directory = Path(
        token_cache,
        urllib.parse.quote_plus(
            netloc.decode()
        ),  # Make a valid filename out of hostname:port.
    )
    for filepath in [directory / "refresh_token", directory / "access_token"]:
        # filepath.unlink(missing_ok=False)  # Python 3.8+
        try:
            filepath.unlink()
        except FileNotFoundError:
            pass
    return netloc


def sessions(token_cache=DEFAULT_TOKEN_CACHE):
    """
    List all sessions.

    Note that this may include expired sessions. It does not confirm that
    any cached tokens are still valid.

    Parameters
    ----------
    token_cache : str or Path, optional

    Returns
    -------
    tokens : dict
        Maps netloc to refresh_token
    """
    tokens = {}
    for directory in Path(token_cache).iterdir():
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


def logout_all(token_cache=DEFAULT_TOKEN_CACHE):
    """
    Logout of a all sessions.

    If not logged in to any sessions, calling this function has no effect.

    Parameters
    ----------
    token_cache : str or Path, optional

    Returns
    -------
    logged_out_from : list
        List of netloc of logged-out sessions
    """
    logged_out_from = []
    for directory in Path(token_cache).iterdir():
        if not directory.is_dir():
            # Some stray file. Ignore it.
            continue
        for filepath in [directory / "refresh_token", directory / "access_token"]:
            try:
                filepath.unlink()
            except FileNotFoundError:
                pass
            else:
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


class TiledAuth(httpx.Auth):
    def __init__(self, refresh_url, csrf_token, token_directory):
        self.refresh_url = refresh_url
        self.csrf_token = csrf_token
        self.token_directory = token_directory
        self.token_directory.mkdir(exist_ok=True, parents=True)
        self._sync_lock = threading.RLock()
        # self._async_lock = asyncio.Lock()
        self.tokens = {}

    def sync_get_token(self, key, reload_from_disk=False):
        if not reload_from_disk:
            # Use in-memory cached copy.
            try:
                return self.tokens[key]
            except Exception:
                pass
        with self._sync_lock:
            filepath = self.token_directory / key
            try:
                with open(filepath, "r") as file:
                    token = file.read()
                    self.tokens[key] = token
                    return token
            except FileNotFoundError:
                return None

    def sync_set_token(self, key, value):
        with self._sync_lock:
            if not isinstance(value, str):
                raise ValueError("Expected string value, got {value!r}")
            filepath = self.token_directory / key
            filepath.touch(mode=0o600)  # Set permissions.
            with open(filepath, "w") as file:
                file.write(value)

    def sync_clear_token(self, key):
        with self._sync_lock:
            self.tokens.pop(key, None)
            filepath = self.token_directory / key
            # filepath.unlink(missing_ok=False)  # Python 3.8+
            try:
                filepath.unlink()
            except FileNotFoundError:
                pass

    def sync_auth_flow(self, request, attempt=0):
        access_token = self.sync_get_token("access_token")
        if access_token is not None:
            request.headers["Authorization"] = f"Bearer {access_token}"
            response = yield request
        if (access_token is None) or (response.status_code == 401):
            # Maybe the token cached in memory is stale.
            maybe_new_access_token = self.sync_get_token(
                "access_token", reload_from_disk=True
            )
            if (attempt < 2) and (maybe_new_access_token != access_token):
                return (yield from self.sync_auto_flow(request, attempt=1 + attempt))
            if access_token is not None:
                # The access token is stale or otherwise invalid. Discard.
                self.sync_clear_token("access_token")
            # Begin refresh flow to get a new access token.
            refresh_token = self.sync_get_token("refresh_token", reload_from_disk=True)
            if refresh_token is None:
                raise CannotRefreshAuthentication(
                    "No refresh token was found in token cache. "
                    "Provide fresh credentials. "
                    "For a given client c, use c.context.authenticate()."
                )
            token_request = httpx.Request(
                "POST",
                self.refresh_url,
                json={"refresh_token": refresh_token},
                # Submit CSRF token in both header and cookie.
                # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
                headers={"x-csrf": self.csrf_token},
            )
            token_response = yield token_request
            if token_response.status_code == 401:
                # Refreshing the token failed.
                # Discard the expired (or otherwise invalid) refresh_token.
                self.sync_clear_token("refresh_token")
                raise CannotRefreshAuthentication(
                    "Server rejected attempt to refresh token. "
                    "Provide fresh credentials. "
                    "For a given client c, use c.context.authenticate()."
                )
            handle_error(token_response)
            tokens = token_response.json()
            # If we get this far, refreshing authentication worked.
            # Store the new refresh token.
            self.sync_set_token("refresh_token", tokens["refresh_token"])
            self.sync_set_token("access_token", tokens["access_token"])
            request.headers["Authorization"] = f"Bearer {tokens['access_token']}"
        yield request

    async def async_get_token(self, key):
        raise NotImplementedError("Async support is planned but not yet implemented.")

    async def async_set_token(self, key, token):
        raise NotImplementedError("Async support is planned but not yet implemented.")

    async def async_clear_token(self, key):
        raise NotImplementedError("Async support is planned but not yet implemented.")

    async def async_auth_flow(self, request):
        raise NotImplementedError("Async support is planned but not yet implemented.")
