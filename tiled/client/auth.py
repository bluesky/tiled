import asyncio
import enum
import os
import threading
import urllib.parse
from pathlib import Path

import appdirs
import httpx


class CannotRefreshAuthentication(Exception):
    pass


class PromptForReauthentication(enum.Enum):
    AT_INIT = "at_init"
    NEVER = "never"
    ALWAYS = "always"


DEFAULT_TOKEN_CACHE = os.getenv(
    "TILED_TOKEN_CACHE", os.path.join(appdirs.user_config_dir("tiled"), "tokens")
)


def token_directory(token_cache, netloc):
    return Path(
        token_cache,
        urllib.parse.quote_plus(
            netloc.decode()
        ),  # Make a valid filename out of hostname:port.
    )


def logout(uri_or_profile, *, token_cache=DEFAULT_TOKEN_CACHE):
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
        directory = token_directory(token_cache, netloc)
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


class TiledAuth(httpx.Auth):
    def __init__(self):
        self._sync_lock = threading.RLock()
        self._async_lock = asyncio.Lock()

    def sync_get_token(self):
        with self._sync_lock:
            ...

    def sync_auth_flow(self, request):
        token = self.sync_get_token()
        request.headers["Authorization"] = f"Token {token}"
        yield request

    async def async_get_token(self):
        async with self._async_lock:
            ...

    async def async_auth_flow(self, request):
        token = await self.async_get_token()
        request.headers["Authorization"] = f"Token {token}"
        yield request


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
        # filepath.unlink(missing_ok=False)  # Python 3.8+
        try:
            filepath.unlink()
        except FileNotFoundError:
            pass

    def pop(self, key, fallback=None):
        filepath = self._directory / key
        try:
            with open(filepath, "r") as file:
                content = file.read()
        except FileNotFoundError:
            content = fallback
        # filepath.unlink(missing_ok=True)  # Python 3.8+
        try:
            filepath.unlink()
        except FileNotFoundError:
            pass
        return content
