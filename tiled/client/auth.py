import os
from pathlib import Path

import appdirs
import httpx

from .utils import SerializableLock, handle_error


class CannotRefreshAuthentication(Exception):
    pass


DEFAULT_TOKEN_CACHE = os.getenv(
    "TILED_TOKEN_CACHE", os.path.join(appdirs.user_cache_dir("tiled"), "tokens")
)


class TiledAuth(httpx.Auth):
    def __init__(self, refresh_url, csrf_token, token_directory):
        self.refresh_url = refresh_url
        self.csrf_token = csrf_token
        self.token_directory = Path(token_directory)
        self.token_directory.mkdir(exist_ok=True, parents=True)
        self._sync_lock = SerializableLock()
        # self._async_lock = asyncio.Lock()
        self.tokens = {}
        self._check_writable_token_directory()

    def _check_writable_token_directory(self):
        if not os.access(self.token_directory, os.W_OK | os.X_OK):
            raise ValueError(
                f"The token_directory {self.token_directory} is not writable."
            )

    def __getstate__(self):
        return (
            self.refresh_url,
            self.csrf_token,
            self.token_directory,
            self._sync_lock,
        )

    def __setstate__(self, state):
        # Omit the cached tokens (self.tokens) from the pickled bundle because:
        # 1. Sometimes users persist pickled data and handle it insecurely.
        # 2. The un-serialized instance of TiledAuth will need to be able to
        #    read/write from token_directory anyway.
        self.tokens = {}
        (refresh_url, csrf_token, token_directory, sync_lock) = state
        self.refresh_url = refresh_url
        self.csrf_token = csrf_token
        self.token_directory = token_directory
        self._sync_lock = sync_lock
        self._check_writable_token_directory()

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
            self.tokens[key] = value  # Update cached value.

    def sync_clear_token(self, key):
        with self._sync_lock:
            self.tokens.pop(key, None)
            filepath = self.token_directory / key
            # filepath.unlink(missing_ok=False)  # Python 3.8+
            try:
                filepath.unlink()
            except FileNotFoundError:
                pass
            self.tokens.pop(key, None)  # Clear cached value.

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
                return (yield from self.sync_auth_flow(request, attempt=1 + attempt))
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
            token_request = build_refresh_request(
                self.refresh_url, refresh_token, self.csrf_token
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
            token_response.read()
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


def build_refresh_request(refresh_url, refresh_token, csrf_token):
    return httpx.Request(
        "POST",
        refresh_url,
        json={"refresh_token": refresh_token},
        # Submit CSRF token in both header and cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        headers={"x-csrf": csrf_token},
    )
