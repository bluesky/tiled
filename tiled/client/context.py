import contextlib
import enum
import getpass
import os
import secrets
import threading
import urllib.parse
from pathlib import Path, PurePosixPath

import appdirs
import httpx
import msgpack

from .._version import get_versions
from ..utils import DictView
from .cache import Revalidate
from .utils import (
    ASYNC_EVENT_HOOKS,
    DEFAULT_ACCEPTED_ENCODINGS,
    NotAvailableOffline,
    handle_error,
)

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
        directory = _token_directory(token_cache, netloc)
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


class PromptForReauthentication(enum.Enum):
    AT_INIT = "at_init"
    NEVER = "never"
    ALWAYS = "always"


class Context:
    """
    Wrap an httpx.Client with an optional cache and authentication functionality.
    """

    def __init__(
        self,
        client,
        *,
        username=None,
        auth_provider=None,
        api_key=None,
        cache=None,
        offline=False,
        token_cache=DEFAULT_TOKEN_CACHE,
        prompt_for_reauthentication=PromptForReauthentication.AT_INIT,
        app=None,
    ):
        if (username is not None) or (auth_provider is not None):
            if api_key is not None:
                raise ValueError("Use api_key or username/auth_provider, not both.")
        elif api_key is None:
            # Check for an API key from the environment.
            api_key = os.getenv("TILED_API_KEY")
        self._client = client
        self._cache = cache
        self._revalidate = Revalidate.IF_WE_MUST
        self._username = username
        self._auth_provider = auth_provider
        self.api_key = api_key  # property setter sets Authorization header
        self._offline = offline
        self._token_cache_or_root_directory = token_cache
        self._prompt_for_reauthentication = PromptForReauthentication(
            prompt_for_reauthentication
        )
        self._refresh_lock = threading.Lock()
        if isinstance(token_cache, (str, Path)):
            directory = _token_directory(token_cache, self._client.base_url.netloc)
            token_cache = TokenCache(directory)
        self._token_cache = token_cache
        # The token *cache* is optional. The tokens attrbiute is always present,
        # and it isn't actually used for anything internally. It's just a view
        # of the current tokens.
        self._tokens = {}
        self._app = app

        # Set the User Agent to help the server fail informatively if the client
        # version is too old.
        self._client.headers["user-agent"] = f"python-tiled/{get_versions()['version']}"

        # Stash the URL of the original request. We will alter the base_url below
        # if it is not aligned with root_path of the tiled server.
        url = httpx.URL(self._client.base_url)

        # Make an initial "safe" request to let the server set the CSRF cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        # And, at the same time, obtain the 'root_path', the path to the root route
        # of the Tiled application, which may or may not be the same as the URL that
        # the user provided.
        if offline:
            self._handshake_data = self.get_json("/", params={"root_path": True})
        else:
            # We need a CSRF token.
            with self.disable_cache(allow_read=False, allow_write=True):
                # Make this request manually to inject custom error handling.
                request = self._client.build_request(
                    "GET", "/", params={"root_path": True}
                )
                response = self._client.send(request)
                # Handle case where user pastes in a link like
                # https://example.com/some/subpath/node/metadata/a/b/c
                # and it requires authentication. The 401 response includes a header
                # that points us to https://examples.com/some/subpath where we
                # can see the authentication providers and their endpoints.
                if response.status_code == 401:
                    self._client.base_url = response.headers["x-tiled-root"]
                # Now try again.
                self._handshake_data = self.get_json("/", params={"root_path": True})

        if (
            (not offline)
            and (api_key is None)
            and (
                self._handshake_data["authentication"]["required"]
                or (username is not None)
            )
        ):
            if not self._handshake_data["authentication"]["providers"]:
                raise RuntimeError(
                    """This server requires API key authentication.
Set an api_key as in:

>>> c = from_uri("...", api_key="...")
"""
                )
            # Authenticate. If a valid refresh_token is available in the token_cache,
            # it will be used. Otherwise, this will prompt for input from the stdin
            # or raise CannotRefreshAuthentication.
            prompt = (
                prompt_for_reauthentication == PromptForReauthentication.AT_INIT
                or prompt_for_reauthentication == PromptForReauthentication.ALWAYS
            )
            tokens = self.reauthenticate(prompt=prompt)
            access_token = tokens["access_token"]
            client.headers["Authorization"] = f"Bearer {access_token}"
        base_path = self._handshake_data["meta"]["root_path"]
        base_url = urllib.parse.urlunsplit(
            (url.scheme, url.netloc.decode(), base_path, {}, url.fragment)
        )
        client.base_url = base_url
        path_parts = list(PurePosixPath(url.path).relative_to(base_path).parts)
        # Strip "/api/node/metadata"
        self._path_parts = path_parts[3:]

    @property
    def tokens(self):
        "A view of the current access and refresh tokens."
        return DictView(self._tokens)

    @property
    def api_key(self):
        return self._api_key

    @api_key.setter
    def api_key(self, api_key):
        if api_key is None:
            if self._client.headers.get("Authorization", "").startswith("Apikey"):
                self._client.headers.pop("Authorization")
        else:
            self._client.headers["Authorization"] = f"Apikey {api_key}"
        self._api_key = api_key

    @property
    def cache(self):
        return self._cache

    @property
    def offline(self):
        return self._offline

    @offline.setter
    def offline(self, value):
        if self._cache is None:
            raise RuntimeError(
                """To use offline mode,  Tiled must be configured with a Cache, as in

>>> from tiled.client import from_uri
>>> from tiled.client.cache import Cache
>>> client = from_uri("...", cache=Cache.on_disk("my_cache"))
"""
            )
        self._offline = bool(value)
        if not self._offline:
            # We need a CSRF token.
            with self.disable_cache(allow_read=False, allow_write=True):
                self._handshake_data = self.get_json("/")

    def which_api_key(self):
        """
        A 'who am I' for API keys
        """
        if not self.api_key:
            raise RuntimeError("Not API key is configured for the client.")
        return self.get_json(self._handshake_data["authentication"]["links"]["apikey"])

    def create_api_key(self, scopes=None, expires_in=None, note=None):
        """
        Generate a new API for the currently-authenticated user.

        Parameters
        ----------
        scopes : Optional[List[str]]
            Restrict the access available to the API key by listing specific scopes.
            By default, this will have the same access as the user.
        expires_in : Optional[int]
            Number of seconds until API key expires. If None,
            it will never expire or it will have the maximum lifetime
            allowed by the server.
        note : Optional[str]
            Description (for humans).
        """
        return self.post_json(
            self._handshake_data["authentication"]["links"]["apikey"],
            {"scopes": scopes, "expires_in": expires_in, "note": note},
        )

    def revoke_api_key(self, first_eight):
        request = self._client.build_request(
            "DELETE",
            self._handshake_data["authentication"]["links"]["apikey"],
            headers={"x-csrf": self._client.cookies["tiled_csrf"]},
            params={"first_eight": first_eight},
        )
        response = self._client.send(request)
        handle_error(response)

    @property
    def app(self):
        return self._app

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

    @property
    def revalidate(self):
        """
        This controls how aggressively to check whether cache entries are out of date.

        - FORCE: Always revalidate (generally too aggressive and expensive)
        - IF_EXPIRED: Revalidate if the "Expire" date provided by the server has passed
        - IF_WE_MUST: Only revalidate if the server indicated that is is a
          particularly volatile entry, such as a search result to a dynamic query.
        """
        return self._revalidate

    @revalidate.setter
    def revalidate(self, value):
        self._revalidate = Revalidate(value)

    @contextlib.contextmanager
    def revalidation(self, revalidate):
        """
        Temporarily change the 'revalidate' property in a context.

        Parameters
        ----------
        revalidate: string or tiled.client.cache.Revalidate enum member
        """
        try:
            member = Revalidate(revalidate)
        except ValueError as err:
            # This raises a more helpful error that lists the valid options.
            raise ValueError(
                f"Revalidation {revalidate} not recognized. Must be one of {set(Revalidate.__members__)}"
            ) from err
        original = self.revalidate
        self.revalidate = member
        yield
        # Upon leaving context, set it back.
        self.revalidate = original

    @contextlib.contextmanager
    def disable_cache(self, allow_read=False, allow_write=False):
        self._disable_cache_read = not allow_read
        self._disable_cache_write = not allow_write
        yield
        self._disable_cache_read = False
        self._disable_cache_write = False

    def get_content(self, path, accept=None, stream=False, revalidate=None, **kwargs):
        if revalidate is None:
            # Fallback to context default.
            revalidate = self.revalidate
        request = self._client.build_request("GET", path, **kwargs)
        if accept:
            request.headers["Accept"] = accept
        url = request.url
        if self._offline:
            # We must rely on the cache alone.
            # The role of a 'reservation' is to ensure that the content
            # of interest is not evicted from the cache between the moment
            # that we start verifying its validity and the moment that
            # we actually read the content. It is used more extensively
            # below.
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
        # Parse Cache-Control header directives.
        cache_control = {
            directive.lstrip(" ")
            for directive in request.headers.get("Cache-Control", "").split(",")
        }
        if "no-cache" in cache_control:
            reservation = None
        else:
            reservation = self._cache.get_reservation(url)
        try:
            if reservation is not None:
                is_stale = reservation.is_stale()
                if not (
                    # This condition means "client user wants us to unconditionally revalidate"
                    (revalidate == Revalidate.FORCE)
                    or
                    # This condition means "client user wants us to revalidate if expired"
                    (is_stale and (revalidate == Revalidate.IF_EXPIRED))
                    or
                    # This condition means "server really wants us to revalidate"
                    (is_stale and reservation.item.must_revalidate)
                    or self._disable_cache_read
                ):
                    # Short-circuit. Do not even bother consulting the server.
                    return reservation.load_content()
                if not self._disable_cache_read:
                    request.headers["If-None-Match"] = reservation.item.etag
            response = self._send(request, stream=stream)
            handle_error(response)
            if response.status_code == 304:  # HTTP 304 Not Modified
                # Update the expiration time.
                reservation.renew(response.headers.get("expires"))
                # Read from the cache
                return reservation.load_content()
            elif not response.is_error:
                etag = response.headers.get("ETag")
                encoding = response.headers.get("Content-Encoding")
                content = response.content
                # httpx handles standard HTTP encodings transparently, but we have to
                # handle "blosc" manually.
                if encoding == "blosc":
                    import blosc

                    content = blosc.decompress(content)
                if (
                    ("no-store" not in cache_control)
                    and (etag is not None)
                    and (not self._disable_cache_write)
                ):
                    # Write to cache.
                    self._cache.put(
                        url,
                        response.headers,
                        content,
                    )
                return content
            else:
                raise NotImplementedError(
                    f"Unexpected status_code {response.status_code}"
                )
        finally:
            if reservation is not None:
                reservation.ensure_released()

    def get_json(self, path, stream=False, **kwargs):
        return msgpack.unpackb(
            self.get_content(
                path, accept="application/x-msgpack", stream=stream, **kwargs
            ),
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def post_json(self, path, content):
        request = self._client.build_request(
            "POST",
            path,
            json=content,
            # Submit CSRF token in both header and cookie.
            # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
            headers={
                "x-csrf": self._client.cookies["tiled_csrf"],
                "accept": "application/x-msgpack",
            },
        )
        response = self._client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def _send(self, request, stream=False, attempts=0):
        """
        If sending results in an authentication error, reauthenticate.
        """
        response = self._client.send(request, stream=stream)
        if (self.api_key is None) and (response.status_code == 401) and (attempts == 0):
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

    def authenticate(self, provider=None):
        "Authenticate. Prompt for password or access code (refresh token)."
        if self.api_key is not None:
            raise RuntimeError("API key authentication is being used.")
        providers = self._handshake_data["authentication"]["providers"]
        if len(providers) == 0:
            raise RuntimeError(
                "The authenticate() method is not applicable. "
                "This server does not support any authentication providers."
            )
        if provider is not None:
            for spec in providers:
                if spec["provider"] == provider:
                    break
            else:
                raise ValueError(
                    f"No such provider {provider}. Choices are {[spec['provider'] for spec in providers]}"
                )
        else:
            if len(providers) == 1:
                # There is only one choice, so no need to prompt the user.
                (spec,) = providers
            else:
                while True:
                    print("Authenticaiton providers:")
                    for i, spec in enumerate(providers, start=1):
                        print(f"{i} - {spec['provider']}")
                    raw_choice = input(
                        "Choose an authentication provider (or press Enter to escape): "
                    )
                    if not raw_choice:
                        print("No authentication provider chosen. Failed.")
                        break
                    try:
                        choice = int(raw_choice)
                    except TypeError:
                        print("Choice must be a number.")
                        continue
                    try:
                        spec = providers[choice - 1]
                    except IndexError:
                        print(f"Choice must be a number 1 through {len(providers)}.")
                        continue
                    break
        mode = spec["mode"]
        auth_endpoint = spec["links"]["auth_endpoint"]
        confirmation_message = spec["confirmation_message"]
        if mode == "password":
            username = self._username or input("Username: ")
            password = getpass.getpass()
            form_data = {
                "grant_type": "password",
                "username": username,
                "password": password,
            }
            token_request = self._client.build_request(
                "POST",
                auth_endpoint,
                data=form_data,
                headers={},
            )
            token_request.headers.pop("Authorization", None)
            token_response = self._client.send(token_request)
            handle_error(token_response)
            tokens = token_response.json()
            refresh_token = tokens["refresh_token"]
        elif mode == "external":
            print(
                f"""
Navigate web browser to this address to obtain access code:

{auth_endpoint}

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
        else:
            raise ValueError(f"Server has unknown authentication mechanism {mode!r}")
        if self._token_cache is not None:
            # We are using a token cache. Store the new refresh token.
            self._token_cache["refresh_token"] = refresh_token
        self._tokens.update(
            refresh_token=tokens["refresh_token"], access_token=tokens["access_token"]
        )
        if confirmation_message:
            identities = self.whoami()["identities"]
            identities_by_provider = {
                identity["provider"]: identity["id"] for identity in identities
            }
            print(
                confirmation_message.format(id=identities_by_provider[spec["provider"]])
            )
        return tokens

    def reauthenticate(self, prompt=None):
        """
        Refresh authentication.

        Parameters
        ----------
        prompt : bool
            If True, give interactive prompt for authentication when refreshing
            tokens fails. If False raise an error. If None, fall back
            to default `prompt_for_reauthentication` set in Context.__init__.
        """
        if self.api_key is not None:
            raise RuntimeError("API key authentication is being used.")
        try:
            return self._refresh()
        except CannotRefreshAuthentication:
            if prompt is None:
                prompt = self._prompt_for_reauthentication
            if prompt:
                return self.authenticate()
            raise

    def whoami(self):
        "Return information about the currently-authenticated user or service."
        return self.get_json(self._handshake_data["authentication"]["links"]["whoami"])

    def logout(self):
        """
        Clear the access token and the cached refresh token.

        This method is idempotent.
        """
        self._client.headers.pop("Authorization", None)
        if self._token_cache is not None:
            self._token_cache.pop("refresh_token", None)
        self._tokens.clear()

    def revoke_session(self, session_id):
        """
        Revoke a Session so it cannot be refreshed.

        This may be done to ensure that a possibly-leaked refresh token cannot be used.
        """
        request = self._client.build_request(
            "DELETE",
            self._handshake_data["authentication"]["links"]["revoke_session"].format(
                session_id=session_id
            ),
            headers={"x-csrf": self._client.cookies["tiled_csrf"]},
        )
        response = self._client.send(request)
        handle_error(response)

    def _refresh(self, refresh_token=None):
        with self._refresh_lock:
            if refresh_token is None:
                if self._token_cache is None:
                    # We are not using a token cache.
                    raise CannotRefreshAuthentication(
                        "No token cache was given. "
                        "Provide fresh credentials. "
                        "For a given client c, use c.context.authenticate()."
                    )
                # We are using a token_cache.
                try:
                    refresh_token = self._token_cache["refresh_token"]
                except KeyError:
                    raise CannotRefreshAuthentication(
                        "No refresh token was found in token cache. "
                        "Provide fresh credentials. "
                        "For a given client c, use c.context.authenticate()."
                    )
            token_request = self._client.build_request(
                "POST",
                self._handshake_data["authentication"]["links"]["refresh_session"],
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
                    "Server rejected attempt to refresh token. "
                    "Provide fresh credentials. "
                    "For a given client c, use c.context.authenticate()."
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
                refresh_token=tokens["refresh_token"],
                access_token=tokens["access_token"],
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
    prompt_for_reauthentication=PromptForReauthentication.AT_INIT,
    username=None,
    auth_provider=None,
    api_key=None,
    headers=None,
):
    from ..server.app import build_app

    # By default make it "public" because there is no way to
    # secure access from inside the same process anyway.
    authentication = authentication or {"allow_anonymous_access": True}
    server_settings = server_settings or {}
    params = {}
    headers = headers or {}
    headers.setdefault("accept-encoding", ",".join(DEFAULT_ACCEPTED_ENCODINGS))
    # If a single-user API key will be used, generate the key here instead of
    # letting build_app do it for us, so that we can give it to the client
    # below.
    if (
        (not authentication.get("providers"))
        and (not authentication.get("allow_anonymous_access", False))
        and (authentication.get("single_user_api_key") is None)
    ):
        single_user_api_key = os.getenv(
            "TILED_SINGLE_USER_API_KEY", secrets.token_hex(32)
        )
        authentication["single_user_api_key"] = single_user_api_key
        params["api_key"] = single_user_api_key
    app = build_app(
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
        base_url="http://local-tiled-app/api/",
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
        auth_provider=auth_provider,
        api_key=api_key,
        prompt_for_reauthentication=prompt_for_reauthentication,
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
