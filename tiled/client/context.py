import contextlib
import getpass
import os
import re
import urllib.parse
import warnings
from pathlib import Path

import httpx
import msgpack

from .._version import get_versions
from ..utils import DictView
from .auth import DEFAULT_TOKEN_CACHE, CannotRefreshAuthentication, TiledAuth
from .cache import Revalidate
from .utils import (
    DEFAULT_ACCEPTED_ENCODINGS,
    DEFAULT_TIMEOUT_PARAMS,
    EVENT_HOOKS,
    NotAvailableOffline,
    handle_error,
)

USER_AGENT = f"python-tiled/{get_versions()['version']}"
API_KEY_AUTH_HEADER_PATTERN = re.compile(r"^Apikey (\w+)$")


class Context:
    """
    Wrap an httpx.Client with an optional cache and authentication functionality.
    """

    def __init__(
        self,
        uri,
        *,
        headers=None,
        api_key=None,
        cache=None,
        offline=False,
        timeout=None,
        verify=True,
        token_cache=DEFAULT_TOKEN_CACHE,
        app=None,
    ):
        # The uri is expected to reach the root API route.
        uri = httpx.URL(uri)
        headers = headers or {}
        headers.setdefault("accept-encoding", ",".join(DEFAULT_ACCEPTED_ENCODINGS))
        # Set the User Agent to help the server fail informatively if the client
        # version is too old.
        headers.setdefault("user-agent", USER_AGENT)

        # If ?api_key=... is present, move it from the query into a header.
        # The server would accept it in the query parameter, but using
        # a header is a little more secure (e.g. not logged).
        parsed_params = urllib.parse.parse_qs(uri.query.decode())
        api_key_list = parsed_params.pop("api_key", None)
        if api_key_list is not None:
            if api_key is not None:
                raise ValueError(
                    "api_key was provided as query parameter in URI and as keyword argument. Pick one."
                )
            if len(api_key_list) != 1:
                raise ValueError("Cannot handle two api_key query parameters")
            (api_key,) = api_key_list
        if api_key is None:
            # Check for an API key from the environment.
            api_key = os.getenv("TILED_API_KEY")
        # We will set the API key via the `api_key` property below,
        # after constructing the Client object.

        # FastAPI redirects /api -> /api/ so add it here to save a request.
        path = uri.path
        if not path.endswith("/"):
            path = f"{path}/"
        # Construct the uri *without* api_key param.
        # Drop any params/fragments.
        self.api_uri = httpx.URL(
            urllib.parse.urlunsplit((uri.scheme, uri.netloc.decode(), path, {}, ""))
        )
        if timeout is None:
            timeout = httpx.Timeout(**DEFAULT_TIMEOUT_PARAMS)
        if app is None:
            client = httpx.Client(
                verify=verify,
                event_hooks=EVENT_HOOKS,
                timeout=timeout,
                headers=headers,
                follow_redirects=True,
            )
        else:
            import atexit

            from ._testclient import TestClient

            # verify parameter is dropped, as there is no SSL in ASGI mode
            client = TestClient(
                app=app,
                event_hooks=EVENT_HOOKS,
                timeout=timeout,
                headers=headers,
            )
            client.follow_redirects = True
            client.__enter__()
            atexit.register(client.__exit__)

        self.http_client = client
        self._cache = cache
        self._revalidate = Revalidate.IF_WE_MUST
        self._offline = offline
        self._token_cache = Path(token_cache)

        # Make an initial "safe" request to:
        # (1) Get the server_info.
        # (2) Let the server set the CSRF cookie.
        # No authentication has been set up yet, so these requests will be unauthenticated.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        if offline:
            self.server_info = self.get_json(self.api_uri)
        else:
            # We need a CSRF token.
            with self.disable_cache(allow_read=False, allow_write=True):
                self.server_info = self.get_json(self.api_uri)
        self.api_key = api_key  # property setter sets Authorization header

    @property
    def tokens(self):
        "A view of the current access and refresh tokens."
        return DictView(self.http_client.auth.tokens)

    @property
    def api_key(self):
        # Extract from header to ensure that there is one "ground truth" here
        # and no possibility of state getting out of sync.
        header = self.http_client.headers.get("Authorization", "")
        match = API_KEY_AUTH_HEADER_PATTERN.match(header)
        if match is not None:
            return match.group(1)

    @api_key.setter
    def api_key(self, api_key):
        if api_key is None:
            if self.http_client.headers.get("Authorization", "").startswith("Apikey "):
                self.http_client.headers.pop("Authorization")
        else:
            self.http_client.headers["Authorization"] = f"Apikey {api_key}"

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
                self.server_info = self.get_json(self.api_uri)

    def which_api_key(self):
        """
        A 'who am I' for API keys
        """
        if not self.api_key:
            raise RuntimeError("Not API key is configured for the client.")
        return self.get_json(self.server_info["authentication"]["links"]["apikey"])

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
            self.server_info["authentication"]["links"]["apikey"],
            {"scopes": scopes, "expires_in": expires_in, "note": note},
        )

    def revoke_api_key(self, first_eight):
        request = self.http_client.build_request(
            "DELETE",
            self.server_info["authentication"]["links"]["apikey"],
            headers={"x-csrf": self.http_client.cookies["tiled_csrf"]},
            params={"first_eight": first_eight},
        )
        response = self.http_client.send(request)
        handle_error(response)

    @property
    def app(self):
        warnings.warn(
            "The 'app' accessor on Context is deprecated. Use Context.http_client.app.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.http_client.app

    @property
    def base_url(self):
        warnings.warn(
            "The 'base_url' accessor on Context is deprecated. Use Context.http_client.base_url.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.http_client.base_url

    @property
    def event_hooks(self):
        "httpx.Client event hooks. This is exposed for testing."
        warnings.warn(
            "The 'event_hooks' accessor on Context is deprecated. Use Context.http_client.event_hooks.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.http_client.event_hooks

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
        request = self.http_client.build_request("GET", path, **kwargs)
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
            response = self.http_client.send(request, stream=stream)
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
            response = self.http_client.send(request, stream=stream)
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
        request = self.http_client.build_request(
            "POST",
            path,
            json=content,
            # Submit CSRF token in both header and cookie.
            # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
            headers={
                "x-csrf": self.http_client.cookies["tiled_csrf"],
                "accept": "application/x-msgpack",
            },
        )
        response = self.http_client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def put_json(self, path, content):
        request = self.http_client.build_request(
            "PUT",
            path,
            json=content,
            # Submit CSRF token in both header and cookie.
            # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
            headers={
                "x-csrf": self.http_client.cookies["tiled_csrf"],
                "accept": "application/x-msgpack",
            },
        )
        response = self.http_client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def put_content(self, path, content, headers=None, params=None):
        # Submit CSRF token in both header and cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        headers = headers or {}
        headers.setdefault("x-csrf", self.http_client.cookies["tiled_csrf"])
        headers.setdefault("accept", "application/x-msgpack")
        request = self.http_client.build_request(
            "PUT",
            path,
            content=content,
            headers=headers,
            params=params,
        )
        response = self.http_client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def delete_content(self, path, content, headers=None, params=None):
        # Submit CSRF token in both header and cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        headers = headers or {}
        headers.setdefault("x-csrf", self.http_client.cookies["tiled_csrf"])
        headers.setdefault("accept", "application/x-msgpack")
        request = self.http_client.build_request(
            "DELETE", path, content=None, headers=headers, params=params
        )
        response = self.http_client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def authenticate(self, username=None, provider=None):
        "Authenticate. Prompt for password or access code (refresh token)."
        providers = self.server_info["authentication"]["providers"]
        spec = _choose_identity_provider(providers, provider)
        provider = spec["provider"]
        if self.api_key is not None:
            # Check that API key authenticates us as this user,
            # and then either return or raise.
            identities = self.whoami()["identities"]
            for identity in identities:
                if (identity["provider"] == provider) and (identity["id"] == username):
                    return
            raise RuntimeError(
                "An API key is set, and it is not associated with the username/provider "
                f"{username}/{provider}. Unset the API key first."
            )

        csrf_token = self.http_client.cookies["tiled_csrf"]
        # ~/.config/tiled/tokens/{host:port}/{provider}/{username}
        # with each templated element URL-encoded so it is a valid filename.
        token_directory = Path(
            self._token_cache,
            urllib.parse.quote_plus(self.api_uri.netloc.decode()),
            urllib.parse.quote_plus(provider),
            urllib.parse.quote_plus(username),
        )
        refresh_url = self.server_info["authentication"]["links"]["refresh_session"]
        self.http_client.auth = TiledAuth(refresh_url, csrf_token, token_directory)
        try:
            self.whoami()
        except CannotRefreshAuthentication:
            # Continue below, where we will prompt for log in.
            pass
        else:
            # We have a live session already. No need to log in again.
            return

        mode = spec["mode"]
        auth_endpoint = spec["links"]["auth_endpoint"]
        confirmation_message = spec["confirmation_message"]
        if mode == "password":
            username = username or input("Username: ")
            password = getpass.getpass()
            form_data = {
                "grant_type": "password",
                "username": username,
                "password": password,
            }
            token_request = self.http_client.build_request(
                "POST",
                auth_endpoint,
                data=form_data,
                headers={},
            )
            token_request.headers.pop("Authorization", None)
            token_response = self.http_client.send(token_request, auth=None)
            handle_error(token_response)
            tokens = token_response.json()
        elif mode == "external":
            print(
                f"""
Navigate web browser to this address to obtain access code:

{auth_endpoint}

"""
            )
            import webbrowser

            webbrowser.open(auth_endpoint)
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
                refresh_request = self.http_client.auth.build_refresh_request(
                    refresh_token
                )
                token_response = self.http_client.send(refresh_request, auth=None)
                if token_response.status_code == 401:
                    print(
                        "That didn't work. Try pasting the access code again, or press Enter to escape."
                    )
                else:
                    tokens = token_response.json()
                    break
        else:
            raise ValueError(f"Server has unknown authentication mechanism {mode!r}")
        self.http_client.auth.sync_set_token("access_token", tokens["access_token"])
        self.http_client.auth.sync_set_token("refresh_token", tokens["refresh_token"])
        if confirmation_message:
            identities = self.whoami()["identities"]
            identities_by_provider = {
                identity["provider"]: identity["id"] for identity in identities
            }
            print(
                confirmation_message.format(id=identities_by_provider[spec["provider"]])
            )
        return tokens

    def force_auth_refresh(self):
        """
        Execute refresh flow.

        This method is exposed for testing and debugging uses.

        It should never be necessary for the user to call. Refresh flow is
        automatically executed by tiled.client.auth.TiledAuth when the current
        access_token expires.
        """
        if self.http_client.auth is None:
            raise RuntimeError(
                "No authentication has been set up. Cannot reauthenticate."
            )
        refresh_token = self.http_client.auth.sync_get_token(
            "refresh_token", reload_from_disk=True
        )
        if refresh_token is None:
            raise CannotRefreshAuthentication("There is no refresh_token.")
        refresh_request = self.http_client.auth.build_refresh_request(
            refresh_token,
        )
        token_response = self.http_client.send(refresh_request, auth=None)
        if token_response.status_code == 401:
            raise CannotRefreshAuthentication(
                "Session cannot be refreshed. Log in again."
            )
        handle_error(token_response)
        tokens = token_response.json()
        self.http_client.auth.sync_set_token("access_token", tokens["access_token"])
        self.http_client.auth.sync_set_token("refresh_token", tokens["refresh_token"])
        return tokens

    def whoami(self):
        "Return information about the currently-authenticated user or service."
        return self.get_json(self.server_info["authentication"]["links"]["whoami"])

    def logout(self):
        """
        Clear the access token and the cached refresh token.

        This method is idempotent.
        """
        self.http_client.headers.pop("Authorization", None)
        self.http_client.auth.sync_clear_token("access_token")
        self.http_client.auth.sync_clear_token("refresh_token")

    def revoke_session(self, session_id):
        """
        Revoke a Session so it cannot be refreshed.

        This may be done to ensure that a possibly-leaked refresh token cannot be used.
        """
        request = self.http_client.build_request(
            "DELETE",
            self.server_info["authentication"]["links"]["revoke_session"].format(
                session_id=session_id
            ),
            headers={"x-csrf": self.http_client.cookies["tiled_csrf"]},
        )
        response = self.http_client.send(request)
        handle_error(response)


def _choose_identity_provider(providers, provider=None):
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
    return spec
