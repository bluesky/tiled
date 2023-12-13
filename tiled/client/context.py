import getpass
import json
import os
import re
import sys
import time
import urllib.parse
import warnings
from pathlib import Path

import appdirs
import httpx

from .._version import __version__ as tiled_version
from ..utils import UNSET, DictView
from .auth import CannotRefreshAuthentication, TiledAuth, build_refresh_request
from .decoders import SUPPORTED_DECODERS
from .transport import Transport
from .utils import DEFAULT_TIMEOUT_PARAMS, MSGPACK_MIME_TYPE, handle_error

USER_AGENT = f"python-tiled/{tiled_version}"
API_KEY_AUTH_HEADER_PATTERN = re.compile(r"^Apikey (\w+)$")
PROMPT_FOR_REAUTHENTICATION = None


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
        cache=UNSET,
        timeout=None,
        verify=True,
        token_cache=None,
        app=None,
        raise_server_exceptions=True,
    ):
        # The uri is expected to reach the root API route.
        uri = httpx.URL(uri)
        headers = headers or {}
        # Define this here instead of at module scope so that the SUPPORTED_DECODERS
        # may be modified before Context instantiation.
        ACCEPT_ENCODING = ", ".join(
            [key for key in SUPPORTED_DECODERS.keys() if key != "identity"]
        )
        # Resolve this here, not at module scope, because the test suite
        # injects TILED_CACHE_DIR env var to use a temporary directory.
        TILED_CACHE_DIR = Path(
            os.getenv("TILED_CACHE_DIR", appdirs.user_cache_dir("tiled"))
        )
        headers.setdefault("accept-encoding", ACCEPT_ENCODING)
        # Set the User Agent to help the server fail informatively if the client
        # version is too old.
        headers.setdefault("user-agent", USER_AGENT)
        if token_cache is None:
            token_cache = TILED_CACHE_DIR / "tokens"

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
        if cache is UNSET:
            cache = None
        if app is None:
            client = httpx.Client(
                transport=Transport(cache=cache),
                verify=verify,
                timeout=timeout,
                follow_redirects=True,
            )
            # Do this in the setter to avoid being overwritten.
            client.headers = headers
        else:
            # Set up an ASGI client.
            # Because we have been handed an app, we can infer that
            # starlette is available.
            from starlette.testclient import TestClient

            # verify parameter is dropped, as there is no SSL in ASGI mode
            client = TestClient(
                app=app,
                raise_server_exceptions=raise_server_exceptions,
            )
            client.timeout = timeout
            client.headers = headers
            # Do this in the setter to avoid being overwritten.
            client.follow_redirects = True
            client._transport = Transport(transport=client._transport, cache=cache)
            client.__enter__()
            # The TestClient is meant to be used only as a context manager,
            # where the context starts and stops and the wrapped ASGI app.
            # We are abusing it slightly to enable interactive use of the
            # TestClient.
            if sys.version_info < (3, 9):
                import atexit

                atexit.register(client.__exit__)
            else:
                import threading

                # The threading module has its own (internal) atexit
                # mechanism that runs at thread shutdown, prior to the atexit
                # mechanism that runs at interpreter shutdown.
                # We need to intervene at that layer to close the portal, or else
                # we will wait forever for a thread run by the portal to join().
                threading._register_atexit(client.__exit__)

        self.http_client = client
        self._verify = verify
        self._cache = cache
        self._token_cache = Path(token_cache)

        # Make an initial "safe" request to:
        # (1) Get the server_info.
        # (2) Let the server set the CSRF cookie.
        # No authentication has been set up yet, so these requests will be unauthenticated.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        self.server_info = handle_error(
            self.http_client.get(
                self.api_uri,
                headers={
                    "Accept": MSGPACK_MIME_TYPE,
                    "Cache-Control": "no-cache, no-store",
                },
            )
        ).json()
        self.api_key = api_key  # property setter sets Authorization header
        self.admin = Admin(self)  # accessor for admin-related requests

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.http_client.__exit__()

    def __getstate__(self):
        if getattr(self.http_client, "app", None):
            raise TypeError(
                "Cannot pickle a Tiled Context built around an ASGI. "
                "Only Tiled Context connected to remote servers can be pickled."
            )
        return (
            tiled_version,
            self.api_uri,
            self.http_client.headers,
            list(self.http_client.cookies.jar),
            self.http_client.timeout,
            self.http_client.auth,
            self._verify,
            self._token_cache,
            self.server_info,
            self.cache,
        )

    def __setstate__(self, state):
        (
            state_tiled_version,
            api_uri,
            headers,
            cookies_list,
            timeout,
            auth,
            verify,
            token_cache,
            server_info,
            cache,
        ) = state
        if state_tiled_version != tiled_version:
            raise TypeError(
                f"Cannot unpickle {type(self).__name__} from tiled version {state_tiled_version} "
                f"using tiled version {tiled_version}. Pickle should only be used to short-term "
                "transfer between identical versions of tiled."
            )
        self.api_uri = api_uri
        cookies = httpx.Cookies()
        for cookie in cookies_list:
            cookies.set(
                cookie.name, cookie.value, domain=cookie.domain, path=cookie.path
            )
        self.http_client = httpx.Client(
            verify=verify,
            cookies=cookies,
            timeout=timeout,
            headers=headers,
            follow_redirects=True,
            auth=auth,
        )
        self._token_cache = token_cache
        self._cache = cache
        self.server_info = server_info

    @classmethod
    def from_any_uri(
        cls,
        uri,
        *,
        headers=None,
        api_key=None,
        cache=UNSET,
        timeout=None,
        verify=True,
        token_cache=None,
        app=None,
    ):
        """
        Accept a URI to a specific node.

        For example, given URI "https://example.com/api/v1//metadata/a/b/c"
        return a Context connected to "https://examples/api/v1" and the list
        ["a", "b", "c"].
        """
        uri = httpx.URL(uri)
        node_path_parts = []
        if "/metadata" in uri.path:
            api_path, _, node_path = uri.path.partition("/metadata")
            api_uri = uri.copy_with(path=api_path)
            node_path_parts.extend(
                [segment for segment in node_path.split("/") if segment]
            )
        # Below we handle the case where we are given *less* than the full URI
        # to the root endpoint. Here we are taking some care to plan for the case
        # where tiled is served at a sub-path, even though that is not yet supported
        # on the server side.
        elif "/api" not in uri.path:
            # Looks like we were given the root path (to the HTML landing page).
            path = uri.path
            if path.endswith("/"):
                path = path[:-1]
            api_uri = uri.copy_with(path=f"{path}/api/v1")
        elif "/v1" not in uri.path:
            # Looks like we were given the /api but no version.
            path = uri.path
            if path.endswith("/"):
                path = path[:-1]
            api_uri = uri.copy_with(path=f"{path}/v1")
        else:
            api_uri = uri
        context = cls(
            api_uri,
            headers=headers,
            api_key=api_key,
            cache=cache,
            timeout=timeout,
            verify=verify,
            token_cache=token_cache,
            app=app,
        )
        return context, node_path_parts

    @classmethod
    def from_app(
        cls,
        app,
        *,
        cache=UNSET,
        token_cache=None,
        headers=None,
        timeout=None,
        api_key=UNSET,
        raise_server_exceptions=True,
    ):
        """
        Construct a Context around a FastAPI app. Primarily for testing.
        """
        context = cls(
            uri="http://local-tiled-app/api/v1",
            headers=headers,
            api_key=api_key,
            cache=cache,
            timeout=timeout,
            token_cache=token_cache,
            app=app,
            raise_server_exceptions=raise_server_exceptions,
        )
        if (api_key is UNSET) and (
            not context.server_info["authentication"]["providers"]
        ):
            # Extract the API key from the app and set it.
            from ..server.settings import get_settings

            settings = app.dependency_overrides[get_settings]()
            api_key = settings.single_user_api_key or None
            context.api_key = api_key
        return context

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
        return self.http_client._transport.cache

    @cache.setter
    def cache(self, cache):
        self.http_client._transport.cache = cache

    def which_api_key(self):
        """
        A 'who am I' for API keys
        """
        if not self.api_key:
            raise RuntimeError("Not API key is configured for the client.")
        return handle_error(
            self.http_client.get(
                self.server_info["authentication"]["links"]["apikey"],
                headers={"Accept": MSGPACK_MIME_TYPE},
            )
        ).json()

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
        return handle_error(
            self.http_client.post(
                self.server_info["authentication"]["links"]["apikey"],
                headers={"Accept": MSGPACK_MIME_TYPE},
                json={"scopes": scopes, "expires_in": expires_in, "note": note},
            )
        ).json()

    def revoke_api_key(self, first_eight):
        handle_error(
            self.http_client.delete(
                self.server_info["authentication"]["links"]["apikey"],
                headers={"x-csrf": self.http_client.cookies["tiled_csrf"]},
                params={"first_eight": first_eight},
            )
        )

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

    def authenticate(
        self,
        username=UNSET,
        provider=UNSET,
        prompt_for_reauthentication=UNSET,
        set_default=True,
    ):
        """
        See login. This is for programmatic use.
        """
        if prompt_for_reauthentication is UNSET:
            prompt_for_reauthentication = PROMPT_FOR_REAUTHENTICATION
        if prompt_for_reauthentication is None:
            prompt_for_reauthentication = _can_prompt()
        if (username is UNSET) and (provider is UNSET):
            default_identity = get_default_identity(self.api_uri)
            if default_identity is not None:
                username = default_identity["username"]
                provider = default_identity["provider"]
        if username is UNSET:
            username = None
        if provider is UNSET:
            provider = None
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

        refresh_url = self.server_info["authentication"]["links"]["refresh_session"]
        csrf_token = self.http_client.cookies["tiled_csrf"]

        # If we are passed a username, we can check whether we already have
        # tokens stashed.
        if username is not None:
            token_directory = self._token_directory(provider, username)
            self.http_client.auth = TiledAuth(refresh_url, csrf_token, token_directory)
            # This will either:
            # * Use an access_token and succeed.
            # * Use a refresh_token to attempt refresh flow and succeed.
            # * Use a refresh_token to attempt refresh flow and fail, raise.
            # * Find no tokens and raise.
            try:
                self.whoami()
            except CannotRefreshAuthentication:
                # Continue below, where we will prompt for log in.
                self.http_client.auth = None
                if not prompt_for_reauthentication:
                    raise
            else:
                # We have a live session for the specified provider and username already.
                # No need to log in again.
                return

        if not prompt_for_reauthentication:
            raise CannotPrompt(
                """Authentication is needed but Tiled has detected that it is running
in a 'headless' context where it cannot prompt the user to provide
credentials in the stdin. Options:

- If Tiled has detected this wrongly, pass prompt_for_reauthentication=True
  to force it to prompt.
- Provide an API key in the environment variable TILED_API_KEY for Tiled to use.
"""
            )
        self.http_client.auth = None
        mode = spec["mode"]
        auth_endpoint = spec["links"]["auth_endpoint"]
        if mode == "password":
            if username:
                print(f"Username {username}")
            else:
                username = input("Username: ")
            password = getpass.getpass()
            form_data = {
                "grant_type": "password",
                "username": username,
                "password": password,
            }
            token_response = self.http_client.post(
                auth_endpoint, data=form_data, auth=None
            )
            handle_error(token_response)
            tokens = token_response.json()
        elif mode == "external":
            verification_response = self.http_client.post(
                auth_endpoint, json={}, auth=None
            )
            handle_error(verification_response)
            verification = verification_response.json()
            authorization_uri = verification["authorization_uri"]
            print(
                f"""
You have {int(verification['expires_in']) // 60} minutes visit this URL

  {authorization_uri}

and enter the code:

  {verification['user_code']}

"""
            )
            import webbrowser

            webbrowser.open(authorization_uri)
            deadline = verification["expires_in"] + time.monotonic()
            print("Waiting...", end="", flush=True)
            while True:
                time.sleep(verification["interval"])
                if time.monotonic() > deadline:
                    raise Exception("Deadline expired.")
                # Intentionally do not wrap this in handle_error(...).
                # Check status codes manually below.
                access_response = self.http_client.post(
                    verification["verification_uri"],
                    json={
                        "device_code": verification["device_code"],
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    },
                    auth=None,
                )
                if (access_response.status_code == 400) and (
                    access_response.json()["detail"]["error"] == "authorization_pending"
                ):
                    print(".", end="", flush=True)
                    continue
                handle_error(access_response)
                print("")
                break
            tokens = access_response.json()

        else:
            raise ValueError(f"Server has unknown authentication mechanism {mode!r}")
        username = tokens["identity"]["id"]
        token_directory = self._token_directory(provider, username)
        auth = TiledAuth(refresh_url, csrf_token, token_directory)
        auth.sync_set_token("access_token", tokens["access_token"])
        auth.sync_set_token("refresh_token", tokens["refresh_token"])
        self.http_client.auth = auth
        confirmation_message = spec.get("confirmation_message")
        if confirmation_message:
            print(confirmation_message.format(id=username))
        if set_default:
            set_default_identity(
                self.api_uri, username=username, provider=spec["provider"]
            )
        return spec, username

    def login(self, username=None, provider=None, prompt_for_reauthentication=UNSET):
        """
        Depending on the server's authentication method, this will prompt for username/password:

        >>> c.login()
        Username: USERNAME
        Password: <input is hidden>

        or prompt you to open a link in a web browser to login with a third party:

        >>> c.login()
        You have ... minutes visit this URL

        https://...

        and enter the code: XXXX-XXXX
        """
        self.authenticate(
            username, provider, prompt_for_reauthentication=prompt_for_reauthentication
        )
        # For programmatic access to the return values, use authenticate().
        # This returns None in order to provide a clean UX in an interpreter.
        return None

    def _token_directory(self, provider, username):
        # ~/.config/tiled/tokens/{host:port}/{provider}/{username}
        # with each templated element URL-encoded so it is a valid filename.
        return Path(
            self._token_cache,
            urllib.parse.quote_plus(str(self.api_uri)),
            urllib.parse.quote_plus(provider),
            urllib.parse.quote_plus(username),
        )

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
        csrf_token = self.http_client.cookies["tiled_csrf"]
        refresh_request = build_refresh_request(
            self.http_client.auth.refresh_url,
            refresh_token,
            csrf_token,
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
        return handle_error(
            self.http_client.get(
                self.server_info["authentication"]["links"]["whoami"],
                headers={"Accept": MSGPACK_MIME_TYPE},
            )
        ).json()

    def logout(self):
        """
        Log out of the current session (if any).

        This method is idempotent.
        """
        if self.http_client.auth is None:
            return

        # Revoke the current session.
        refresh_token = self.http_client.auth.sync_get_token("refresh_token")
        handle_error(
            self.http_client.post(
                f"{self.api_uri}auth/session/revoke",
                json={"refresh_token": refresh_token},
                # Circumvent auth because this request is not authenticated.
                # The refresh_token in the body is the relevant proof, not the
                # 'Authentication' header.
                auth=None,
            )
        )

        # Clear on-disk state.
        self.http_client.auth.sync_clear_token("access_token")
        self.http_client.auth.sync_clear_token("refresh_token")

        # Clear in-memory state.
        self.http_client.headers.pop("Authorization", None)
        self.http_client.auth = None

    def revoke_session(self, session_id):
        """
        Revoke a Session so it cannot be refreshed.

        This may be done to ensure that a possibly-leaked refresh token cannot be used.
        """
        handle_error(
            self.http_client.delete(
                self.server_info["authentication"]["links"]["revoke_session"].format(
                    session_id=session_id
                ),
                headers={"x-csrf": self.http_client.cookies["tiled_csrf"]},
            )
        )


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


class Admin:
    "Accessor for requests that require administrative privileges."

    def __init__(self, context):
        self.context = context
        self.base_url = context.server_info["links"]["self"]

    def list_principals(self, offset=0, limit=100):
        "List Principals (users and services) in the authenticaiton database."
        params = dict(offset=offset, limit=limit)
        return handle_error(
            self.context.http_client.get(
                f"{self.base_url}/auth/principal", params=params
            )
        ).json()

    def show_principal(self, uuid):
        "Show one Principal (user or service) in the authenticaiton database."
        return handle_error(
            self.context.http_client.get(f"{self.base_url}/auth/principal/{uuid}")
        ).json()

    def create_api_key(self, uuid, scopes=None, expires_in=None, note=None):
        """
        Generate a new API key for another user or service.

        Parameters
        ----------
        uuid : str
            Identify the principal -- the user or service
        scopes : Optional[List[str]]
            Restrict the access available to the API key by listing specific scopes.
            By default, this will have the same access as the principal.
        expires_in : Optional[int]
            Number of seconds until API key expires. If None,
            it will never expire or it will have the maximum lifetime
            allowed by the server.
        note : Optional[str]
            Description (for humans).
        """
        return handle_error(
            self.context.http_client.post(
                f"{self.base_url}/auth/principal/{uuid}/apikey",
                headers={"Accept": MSGPACK_MIME_TYPE},
                json={"scopes": scopes, "expires_in": expires_in, "note": note},
            )
        ).json()

    def create_service_principal(
        self,
        role,
    ):
        """
        Generate a new service principal.

        Parameters
        ----------
        role : str
            Specify the role (e.g. user or admin)
        """
        return handle_error(
            self.context.http_client.post(
                f"{self.base_url}/auth/principal",
                headers={"Accept": MSGPACK_MIME_TYPE},
                params={"role": role},
            )
        ).json()


class CannotPrompt(Exception):
    pass


def _can_prompt():
    "Infer whether the user can be prompted for a password or user code."

    if (not sys.__stdin__.closed) and sys.__stdin__.isatty():
        return True
    # In IPython (TerminalInteractiveShell) the above is true, but in
    # Jupyter (ZMQInteractiveShell) it is False.
    # Jupyter own mechanism for giving a prompt, so we always return
    # True if we detect IPython/Jupyter.
    if "IPython" in sys.modules:
        import IPython

        if IPython.get_ipython() is not None:
            return True
    return False


def _default_identity_filepath(api_uri):
    # Resolve this here, not at module scope, because the test suite
    # injects TILED_CACHE_DIR env var to use a temporary directory.
    TILED_CACHE_DIR = Path(
        os.getenv("TILED_CACHE_DIR", appdirs.user_cache_dir("tiled"))
    )
    return Path(
        TILED_CACHE_DIR, "default_identities", urllib.parse.quote_plus(str(api_uri))
    )


def set_default_identity(api_uri, provider, username):
    """
    Stash the identity used with this API so that we can reuse it by default.
    """
    filepath = _default_identity_filepath(api_uri)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as file:
        json.dump({"username": username, "provider": provider}, file)


def get_default_identity(api_uri):
    """
    Look up the default identity to use with this API.
    """
    filepath = _default_identity_filepath(api_uri)
    if filepath.exists():
        return json.loads(filepath.read_text())


def clear_default_identity(api_uri):
    """
    Clear the cached default identity for this API.
    """
    filepath = _default_identity_filepath(api_uri)
    if filepath.exists():
        filepath.unlink()
