"""
Tests for connecting the Tiled Python client through an authenticating reverse
proxy, such as JupyterHub's ``jupyter-server-proxy``.

A Tiled server mounted behind a proxy that requires a credential on the API root
used to be unreachable, because the client's initial "discovery" request was
always sent unauthenticated. These tests exercise the fix: a caller-supplied
``auth`` (an ``httpx.Auth``) is applied to the discovery request.

They also pin down a deliberate security property: the long-lived ``api_key``
is *not* sent on the discovery request by default; a proxy whose credential is
the API key must express it via ``auth`` instead.
"""

import contextlib
import json
import urllib.parse

import httpx
import pytest
import uvicorn
from starlette.types import ASGIApp, Receive, Scope, Send

from tiled.catalog import in_memory
from tiled.client import from_uri
from tiled.config import Authentication
from tiled.server.app import build_app
from tiled.server.logging_config import LOGGING_CONFIG

from .utils import Server

API_KEY = "tiledsecret"
PROXY_PREFIX = "/user/spike/tiled"
PROXY_TOKEN = "proxy-token"
LOGIN_HTML = "<html><body>Please log in</body></html>"


class TokenAuth(httpx.Auth):
    """
    A minimal stand-in for the httpx.Auth that a Jupyter notebook would use to
    authenticate to JupyterHub, by adding ``?token=...`` to every request.
    """

    def __init__(self, token):
        self.token = token

    def auth_flow(self, request):
        request.url = request.url.copy_add_param("token", self.token)
        yield request


class ApikeyAuth(httpx.Auth):
    """
    An httpx.Auth that sets the Tiled API key header.

    This is how a caller expresses a credential that is shared by the proxy and
    Tiled: because it is an ``httpx.Auth``, it reaches the discovery request,
    whereas passing the same secret as ``api_key=`` does not.
    """

    def __init__(self, key):
        self.key = key

    def auth_flow(self, request):
        request.headers["Authorization"] = f"Apikey {self.key}"
        yield request


class JupyterServerProxyMiddleware:
    """
    Emulate ``jupyter-server-proxy`` in front of a Tiled app.

    * Strip ``prefix`` from the incoming path and record it as ``root_path``,
      the way ``jupyter-server-proxy`` forwards to a local Tiled process.
    * Serve a login page to requests that do not carry the configured
      credential, the way JupyterHub does for an unauthenticated session.
    """

    def __init__(self, app: ASGIApp, *, prefix: str, token: str = None):
        self.app = app
        self.prefix = prefix
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        scope = dict(scope)
        path = scope["path"]
        if path.startswith(self.prefix):
            scope["path"] = path[len(self.prefix) :] or "/"  # noqa: E203
            scope["root_path"] = self.prefix

        if not self._authenticated(scope):
            # Serve a login page, as JupyterHub does for an unauthenticated
            # session. This is HTML, not JSON, so an unauthenticated Tiled
            # client fails when it tries to parse it.
            body = LOGIN_HTML.encode()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/html")],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return

        await self.app(scope, receive, send)

    def _authenticated(self, scope: Scope) -> bool:
        if self.token is None:
            return True
        query = urllib.parse.parse_qs(scope["query_string"].decode())
        if scope["headers"]:
            headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
        else:
            headers = {}
        if self.token in query.get("token", []):
            return True
        # Also accept the credential in the Authorization header.
        if headers.get("authorization") in (
            f"token {self.token}",
            f"Apikey {self.token}",
        ):
            return True
        return False


def make_proxy_app(*, requires_token=PROXY_TOKEN):
    catalog = in_memory(writable_storage=None)
    app = build_app(catalog, Authentication(single_user_api_key=API_KEY))
    return JupyterServerProxyMiddleware(app, prefix=PROXY_PREFIX, token=requires_token)


@contextlib.contextmanager
def run_proxy(*, requires_token=PROXY_TOKEN):
    config = uvicorn.Config(
        make_proxy_app(requires_token=requires_token),
        port=0,
        loop="asyncio",
        log_config=LOGGING_CONFIG,
    )
    with Server(config).run_in_thread() as url:
        yield f"{url}{PROXY_PREFIX}"


@pytest.fixture
def proxy_url():
    with run_proxy() as url:
        yield url


def test_from_uri_with_auth_authenticates_discovery(proxy_url):
    """
    A caller-supplied ``httpx.Auth`` reaches the discovery request, so a client
    can connect through a proxy that requires credentials.
    """
    client = from_uri(proxy_url, api_key=API_KEY, auth=TokenAuth(PROXY_TOKEN))
    assert client.context.has_external_auth is True
    assert client.context.authenticated
    # Following links (e.g. the metadata request made by from_uri) is also
    # authenticated; otherwise this would have raised.
    assert list(client) == []


def test_api_key_not_sent_on_discovery():
    """
    The API key is deliberately applied *after* discovery. If the proxy's
    credential is the API key, passing only ``api_key=`` is not enough: the
    unauthenticated discovery request receives the login page.
    """
    with run_proxy(requires_token=API_KEY) as proxy_url:
        with pytest.raises(json.JSONDecodeError):
            from_uri(proxy_url, api_key=API_KEY)


def test_shared_secret_via_auth_header():
    """
    A credential shared by the proxy and Tiled can be expressed as an
    ``httpx.Auth``, which *does* reach the discovery request.
    """
    with run_proxy(requires_token=API_KEY) as proxy_url:
        client = from_uri(proxy_url, auth=ApikeyAuth(API_KEY))
        assert client.context.authenticated
        assert list(client) == []


def test_csrf_cookie_still_set_on_authenticated_discovery(proxy_url):
    """
    Authenticating the discovery request must not break the double-submit CSRF
    cookie that the server sets on that response.
    """
    client = from_uri(proxy_url, api_key=API_KEY, auth=TokenAuth(PROXY_TOKEN))
    assert "tiled_csrf" in client.context.http_client.cookies
