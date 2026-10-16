"""Unit tests for the auth path used by client streaming.

These exercise the branching logic directly (no real websocket/server),
mirroring the style of test_transport.py.

Streaming authenticates via the Authorization header on the WebSocket
handshake: 'Apikey SECRET' or 'Bearer TOKEN'. The server side of this lives in
tiled.server.authentication.websocket_credentials.
"""
from unittest.mock import MagicMock, patch

import httpx

from tiled.client.auth import TiledAuth
from tiled.client.stream import API_KEY_LIFETIME, Subscription, _RegularWebsocketWrapper


def test_regular_websocket_wrapper_sends_auth_header():
    uri = httpx.URL("ws://example.com/api/v1/stream/single/node")
    wrapper = _RegularWebsocketWrapper(MagicMock(), uri)

    with patch("tiled.client.stream.connect") as mock_connect:
        wrapper.connect("Bearer tok123")

    _, kwargs = mock_connect.call_args
    assert kwargs["additional_headers"] == {"Authorization": "Bearer tok123"}


def test_regular_websocket_wrapper_sends_no_header_when_anonymous():
    uri = httpx.URL("ws://example.com/api/v1/stream/single/node")
    wrapper = _RegularWebsocketWrapper(MagicMock(), uri)

    with patch("tiled.client.stream.connect") as mock_connect:
        wrapper.connect(None)

    _, kwargs = mock_connect.call_args
    assert kwargs["additional_headers"] == {}


def test_regular_websocket_wrapper_passes_start_as_query_param():
    uri = httpx.URL("ws://example.com/api/v1/stream/single/node")
    wrapper = _RegularWebsocketWrapper(MagicMock(), uri)

    with patch("tiled.client.stream.connect") as mock_connect:
        wrapper.connect(None, start=7)

    args, _ = mock_connect.call_args
    assert "start=7" in args[0]


def make_subscription_double(access_token=None, api_key=None, is_tiled_auth=True):
    """A bare double for Subscription with only the attributes _connect touches."""
    sub = MagicMock()
    sub._disconnect_event.is_set.return_value = False
    if is_tiled_auth:
        sub.context.http_client.auth = MagicMock(spec=TiledAuth)
        sub.context.http_client.auth.sync_get_token.return_value = access_token
    else:
        sub.context.http_client.auth = MagicMock()  # not a TiledAuth instance
    sub.context.api_key = api_key
    sub.context.authenticated = (api_key is not None) or (access_token is not None)
    sub.context.create_api_key.return_value = {
        "secret": "short-lived-secret",
        "first_eight": "12345678",
    }
    return sub


def test_connect_sends_no_header_when_unauthenticated():
    sub = make_subscription_double()
    sub.context.authenticated = False

    Subscription._connect(sub)

    sub._websocket.connect.assert_called_once_with(None, None, max_size=1_000_000)
    sub.context.create_api_key.assert_not_called()


def test_connect_uses_api_key_when_present():
    sub = make_subscription_double(api_key="single-user-key")

    Subscription._connect(sub)

    sub._websocket.connect.assert_called_once_with(
        "Apikey single-user-key", None, max_size=1_000_000
    )
    sub.context.create_api_key.assert_not_called()


def test_connect_uses_bearer_token_for_tiled_auth():
    sub = make_subscription_double(access_token="tok123")

    Subscription._connect(sub)

    sub.context.http_client.auth.sync_get_token.assert_called_once_with(
        "access_token", reload_from_disk=True
    )
    sub._websocket.connect.assert_called_once_with(
        "Bearer tok123", None, max_size=1_000_000
    )
    sub.context.create_api_key.assert_not_called()


def test_connect_requests_and_revokes_short_lived_key_for_other_auth():
    # Authenticated, but not via an API key and not via TiledAuth: fall back to
    # minting a short-lived API key for the handshake, then revoking it.
    sub = make_subscription_double(is_tiled_auth=False)
    sub.context.authenticated = True

    Subscription._connect(sub)

    sub.context.create_api_key.assert_called_once_with(
        expires_in=API_KEY_LIFETIME, note="websocket"
    )
    sub._websocket.connect.assert_called_once_with(
        "Apikey short-lived-secret", None, max_size=1_000_000
    )
    # Regression: should_revoke_api_key/key_info are assigned inside a nested
    # function and need `nonlocal`, or the key leaks unrevoked.
    sub.context.revoke_api_key.assert_called_once_with("12345678")
