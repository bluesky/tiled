"""Unit tests for the access-token auth path added to client streaming.

These exercise the branching logic directly (no real websocket/server),
mirroring the style of test_transport.py.
"""
import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from tiled.client.auth import TiledAuth
from tiled.client.stream import API_KEY_LIFETIME, Subscription, _RegularWebsocketWrapper


def make_http_client(access_token=None, is_tiled_auth=True):
    http_client = MagicMock()
    if is_tiled_auth:
        http_client.auth = MagicMock(spec=TiledAuth)
        http_client.auth.sync_get_token.return_value = access_token
    else:
        http_client.auth = MagicMock()  # not a TiledAuth instance
    return http_client


def test_regular_websocket_wrapper_sends_access_token_when_present():
    http_client = make_http_client(access_token="tok123")
    uri = httpx.URL("ws://example.com/api/v1/stream/single/node")
    wrapper = _RegularWebsocketWrapper(http_client, uri)

    with patch("tiled.client.stream.connect") as mock_connect:
        mock_ws = mock_connect.return_value
        wrapper.connect(api_key=None)

    http_client.auth.sync_get_token.assert_called_once_with(
        "access_token", reload_from_disk=True
    )
    mock_ws.send.assert_called_once_with(
        json.dumps({"type": "auth", "access_token": "tok123"})
    )


def test_regular_websocket_wrapper_no_send_without_access_token():
    http_client = make_http_client(access_token=None)
    uri = httpx.URL("ws://example.com/api/v1/stream/single/node")
    wrapper = _RegularWebsocketWrapper(http_client, uri)

    with patch("tiled.client.stream.connect") as mock_connect:
        mock_ws = mock_connect.return_value
        wrapper.connect(api_key="secret")

    mock_connect.assert_called_once()
    _, kwargs = mock_connect.call_args
    assert kwargs["additional_headers"] == {"Authorization": "Apikey secret"}
    mock_ws.send.assert_not_called()


def test_regular_websocket_wrapper_ignores_non_tiled_auth():
    http_client = make_http_client(is_tiled_auth=False)
    uri = httpx.URL("ws://example.com/api/v1/stream/single/node")
    wrapper = _RegularWebsocketWrapper(http_client, uri)

    with patch("tiled.client.stream.connect") as mock_connect:
        mock_ws = mock_connect.return_value
        wrapper.connect(api_key=None)

    mock_ws.send.assert_not_called()


def make_subscription_double(access_token, providers, api_key="single-user-key"):
    """A bare double for Subscription with only the attributes _connect touches."""
    sub = MagicMock()
    sub._disconnect_event.is_set.return_value = False
    sub.context.http_client.auth = MagicMock(spec=TiledAuth)
    sub.context.http_client.auth.sync_get_token.return_value = access_token
    sub.context.server_info.authentication.providers = providers
    sub.context.create_api_key.return_value = {
        "secret": "short-lived-secret",
        "first_eight": "12345678",
    }
    sub.context.api_key = api_key
    return sub


def test_connect_requests_and_revokes_api_key_when_authenticated_no_token():
    sub = make_subscription_double(access_token=None, providers=["oidc"])

    Subscription._connect(sub)

    sub.context.create_api_key.assert_called_once_with(
        expires_in=API_KEY_LIFETIME, note="websocket"
    )
    sub._websocket.connect.assert_called_once_with(
        "short-lived-secret", None, max_size=1_000_000
    )
    sub.context.revoke_api_key.assert_called_once_with("12345678")


def test_connect_skips_api_key_when_access_token_present():
    sub = make_subscription_double(access_token="tok123", providers=["oidc"])

    Subscription._connect(sub)

    sub.context.create_api_key.assert_not_called()
    sub._websocket.connect.assert_called_once_with(None, None, max_size=1_000_000)
    sub.context.revoke_api_key.assert_not_called()


def test_connect_uses_single_user_api_key_when_unauthenticated():
    sub = make_subscription_double(access_token=None, providers=None)

    Subscription._connect(sub)

    sub.context.create_api_key.assert_not_called()
    sub._websocket.connect.assert_called_once_with(
        "single-user-key", None, max_size=1_000_000
    )
    sub.context.revoke_api_key.assert_not_called()


def test_connect_raises_when_no_token_and_providers_empty():
    # providers is [] (not None, but falsy), so neither the "authenticated"
    # nor the "unauthenticated" branch applies; with no access token there
    # is nothing to authenticate the websocket with.
    sub = make_subscription_double(access_token=None, providers=[])

    with pytest.raises(RuntimeError, match="Cannot authenticate WebSocket connection"):
        Subscription._connect(sub)

    sub.context.create_api_key.assert_not_called()
    sub._websocket.connect.assert_not_called()
