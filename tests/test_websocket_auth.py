"""Tests for credential extraction on the WebSocket handshake.

Browsers cannot set headers when opening a WebSocket, so Tiled accepts
credentials three ways: an Authorization header (preferred, for clients that
can send one), a query parameter, or a first message on the open connection.
These tests cover the first two, which are resolved by FastAPI dependencies
before the handshake completes.
"""
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from tiled.server.authentication import (
    get_api_key_websocket,
    get_decoded_access_token_websocket,
    websocket_credentials,
)


@pytest.mark.parametrize(
    "authorization, expected",
    [
        (None, (None, None)),
        ("Apikey secret", ("secret", None)),
        ("apikey secret", ("secret", None)),  # scheme is case-insensitive
        ("Bearer tok123", (None, "tok123")),
        ("bearer tok123", (None, "tok123")),
    ],
)
def test_websocket_credentials_dispatches_on_scheme(authorization, expected):
    assert websocket_credentials(authorization) == expected


def test_websocket_credentials_rejects_unknown_scheme():
    with pytest.raises(HTTPException) as info:
        websocket_credentials("Basic dXNlcjpwYXNz")
    assert info.value.status_code == 400


def test_api_key_from_header():
    assert get_api_key_websocket("Apikey secret", None) == "secret"


def test_api_key_from_query_param():
    assert get_api_key_websocket(None, "secret") == "secret"


def test_api_key_header_wins_over_query_param():
    assert get_api_key_websocket("Apikey from-header", "from-query") == "from-header"


def test_bearer_header_is_not_an_api_key():
    """A Bearer token must fall through to the access-token dependency.

    Regression: this raised HTTP 400 and killed the handshake, so no
    OIDC-authenticated client could open a stream.
    """
    assert get_api_key_websocket("Bearer tok123", None) is None


def test_bearer_header_falls_through_to_query_api_key():
    assert get_api_key_websocket("Bearer tok123", "secret") == "secret"


def decode_access_token(access_token=None, authorization=None):
    """Call the dependency, reporting which raw token reached decode_token."""
    settings = MagicMock()
    settings.secret_keys = ["secret-key"]
    settings.authenticator = None
    with patch(
        "tiled.server.authentication.decode_token",
        side_effect=lambda token, *args, **kwargs: {"token": token},
    ):
        return get_decoded_access_token_websocket(
            MagicMock(), access_token, authorization, settings
        )


def test_access_token_from_bearer_header():
    assert decode_access_token(authorization="Bearer tok123") == {"token": "tok123"}


def test_access_token_from_query_param():
    """Kept for browsers, which cannot set headers on a WebSocket handshake."""
    assert decode_access_token(access_token="tok123") == {"token": "tok123"}


def test_access_token_header_wins_over_query_param():
    result = decode_access_token(
        access_token="from-query", authorization="Bearer from-header"
    )
    assert result == {"token": "from-header"}


def test_no_credentials_returns_none():
    """Leaves the connection eligible for first-message auth (Issue #1138)."""
    assert decode_access_token() is None


def test_apikey_header_is_not_an_access_token():
    assert decode_access_token(authorization="Apikey secret") is None


def test_malformed_token_is_401_not_500():
    """decode_token must not let jose's JWTError escape as a 500.

    Regression: with a ProxiedOIDCAuthenticator configured, a malformed token
    reached proxied_authenticator.decode_token and the raw JWTError propagated,
    so a bad token produced a 500 on both the HTTP and WebSocket routes.
    """
    from jose import JWTError

    from tiled.server.authentication import decode_token

    proxied = MagicMock()
    proxied.decode_token.side_effect = JWTError("Not enough segments")

    with pytest.raises(HTTPException) as info:
        decode_token("not.a.jwt", ["secret-key"], proxied)
    assert info.value.status_code == 401


def test_expired_token_still_propagates_for_refresh():
    """ExpiredSignatureError must survive so callers can answer 'Refresh token.'"""
    from jose import ExpiredSignatureError

    from tiled.server.authentication import decode_token

    proxied = MagicMock()
    proxied.decode_token.side_effect = ExpiredSignatureError("expired")

    with pytest.raises(ExpiredSignatureError):
        decode_token("expired.jwt.token", ["secret-key"], proxied)
