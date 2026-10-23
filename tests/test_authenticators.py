import asyncio
import logging
import os
import time
from typing import Any, Optional
from unittest.mock import Mock
from urllib.parse import parse_qs, urlencode

import httpx
import jwt
import pytest
from fastapi import HTTPException
from fastapi.security import SecurityScopes
from jwcrypto.jwk import JWK
from respx import MockRouter
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED

from tiled.authenticators import (
    JWKS_CACHE_TTL,
    JWKS_MAX_STALENESS,
    JWKS_MIN_REFETCH_INTERVAL,
    EntraAuthenticator,
    LDAPAuthenticator,
    OIDCAuthenticator,
    ProxiedOIDCAuthenticator,
)
from tiled.server.authentication import _extract_scopes, check_scopes, decode_token
from tiled.server.settings import Settings

# Set this if there is an LDAP container running for testing.
# See continuous_integration/docker-configs/ldap-docker-compose.yml
TILED_TEST_LDAP = os.getenv("TILED_TEST_LDAP")


# fmt: off
@pytest.mark.filterwarnings("ignore", category=DeprecationWarning)
@pytest.mark.parametrize("ldap_server_address, ldap_server_port", [
    ("localhost", 1389),
    ("localhost:1389", 904),  # Random port, ignored
    ("localhost:1389", None),
    ("127.0.0.1", 1389),
    ("127.0.0.1:1389", 904),
    (["localhost"], 1389),
    (["localhost", "127.0.0.1"], 1389),
    (["localhost", "127.0.0.1:1389"], 1389),
    (["localhost:1389", "127.0.0.1:1389"], None),
])
# fmt: on
@pytest.mark.parametrize("use_tls,use_ssl", [(False, False)])
def test_LDAPAuthenticator_01(use_tls, use_ssl, ldap_server_address, ldap_server_port):
    """
    Basic test for ``LDAPAuthenticator``.

    TODO: The test could be extended with enabled TLS or SSL, but it requires configuration
    of the LDAP server.
    """
    if not TILED_TEST_LDAP:
        pytest.skip("Run an LDAP container and set TILED_TEST_LDAP to run")
    authenticator = LDAPAuthenticator(
        ldap_server_address,
        ldap_server_port,
        bind_dn_template="cn={username},ou=users,dc=example,dc=org",
        use_tls=use_tls,
        use_ssl=use_ssl,
    )

    async def testing():
        assert (await authenticator.authenticate("user01", "password1")).user_name == "user01"
        assert (await authenticator.authenticate("user02", "password2")).user_name == "user02"
        assert (await authenticator.authenticate("user02a", "password2")) is None
        assert (await authenticator.authenticate("user02", "password2a")) is None

    asyncio.run(testing())


@pytest.fixture
def well_known_url(base_url: str) -> str:
    return f"{base_url}.well-known/openid-configuration"


@pytest.fixture
def public_jwks(json_web_keyset: JWK) -> list[dict[str, Any]]:
    return [json_web_keyset.export_public(as_dict=True)]


@pytest.fixture
def mock_oidc_server(
    respx_mock: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    public_jwks: list[dict[str, Any]],
) -> MockRouter:
    respx_mock.get(well_known_url).mock(
        return_value=httpx.Response(httpx.codes.OK, json=well_known_response)
    )
    respx_mock.get(well_known_response["jwks_uri"]).mock(
        return_value=httpx.Response(httpx.codes.OK, json={"keys": public_jwks})
    )
    return respx_mock


def test_oidc_authenticator_caching(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    public_jwks: list[dict[str, Any]],
):

    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    assert authenticator.client_id == "tiled"
    assert authenticator.authorization_endpoint == well_known_response["authorization_endpoint"]
    assert authenticator.id_token_signing_alg_values_supported == well_known_response[
        "id_token_signing_alg_values_supported"
    ]
    assert authenticator.issuer == well_known_response["issuer"]
    assert authenticator.jwks_uri == well_known_response["jwks_uri"]
    assert authenticator.token_endpoint == well_known_response["token_endpoint"]
    assert authenticator.device_authorization_endpoint == well_known_response["device_authorization_endpoint"]
    assert authenticator.end_session_endpoint == well_known_response["end_session_endpoint"]

    # Cached the result of .well_known, only GET once
    assert len(mock_oidc_server.calls) == 1
    call_request = mock_oidc_server.calls[0].request
    assert call_request.method == "GET"
    assert call_request.url == well_known_url

    assert authenticator.keys() == public_jwks
    assert len(mock_oidc_server.calls) == 2  # Called also to jwks
    keys_request = mock_oidc_server.calls[1].request
    assert keys_request.method == "GET"
    assert keys_request.url == well_known_response["jwks_uri"]

    for _ in range(10):
        assert authenticator.keys() == public_jwks

    assert len(mock_oidc_server.calls) == 2  # Getting keys is cached


@pytest.mark.parametrize("expired", [True, False])
def test_oidc_decoding(
    mock_oidc_server: MockRouter,
    rsa_private_key: str,
    well_known_url: str,
    expired: bool,
):
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    access_token = token(expired)
    encrypted_access_token = encrypted_token(access_token, rsa_private_key)

    if not expired:
        assert authenticator.decode_token(encrypted_access_token) == access_token

    else:
        with pytest.raises(jwt.ExpiredSignatureError):
            authenticator.decode_token(encrypted_access_token)


@pytest.mark.parametrize("cls", [OIDCAuthenticator, EntraAuthenticator])
def test_verify_login(mock_oidc_server, rsa_private_key, well_known_url, cls):
    if cls is EntraAuthenticator:
        authenticator = EntraAuthenticator("api", "tiled", well_known_url, "tiled-cli")
    else:
        authenticator = OIDCAuthenticator("api", "tiled", "secret", well_known_uri=well_known_url)
    access_token = "opaque-access-token"
    claims = {
        **token(expired=False),
        "preferred_username": "jane@example.org",
        "at_hash": authenticator._calc_at_hash(access_token, "RS256"),
    }
    id_token = encrypted_token(claims, rsa_private_key)

    verified = authenticator.verify_login(id_token, access_token)
    if cls is EntraAuthenticator:
        assert verified["user"] == "jane"
        assert verified["entra_sub"] == "Jane Doe"
    else:
        assert verified["sub"] == "Jane Doe"

    with pytest.raises(jwt.InvalidTokenError):
        authenticator.verify_login(id_token, "some-other-access-token")
    with pytest.raises(jwt.InvalidTokenError):
        authenticator.verify_login(id_token, None)


@pytest.mark.parametrize("kid", ["rotated-out", None])
def test_proxied_decoding_unknown_kid_is_unauthorized(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    rsa_private_key: str,
    kid: Optional[str],
):
    """A token whose signing key is not in the JWKS (e.g. rotated out by the
    provider) must be rejected with 401, not escape as a 500, so that clients
    fall back to refreshing."""
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli"
    )
    # Expired too, to cover the stale-cached-token case: expiry cannot be
    # checked when the signing key is unknown.
    stale_token = jwt.encode(
        token(expired=True),
        key=rsa_private_key,
        algorithm="RS256",
        headers={"kid": kid} if kid else None,
    )
    with pytest.raises(HTTPException) as exception:
        decode_token(stale_token, ["tiled-secret"], authenticator)
    assert exception.value.status_code == HTTP_401_UNAUTHORIZED


@pytest.mark.parametrize("expired", [True, False])
def test_proxied_decoding_known_kid(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    rsa_private_key: str,
    expired: bool,
):
    """Tokens signed with a known key still decode, and expiry still surfaces
    as ExpiredSignatureError (which callers turn into a 'refresh' 401)."""
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli"
    )
    claims = token(expired=expired)
    signed = encrypted_token(claims, rsa_private_key)
    if expired:
        with pytest.raises(jwt.ExpiredSignatureError):
            decode_token(signed, ["tiled-secret"], authenticator)
    else:
        assert decode_token(signed, ["tiled-secret"], authenticator) == claims


@pytest.fixture
def jwks_route(
    mock_oidc_server: MockRouter,
    well_known_response: dict[str, Any],
    public_jwks: list[dict[str, Any]],
):
    "The JWKS endpoint route, for counting calls and changing its response."
    return mock_oidc_server.get(well_known_response["jwks_uri"]).mock(
        return_value=httpx.Response(httpx.codes.OK, json={"keys": public_jwks})
    )


def age_jwks_cache(authenticator: OIDCAuthenticator, seconds: float) -> None:
    "Pretend the last JWKS fetch happened `seconds` earlier than it did."
    authenticator._jwks_fetched_at -= seconds
    authenticator._jwks_attempted_at -= seconds


def test_oidc_unknown_kid_refetches_jwks(
    well_known_url: str,
    jwks_route,
    public_jwks: list[dict[str, Any]],
    rsa_private_key: str,
):
    """The provider started signing with a key published after our last
    fetch: refetch the JWKS rather than wait out the cache TTL."""
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    assert authenticator.keys() == public_jwks
    assert jwks_route.call_count == 1
    age_jwks_cache(authenticator, JWKS_MIN_REFETCH_INTERVAL + 1)

    new_key = JWK.generate(kty="RSA", size=2048, kid="new", use="sig", alg="RS256")
    new_jwk = new_key.export_public(as_dict=True)
    new_private_key = new_key.export_to_pem("private_key", password=None)
    jwks_route.mock(
        return_value=httpx.Response(httpx.codes.OK, json={"keys": [*public_jwks, new_jwk]})
    )
    claims = token(expired=False)
    signed = jwt.encode(claims, new_private_key, algorithm="RS256", headers={"kid": "new"})

    assert authenticator.decode_token(signed) == claims
    assert jwks_route.call_count == 2
    # Tokens signed with the older key still validate from the refreshed cache.
    assert authenticator.decode_token(encrypted_token(claims, rsa_private_key)) == claims
    assert jwks_route.call_count == 2


def test_oidc_unknown_kid_refetch_is_rate_limited(
    well_known_url: str,
    jwks_route,
    rsa_private_key: str,
    caplog,
):
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    bogus = jwt.encode(
        token(expired=False), rsa_private_key, algorithm="RS256", headers={"kid": "bogus"}
    )
    caplog.set_level(logging.INFO, logger="tiled.authenticators")

    # Keys were just fetched, so an unknown kid does not trigger another fetch.
    for _ in range(3):
        with pytest.raises(jwt.InvalidKeyError):
            authenticator.decode_token(bogus)
    assert jwks_route.call_count == 1
    assert "No JWKS key matches kid 'bogus'" in caplog.text

    # Once the minimum interval has passed, one refetch is allowed.
    age_jwks_cache(authenticator, JWKS_MIN_REFETCH_INTERVAL + 1)
    for _ in range(3):
        with pytest.raises(jwt.InvalidKeyError):
            authenticator.decode_token(bogus)
    assert jwks_route.call_count == 2


def test_oidc_unknown_kid_refetch_failure_keeps_cached_keys(
    well_known_url: str,
    jwks_route,
    rsa_private_key: str,
):
    """A failed refetch must not turn an unknown kid into a server error, nor
    discard the keys we already have."""
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    authenticator.keys()
    age_jwks_cache(authenticator, JWKS_MIN_REFETCH_INTERVAL + 1)
    jwks_route.mock(return_value=httpx.Response(httpx.codes.SERVICE_UNAVAILABLE))

    bogus = jwt.encode(
        token(expired=False), rsa_private_key, algorithm="RS256", headers={"kid": "bogus"}
    )
    with pytest.raises(jwt.InvalidKeyError):
        authenticator.decode_token(bogus)
    assert jwks_route.call_count == 2

    claims = token(expired=False)
    assert authenticator.decode_token(encrypted_token(claims, rsa_private_key)) == claims
    assert jwks_route.call_count == 2


def test_oidc_jwks_cache_expires(
    well_known_url: str,
    jwks_route,
    rsa_private_key: str,
):
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    claims = token(expired=False)
    signed = encrypted_token(claims, rsa_private_key)
    authenticator.decode_token(signed)
    authenticator.decode_token(signed)
    assert jwks_route.call_count == 1
    age_jwks_cache(authenticator, JWKS_CACHE_TTL + 1)
    authenticator.decode_token(signed)
    assert jwks_route.call_count == 2


def test_oidc_jwks_ttl_refresh_failure_falls_back_to_cached_keys(
    well_known_url: str,
    jwks_route,
    rsa_private_key: str,
    caplog,
):
    """If the hourly refresh fails, tokens signed with already-known keys keep
    validating, and the failing fetch is not retried on every request."""
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    claims = token(expired=False)
    signed = encrypted_token(claims, rsa_private_key)
    authenticator.decode_token(signed)
    assert jwks_route.call_count == 1

    age_jwks_cache(authenticator, JWKS_CACHE_TTL + 1)
    jwks_route.mock(return_value=httpx.Response(httpx.codes.SERVICE_UNAVAILABLE))
    caplog.set_level(logging.WARNING, logger="tiled.authenticators")
    for _ in range(3):
        assert authenticator.decode_token(signed) == claims
    assert jwks_route.call_count == 2  # one failed attempt, not one per request
    assert "Failed to fetch JWKS" in caplog.text

    # After the retry interval, try again; success refreshes the cache.
    authenticator._jwks_attempted_at -= JWKS_MIN_REFETCH_INTERVAL + 1
    jwks_route.mock(
        return_value=httpx.Response(
            httpx.codes.OK, json={"keys": authenticator._jwks}
        )
    )
    assert authenticator.decode_token(signed) == claims
    assert jwks_route.call_count == 3
    assert authenticator.decode_token(signed) == claims
    assert jwks_route.call_count == 3


def test_oidc_jwks_too_stale_does_not_fall_back(
    well_known_url: str,
    jwks_route,
    rsa_private_key: str,
):
    "Keys older than JWKS_MAX_STALENESS are not trusted if refreshing fails."
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    signed = encrypted_token(token(expired=False), rsa_private_key)
    authenticator.decode_token(signed)

    age_jwks_cache(authenticator, JWKS_MAX_STALENESS + 1)
    jwks_route.mock(return_value=httpx.Response(httpx.codes.SERVICE_UNAVAILABLE))
    with pytest.raises(httpx.HTTPStatusError):
        authenticator.decode_token(signed)


def test_entra_decoding_ignores_unmapped_scopes(caplog):
    def mock_decode_token(self, token):
        return {
            "iss": "https://login.microsoftonline.com/example-tenant/v2.0",
            "sub": "opaque-sub",
            "preferred_username": "alice@example.org",
            "scp": "known.scope unknown.scope",
        }

    original_decode_token = OIDCAuthenticator.decode_token
    OIDCAuthenticator.decode_token = mock_decode_token
    try:
        caplog.set_level(logging.WARNING)

        authenticator = object.__new__(EntraAuthenticator)
        authenticator._scopes_map = {"known.scope": ["read:metadata"]}
        claims = authenticator.decode_token("id-token")

        assert claims["entra_sub"] == "opaque-sub"
        assert claims["entra_username"] == "alice@example.org"
        assert claims["user"] == "alice"
        assert claims["scope"] == "read:metadata"
        assert any(
            "Unmapped scope in 'scp': unknown.scope" in record.message
            for record in caplog.records
        )
    finally:
        OIDCAuthenticator.decode_token = original_decode_token


@pytest.fixture
def rsa_private_key(json_web_keyset: JWK) -> str:
    return json_web_keyset.export_to_pem("private_key", password=None).decode("utf-8")  # type: ignore


@pytest.fixture
def json_web_keyset() -> JWK:
    return JWK.generate(kty="RSA", size=2048, kid="secret", use="sig", alg="RS256")


def token(expired: bool) -> dict[str, str]:
    now = time.time()
    dummy_token = {
        "aud": "tiled",
        "exp": (now - 1500) if expired else (now + 1500),
        "iat": now,
        "iss": "https://example.com/realms/example",
        "sub": "Jane Doe",
    }
    return dummy_token


def encrypted_token(token: dict[str, str], rsa_private_key: str) -> str:
    return jwt.encode(
        token,
        key=rsa_private_key,
        algorithm="RS256",
        headers={"kid": "secret"},
    )


@pytest.mark.asyncio
async def test_proxied_oidc_token_retrieval(well_known_url: str, mock_oidc_server: MockRouter):
    authenticator = ProxiedOIDCAuthenticator("tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli")
    test_request = httpx.Request("GET", "http://example.com", headers={
        "Authorization": "bearer FOO"
    })

    assert "FOO" == await authenticator.oauth2_schema(test_request)


def create_mock_OIDC_request(
    query_params: Optional[dict[str, str]] = None, root_path: str = ""
) -> Request:
    """Helper function to create a realistic request object for testing."""
    # The ASGI server prepends root_path to path, so the request URL contains it.
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("localhost", 8000),
        "path": f"{root_path}/api/v1/auth/provider/orcid/code",
        "root_path": root_path,
        "headers": [(b"host", b"localhost:8000")],
        "query_string": urlencode(query_params or {}).encode(),
    })


@pytest.mark.asyncio
@pytest.mark.parametrize("root_path", ["", "/tiled"])
async def test_OIDCAuthenticator_mock(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    monkeypatch,
    root_path: str
):
    """
    Test OIDCAuthenticator with mocked external dependencies using respx
    and the impact of root path on the redirect_uri used in the token exchange.
    """
    # Mock JWT token payload
    mock_jwt_payload = {
        "sub": "0009-0008-8698-7745",
        "aud": "APP-TEST-CLIENT-ID",
        "iss": well_known_response["issuer"],
        "exp": 9999999999,  # Far future
        "iat": 1000000000,
        "given_name": "Test User"
    }

    # Add token exchange endpoint to existing mock_oidc_server
    token_route = mock_oidc_server.post(well_known_response["token_endpoint"]).mock(
        return_value=httpx.Response(200, json={
            "access_token": "mock-access-token",
            "id_token": "mock-id-token",
            "token_type": "bearer"
        })
    )

    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID",
        client_secret="test-secret",
        well_known_uri=well_known_url  # Use the fixture
    )

    mock_request = create_mock_OIDC_request(
        {"code": "test-auth-code"}, root_path=root_path
    )

    def mock_verify_login(self, id_token, access_token):
        return mock_jwt_payload

    monkeypatch.setattr(OIDCAuthenticator, "verify_login", mock_verify_login)

    # Test authentication
    user_session = await authenticator.authenticate(mock_request)

    assert user_session is not None
    assert user_session.user_name == "0009-0008-8698-7745"

    form = parse_qs(token_route.calls.last.request.content.decode())
    assert form["redirect_uri"] == [
        f"http://localhost:8000{root_path}/api/v1/auth/provider/orcid/code"
    ]


@pytest.mark.asyncio
async def test_OIDCAuthenticator_missing_code_parameter(well_known_url: str):
    """
    Test OIDCAuthenticator when the 'code' query parameter is missing.
    """
    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID",
        client_secret="test-secret",
        well_known_uri=well_known_url  # Use the fixture
    )

    mock_request = create_mock_OIDC_request({})  # Empty, no code parameter

    result = await authenticator.authenticate(mock_request)
    assert result is None


@pytest.mark.asyncio
async def test_OIDCAuthenticator_token_exchange_failure(
    well_known_url: str, mock_oidc_server, well_known_response
):
    """
    Test OIDCAuthenticator when token exchange fails.
    """
    # Mock the token exchange endpoint to return an error
    mock_oidc_server.post(well_known_response["token_endpoint"]).mock(
        return_value=httpx.Response(400, json={
            'error': 'invalid_client',
            'error_description': 'Client not found: APP-TEST-CLIENT-ID'
        })
    )

    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID",
        client_secret="test-secret",
        well_known_uri=well_known_url
    )

    mock_request = create_mock_OIDC_request({"code": "invalid-code"})

    # This should return None, not raise an exception
    result = await authenticator.authenticate(mock_request)
    assert result is None


async def test_ProxiedOIDCAuthenticator_requires_scopes(mock_oidc_server, well_known_url):

    authenticator = ProxiedOIDCAuthenticator("tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
                                             scopes=["read:data"])

    test_request = Request(scope={"type": "http", "scheme": "http", "headers": [(b"host", b"testserver")]}, )

    settings = Settings(authenticator=authenticator)
    security_scopes = SecurityScopes(scopes=["create:apikeys"])
    with pytest.raises(HTTPException) as exception:
        await check_scopes(request=test_request, settings=settings, scopes=["read:data"],
                           security_scopes=security_scopes)
    assert exception.value.status_code == HTTP_401_UNAUTHORIZED


async def test_ProxiedOIDCAuthenticator_scoped_api_key_passes(mock_oidc_server, well_known_url):
    # Regression test: a request (e.g. from a scope-restricted API key) that
    # carries only a *subset* of the authenticator's full scope universe must
    # succeed as long as it holds the scopes the endpoint actually requires.
    # Previously check_scopes required the request to carry *all* of the
    # authenticator's scopes (the union of every scope in scopes_map), which
    # broke scoped API keys under a ProxiedOIDC/Entra authenticator.
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
        scopes_map={"access_as_user": [
            "read:metadata", "read:data", "create:node", "write:metadata",
            "write:data", "delete:revision", "delete:node", "create:apikeys",
            "revoke:apikeys",
        ]},
    )

    test_request = Request(scope={"type": "http", "scheme": "http", "headers": [(b"host", b"testserver")]}, )
    settings = Settings(authenticator=authenticator)

    # The /metadata/ write endpoint requires these scopes; the restricted key
    # has exactly these (and not the full universe, e.g. no read:data).
    request_scopes = ["write:data", "create:node", "read:metadata", "write:metadata"]
    security_scopes = SecurityScopes(scopes=["write:metadata", "create:node"])

    # Should NOT raise.
    await check_scopes(request=test_request, settings=settings, scopes=request_scopes,
                       security_scopes=security_scopes)

    # Sanity check: missing a required endpoint scope still fails.
    security_scopes = SecurityScopes(scopes=["read:data"])
    with pytest.raises(HTTPException) as exception:
        await check_scopes(request=test_request, settings=settings, scopes=request_scopes,
                           security_scopes=security_scopes)
    assert exception.value.status_code == HTTP_401_UNAUTHORIZED


def test_ProxiedOIDCAuthenticator_scopes_explicit(mock_oidc_server, well_known_url):
    # With no scopes_map, explicit scopes are returned as-is.
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
        scopes=["read:data"],
    )
    assert authenticator.scopes == ["read:data"]


def test_ProxiedOIDCAuthenticator_scopes_union(mock_oidc_server, well_known_url):
    # When both are provided, scopes is the (sorted) union of explicit scopes
    # and the Tiled scopes granted via scopes_map.
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
        scopes=["read:data"],
        scopes_map={"provider.write": ["write:data"]},
    )
    assert authenticator.scopes == ["read:data", "write:data"]


def test_ProxiedOIDCAuthenticator_scopes_derived_from_scopes_map(mock_oidc_server, well_known_url):
    # With no explicit scopes, they are derived from the values of scopes_map.
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
        scopes_map={"provider.read": ["read:metadata", "read:data"]},
    )
    assert set(authenticator.scopes) == {"read:metadata", "read:data"}


def test_ProxiedOIDCAuthenticator_scopes_default_empty(mock_oidc_server, well_known_url):
    # With neither scopes nor scopes_map, scopes is an empty list.
    authenticator = ProxiedOIDCAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
    )
    assert authenticator.scopes == []


def test_EntraAuthenticator_scopes_derived_from_scopes_map(mock_oidc_server, well_known_url):
    # EntraAuthenticator no longer defines its own scopes property; it inherits
    # the scopes_map-derived behavior from ProxiedOIDCAuthenticator.
    authenticator = EntraAuthenticator(
        "tiled", "tiled", well_known_url, device_flow_client_id="tiled-cli",
        scopes_map={"provider.read": ["read:metadata"], "provider.write": ["write:data"]},
    )
    assert set(authenticator.scopes) == {"read:metadata", "write:data"}


def test_ProxiedOIDCAuthenticator_decode_token_maps_scopes():
    # scopes_map translates provider scopes ("scp") into Tiled scopes ("scope").
    def mock_decode_token(self, token):
        return {"sub": "abc", "scp": "provider.read provider.unknown"}

    original = OIDCAuthenticator.decode_token
    OIDCAuthenticator.decode_token = mock_decode_token
    try:
        authenticator = object.__new__(ProxiedOIDCAuthenticator)
        authenticator._scopes_map = {"provider.read": ["read:metadata"]}
        claims = authenticator.decode_token("id-token")
        # Mapped scope is translated; unmapped provider scope is dropped.
        assert claims["scope"] == "read:metadata"
    finally:
        OIDCAuthenticator.decode_token = original


def test_ProxiedOIDCAuthenticator_decode_token_no_map_leaves_scopes():
    # Without scopes_map, the token's native scopes are left untouched.
    def mock_decode_token(self, token):
        return {"sub": "abc", "scp": ["read:data"]}

    original = OIDCAuthenticator.decode_token
    OIDCAuthenticator.decode_token = mock_decode_token
    try:
        authenticator = object.__new__(ProxiedOIDCAuthenticator)
        authenticator._scopes_map = {}
        claims = authenticator.decode_token("id-token")
        assert "scope" not in claims
        assert claims["scp"] == ["read:data"]
    finally:
        OIDCAuthenticator.decode_token = original


def test_ProxiedOIDCAuthenticator_decode_token_all_unmapped_empty_scope():
    # When scopes_map is configured but nothing maps, "scope" is set to an empty
    # string (not left unset) so downstream extraction does not fall back to the
    # raw, unmapped provider "scp" claim.
    def mock_decode_token(self, token):
        return {"sub": "abc", "scp": "provider.unknown"}

    original = OIDCAuthenticator.decode_token
    OIDCAuthenticator.decode_token = mock_decode_token
    try:
        authenticator = object.__new__(ProxiedOIDCAuthenticator)
        authenticator._scopes_map = {"provider.read": ["read:metadata"]}
        claims = authenticator.decode_token("id-token")
        assert claims["scope"] == ""
    finally:
        OIDCAuthenticator.decode_token = original


def test_extract_scopes_empty_scope_is_empty_set():
    # An empty "scope" string must yield an empty set, not {""}.
    assert _extract_scopes({"scope": ""}, None) == set()
    assert _extract_scopes(
        {"scope": "read:data write:data"}, None
    ) == {"read:data", "write:data"}
    # "scope" (even empty) takes precedence over "scp" and is not overridden.
    assert _extract_scopes({"scope": "", "scp": "read:data"}, None) == set()


@pytest.mark.parametrize(
    "error, expected",
    [(jwt.DecodeError("bad"), HTTPException), (jwt.ExpiredSignatureError("old"), jwt.ExpiredSignatureError)],
)
def test_server_decode_token_proxied_errors(error, expected):
    # Invalid provider tokens must be a 401, not an unhandled 500;
    # expiry propagates so callers can say "refresh token".
    from tiled.server.authentication import decode_token

    proxied = Mock(decode_token=Mock(side_effect=error))
    with pytest.raises(expected) as exc_info:
        decode_token("not-a-tiled-token", ["x" * 32], proxied)
    if expected is HTTPException:
        assert exc_info.value.status_code == HTTP_401_UNAUTHORIZED
