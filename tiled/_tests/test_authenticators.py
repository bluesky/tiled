import asyncio
import os
import time
from typing import Any, Tuple

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import ExpiredSignatureError, jwt
from jose.backends import RSAKey
from respx import MockRouter
from starlette.datastructures import URL, QueryParams

from ..authenticators import (
    LDAPAuthenticator,
    OIDCAuthenticator,
    ProxiedOIDCAuthenticator,
)

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
def mock_oidc_server(
    respx_mock: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    json_web_keyset: list[dict[str, Any]],
) -> MockRouter:
    respx_mock.get(well_known_url).mock(
        return_value=httpx.Response(httpx.codes.OK, json=well_known_response)
    )
    respx_mock.get(well_known_response["jwks_uri"]).mock(
        return_value=httpx.Response(httpx.codes.OK, json={"keys": json_web_keyset})
    )
    return respx_mock


def test_oidc_authenticator_caching(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    json_web_keyset: list[dict[str, Any]]
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

    assert authenticator.keys() == json_web_keyset
    assert len(mock_oidc_server.calls) == 2  # Called also to jwks
    keys_request = mock_oidc_server.calls[1].request
    assert keys_request.method == "GET"
    assert keys_request.url == well_known_response["jwks_uri"]

    for _ in range(10):
        assert authenticator.keys() == json_web_keyset

    assert len(mock_oidc_server.calls) == 2  # Getting keys is cached
    keys_request = mock_oidc_server.calls[1].request
    assert keys_request.method == "GET"
    assert keys_request.url == well_known_response["jwks_uri"]


@pytest.mark.parametrize("issued", [True, False])
@pytest.mark.parametrize("expired", [True, False])
def test_oidc_decoding(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    issued: bool,
    expired: bool,
    keys: Tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
):
    private_key, _ = keys
    authenticator = OIDCAuthenticator("tiled", "tiled", "secret", well_known_uri=well_known_url)
    access_token = token(issued, expired)
    encrypted_access_token = encrypted_token(access_token, private_key)

    if not expired:
        # Decode does not currently care if issued_at_time > current time
        assert authenticator.decode_token(encrypted_access_token) == access_token

    else:
        with pytest.raises(ExpiredSignatureError):
            authenticator.decode_token(encrypted_access_token)


@pytest.fixture
def keys() -> Tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    # Key generated just for these tests
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return (private_key, public_key)


@pytest.fixture
def json_web_keyset(keys: Tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]) -> list[dict[str, Any]]:
    _, public_key = keys
    return [
        RSAKey(key=public_key, algorithm="RS256").to_dict()
    ]


def token(issued: bool, expired: bool) -> dict[str, str]:
    now = time.time()
    dummy_token = {
        "aud": "tiled",
        "exp": (now - 1500) if expired else (now + 1500),
        "iat": (now - 1500) if issued else (now + 1500),
        "iss": "https://example.com/realms/example",
        "sub": "Jane Doe",
    }
    return dummy_token


def encrypted_token(token: dict[str, str], private_key: rsa.RSAPrivateKey) -> str:
    return jwt.encode(
        token,
        key=private_key,
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


def create_mock_OIDC_request(query_params=None):
    """Helper function to create a realistic request object for testing."""
    if query_params is None:
        query_params = {}

    class MockRequest:
        def __init__(self, query_params):
            self.query_params = QueryParams(query_params)
            self.scope = {
                "type": "http",
                "scheme": "http",
                "server": ("localhost", 8000),
                "path": "/api/v1/auth/provider/orcid/code",
                "headers": []
            }
            self.headers = {"host": "localhost:8000"}
            self.url = URL("http://localhost:8000/api/v1/auth/provider/orcid/code")

    return MockRequest(query_params)


@pytest.mark.asyncio
async def test_OIDCAuthenticator_mock(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    monkeypatch
):
    """
    Test OIDCAuthenticator with mocked external dependencies using respx.
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
    mock_oidc_server.post(well_known_response["token_endpoint"]).mock(
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

    mock_request = create_mock_OIDC_request({"code": "test-auth-code"})

    def mock_jwt_decode(*args, **kwargs):
        return mock_jwt_payload

    def mock_jwk_construct(*args, **kwargs):
        class MockJWK:
            pass
        return MockJWK()

    monkeypatch.setattr("jose.jwt.decode", mock_jwt_decode)
    monkeypatch.setattr("jose.jwk.construct", mock_jwk_construct)

    # Test authentication
    user_session = await authenticator.authenticate(mock_request)

    assert user_session is not None
    assert user_session.user_name == "0009-0008-8698-7745"


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
