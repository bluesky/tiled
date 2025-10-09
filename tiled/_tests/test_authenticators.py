import asyncio
import os

import pytest
import respx
import httpx
from ..authenticators import LDAPAuthenticator, OIDCAuthenticator
from starlette.datastructures import QueryParams, URL


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
@respx.mock
async def test_OIDCAuthenticator_mock(monkeypatch):
    """
    Test OIDCAuthenticator with mocked external dependencies using respx.
    """
    # Mock the well-known configuration
    mock_well_known = {
        "authorization_endpoint": "https://orcid.org/oauth/authorize",
        "token_endpoint": "https://orcid.org/oauth/token",
        "jwks_uri": "https://orcid.org/oauth/jwks",
        "issuer": "https://orcid.org"
    }
    
    # Mock JWT token payload
    mock_jwt_payload = {
        "sub": "0009-0008-8698-7745",
        "aud": "APP-TEST-CLIENT-ID",
        "iss": "https://orcid.org",
        "exp": 9999999999,  # Far future
        "iat": 1000000000,
        "given_name": "Test User"
    }
    
    # Mock the well-known configuration endpoint
    respx.get("https://orcid.org/.well-known/openid-configuration").mock(
        return_value=httpx.Response(200, json=mock_well_known)
    )
    
    # Mock the JWKS endpoint
    respx.get("https://orcid.org/oauth/jwks").mock(
        return_value=httpx.Response(200, json={"keys": [{"kid": "test", "kty": "RSA"}]})
    )
    
    # Mock the token exchange endpoint
    respx.post("https://orcid.org/oauth/token").mock(
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
        well_known_uri="https://orcid.org/.well-known/openid-configuration"
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
async def test_OIDCAuthenticator_missing_code_parameter():
    """
    Test OIDCAuthenticator when the 'code' query parameter is missing.
    """
    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID", 
        client_secret="test-secret",
        well_known_uri="https://orcid.org/.well-known/openid-configuration"
    )
    
    mock_request = create_mock_OIDC_request({})  # Empty, no code parameter
    
    result = await authenticator.authenticate(mock_request)
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_OIDCAuthenticator_token_exchange_failure():
    """
    Test OIDCAuthenticator when token exchange fails using respx.
    """
    # Mock the well-known configuration
    mock_well_known = {
        "token_endpoint": "https://orcid.org/oauth/token",
        "jwks_uri": "https://orcid.org/oauth/jwks"
    }
    
    # Mock the well-known configuration endpoint
    respx.get("https://orcid.org/.well-known/openid-configuration").mock(
        return_value=httpx.Response(200, json=mock_well_known)
    )
    
    # Mock the token exchange endpoint to return an error
    respx.post("https://orcid.org/oauth/token").mock(
        return_value=httpx.Response(400, json={
            'error': 'invalid_client', 
            'error_description': 'Client not found: APP-TEST-CLIENT-ID'
        })
    )
    
    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID",
        client_secret="test-secret", 
        well_known_uri="https://orcid.org/.well-known/openid-configuration"
    )
    
    mock_request = create_mock_OIDC_request({"code": "invalid-code"})
    
    # This should return None, not raise an exception
    result = await authenticator.authenticate(mock_request)
    assert result is None