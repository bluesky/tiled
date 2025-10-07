import asyncio
import os

import pytest
from ..authenticators import LDAPAuthenticator, OIDCAuthenticator
from unittest.mock import AsyncMock, Mock, patch
from starlette.requests import Request


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

#add test for the authenticator for oidc



@pytest.mark.asyncio
async def test_OIDCAuthenticator_mock():
    """
    Test OIDCAuthenticator with mocked external dependencies.
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
    
    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID",
        client_secret="test-secret",
        well_known_uri="https://orcid.org/.well-known/openid-configuration"
    )
    
    # Create a mock request with code parameter
    mock_request = Mock(spec=Request)
    mock_request.query_params = {"code": "test-auth-code"}
    mock_request.scope = {
        "type": "http",
        "scheme": "http",
        "server": ("localhost", 8000),
        "path": "/api/v1/auth/provider/orcid/code",
        "headers": []
    }
    mock_request.headers = {"host": "localhost:8000"}
    mock_request.url = Mock()
    mock_request.url.path = "/api/v1/auth/provider/orcid/code"
    
    with patch('httpx.AsyncClient') as mock_client, \
         patch('jose.jwt.decode') as mock_jwt_decode, \
         patch('jose.jwk.construct') as mock_jwk_construct, \
         patch('tiled.authenticators.exchange_code') as mock_exchange_code, \
         patch('httpx.get') as mock_httpx_get:
        
        # Mock HTTP client responses
        mock_async_client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = mock_async_client
        
        # Mock well-known config fetch
        mock_well_known_response = Mock()
        mock_well_known_response.json.return_value = mock_well_known
        mock_async_client.get.return_value = mock_well_known_response
        
        # Mock token exchange
        mock_token_response = Mock()
        mock_token_response.is_error = False
        mock_token_response.json.return_value = {
            "access_token": "mock-access-token",
            "id_token": "mock-id-token",
            "token_type": "bearer"
        }
        mock_exchange_code.return_value = mock_token_response
        
        # Mock JWKS fetch
        mock_jwks_response = Mock()
        mock_jwks_response.raise_for_status.return_value = mock_jwks_response
        mock_jwks_response.json.return_value = {"keys": [{"kid": "test", "kty": "RSA"}]}
        mock_httpx_get.return_value = mock_jwks_response
        
        # Mock JWT verification
        mock_jwt_decode.return_value = mock_jwt_payload
        mock_jwk_construct.return_value = Mock()
        
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
    
    # Create mock request without code parameter
    mock_request = Mock(spec=Request)
    mock_request.query_params = {} #Empty, no code parameter - should raise KeyError
    mock_request.scope = {
        "type": "http",
        "scheme": "http", 
        "server": ("localhost", 8000),
        "path": "/api/v1/auth/provider/orcid/code",
        "headers": []
    }
    mock_request.headers = {"host": "localhost:8000"}
    mock_request.url = Mock()
    mock_request.url.path = "/api/v1/auth/provider/orcid/code"
    
    result = await authenticator.authenticate(mock_request)
    assert result is None


@pytest.mark.asyncio
async def test_OIDCAuthenticator_token_exchange_failure():
    """
    Test OIDCAuthenticator when token exchange fails.
    """
    authenticator = OIDCAuthenticator(
        audience="APP-TEST-CLIENT-ID",
        client_id="APP-TEST-CLIENT-ID",
        client_secret="test-secret", 
        well_known_uri="https://orcid.org/.well-known/openid-configuration"
    )
    
    mock_request = Mock(spec=Request)
    mock_request.query_params = {"code": "invalid-code"}
    mock_request.scope = {
        "type": "http",
        "scheme": "http",
        "server": ("localhost", 8000), 
        "path": "/api/v1/auth/provider/orcid/code",
        "headers": []
    }
    mock_request.headers = {"host": "localhost:8000"}
    mock_request.url = Mock()
    mock_request.url.path = "/api/v1/auth/provider/orcid/code"
    
    with patch('httpx.AsyncClient') as mock_client, \
         patch('tiled.authenticators.exchange_code') as mock_exchange_code:
        mock_async_client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = mock_async_client
        
        # Mock well-known config
        mock_well_known_response = Mock()
        mock_well_known_response.json.return_value = {
            "token_endpoint": "https://orcid.org/oauth/token",
            "jwks_uri": "https://orcid.org/oauth/jwks"
        }
        mock_async_client.get.return_value = mock_well_known_response
        
        # Mock failed token exchange - set is_error to True
        mock_token_response = Mock()
        mock_token_response.is_error = True
        mock_token_response.json.return_value = {
            'error': 'invalid_client', 
            'error_description': 'Client not found: APP-TEST-CLIENT-ID'
        }
        mock_exchange_code.return_value = mock_token_response
        
        # This should return None, not raise an exception
        result = await authenticator.authenticate(mock_request)
        assert result is None