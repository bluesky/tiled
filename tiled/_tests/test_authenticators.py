import asyncio
import os
import time
from typing import Any

import httpx
import pytest
from jose import ExpiredSignatureError, jwt
from jose.backends import RSAKey
from respx import MockRouter

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
def well_known_response() -> dict[str, Any]:
    return {
        "id_token_signing_alg_values_supported": ["RS256"],
        "issuer": "https://example.com/realms/example",
        "jwks_uri": "https://example.com/realms/example/protocol/openid-connect/certs",
        "authorization_endpoint": "https://example.com/realms/example/protocol/openid-connect/auth",
        "token_endpoint": "https://example.com/realms/example/protocol/openid-connect/token"
    }


@pytest.fixture
def well_known_url() -> str:
    return "http://example.com/well_known/"


@pytest.fixture
def mock_oidc_server(
    respx_mock: MockRouter,
    well_known_url: str,
    well_known_response: dict[str, Any],
    json_web_keyset: list[dict[str, Any]],
) -> MockRouter:
    respx_mock.get(well_known_url).mock(
        return_value=httpx.Response(200, json=well_known_response)
    )
    respx_mock.get(well_known_response["jwks_uri"]).mock(
        return_value=httpx.Response(200, json={"keys": json_web_keyset})
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

    assert authenticator.keys() == json_web_keyset
    assert len(mock_oidc_server.calls) == 3  # Getting keys is not cached
    keys_request = mock_oidc_server.calls[2].request
    assert keys_request.method == "GET"
    assert keys_request.url == well_known_response["jwks_uri"]


@pytest.mark.parametrize("issued", [True, False])
@pytest.mark.parametrize("expired", [True, False])
def test_oidc_decoding(
    mock_oidc_server: MockRouter,
    well_known_url: str,
    issued: bool,
    expired: bool,
    private_key: str
):

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
def private_key() -> str:
    # Key generated just for these tests
    return """-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQCGwHwO3J7L0vdGOw1Hhi6AoN1vnJvDxiUcDu+vF11T6G3KXTpP
4hGtRTTjemio7kDZKIrX1sDeRRvvBatKkEWV6hgQbzQwllqV6O/McpUeG4snoziB
dPEQ/2DvA8Dik1j3v7jG0ATy+M6EkTmsS7z0H9Eha0wujsrvQxxOV0N1jwIDAQAB
AoGAYDQqHd4qzPAINC7Ssz68En9GuHmBx4q+UcLkIgg3TEGDqNdYW1HWNvNS6Bkr
gXff+mn0flZHCiki4UoV2b0Yv/PX/359aXrvtVdcJQfjXj9nEZTFLhd36ARZrrD7
J+EtHclO7SNjGN3KvhFbUWZ4qgTeNRs7Qa3G0AadlY/ogpkCQQD3dK+/Kn488EjP
auUC3Rv4h5KpLk1m7d0W2/+fH+UODVgRjCzH9NIQpaET0uXDMzb3UclHYc48UtxD
OUVhfEftAkEAi2eVrkE1maBQIsvC+wBVavMpleSncUH6h1JvI/gSzApOhWzOSAhy
AnZ2Zq6mFtqBLZhz2xm8qCXlMkT17CdL6wJBAKm6ED1HkRSNHvOddvyS2feKTa7a
wl5B8i4WsWrcPoh34JsQkTqJEng2kpf9RHixrRbPswXR8NnxX4CATLVDwDUCQEWH
9PBlNgbaHx4745SuJeyiPCu3UIz9C6hTRXv7T+TVfzStgHYNQFBaJdQxaEYd1jCX
ybGOtLpprFfWbZLMRuECQQDtef88ZQUBrMMCleCHP2S+dbLuOxNSEoL3/AzxvVzQ
MKOzPo5n3HuLXn3c+ej9hpna8XZKweNKb9s44fMBnQh8
-----END RSA PRIVATE KEY-----"""


@pytest.fixture
def json_web_keyset(private_key: str) -> list[dict[str, Any]]:
    return [
        RSAKey(key=private_key, algorithm="RS256").to_dict()
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


def encrypted_token(token: dict[str, str], private_key: str) -> str:
    return jwt.encode(
        token,
        key=private_key,
        algorithm="RS256",
        headers={"kid": "secret"},
    )


@pytest.mark.asyncio
async def test_proxied_oidc_token_retrieval(well_known_url: str, mock_oidc_server: MockRouter):
    authenticator = ProxiedOIDCAuthenticator("tiled", "tiled", well_known_url)
    test_request = httpx.Request("GET", "http://example.com", headers={
        "Authorization": "bearer FOO"
    })

    assert "FOO" == await authenticator.oauth2_schema(test_request)
