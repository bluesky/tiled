import asyncio
import os

import pytest

from ..authenticators import LDAPAuthenticator

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
@pytest.mark.skipif(not TILED_TEST_LDAP, reason="Requires an LDAP container and TILED_TEST_LDAP to be set")
def test_LDAPAuthenticator_01(use_tls, use_ssl, ldap_server_address, ldap_server_port):
    """
    Basic test for ``LDAPAuthenticator``.

    TODO: The test could be extended with enabled TLS or SSL, but it requires configuration
    of the LDAP server.
    """

    authenticator = LDAPAuthenticator(
        server_address=ldap_server_address,
        bind_dn_template="cn={username},ou=users,dc=example,dc=org",
        use_ssl=use_ssl,
        use_tls=use_tls,
        server_port=ldap_server_port
    )

    async def testing():
        assert (await authenticator.authenticate("user01", "password1")).user_name == "user01"
        assert (await authenticator.authenticate("user02", "password2")).user_name == "user02"
        assert (await authenticator.authenticate("user02a", "password2")) is None
        assert (await authenticator.authenticate("user02", "password2a")) is None

    asyncio.run(testing())


def test_ldap_port_validation():
    # given port can be none but will be replaced with a default
    auth = LDAPAuthenticator(server_address="http://ldap.example.com", server_port=None)
    assert auth.server_port is not None


def test_auth_server_list_wrapping():
    auth = LDAPAuthenticator(server_address="http://ldap.example.com", server_port=None)
    assert auth.server_address_list == ["http://ldap.example.com"]


def test_list_of_addresses_not_nested_into_extra_list():
    auth = LDAPAuthenticator(server_address=["http://ldap.example.com"])
    assert auth.server_address_list == ["http://ldap.example.com"]
