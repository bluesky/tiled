import io
import time
from datetime import timedelta

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_config
from ..client.context import CannotRefreshAuthentication
from ..server import authentication
from .utils import fail_with_status_code

arr = ArrayAdapter.from_array(numpy.ones((5, 5)))


tree = MapAdapter({"A1": arr, "A2": arr})


@pytest.fixture
def config(tmpdir):
    """
    Return config with

    - a unique temporary sqlite database location
    - a unique nested dict instance that the test can mutate
    """
    return {
        "authentication": {
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {"alice": "secret1", "bob": "secret2"}
                    },
                }
            ],
        },
        "database": {
            "uri": f"sqlite:///{tmpdir}/tiled.sqlite",
        },
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/",
            },
        ],
    }


def test_password_auth(enter_password, config):
    """
    A password that is wrong, empty, or belonging to a different user fails.
    """

    with enter_password("secret1"):
        from_config(config, username="alice", token_cache={})
    with enter_password("secret2"):
        from_config(config, username="bob", token_cache={})

    # Bob's password should not work for alice
    with fail_with_status_code(401):
        with enter_password("secret2"):
            from_config(config, username="alice", token_cache={})

    # Empty password should not work.
    with fail_with_status_code(422):
        with enter_password(""):
            from_config(config, username="alice", token_cache={})


def test_key_rotation(enter_password, config):
    """
    Rotate in a new secret used to sign keys.
    Confirm that clients experience a smooth transition.
    """

    # Obtain refresh token.
    token_cache = {}
    with enter_password("secret1"):
        from_config(config, username="alice", token_cache=token_cache)
    # Use refresh token (no prompt to reauthenticate).
    from_config(config, username="alice", token_cache=token_cache)

    # Rotate in a new key.
    config["authentication"]["secret_keys"].insert(0, "NEW_SECRET")
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET", "SECRET"]
    # The refresh token from the old key is still valid.
    # We reauthenticate and receive a refresh token for the new key.
    from_config(config, username="alice", token_cache=token_cache)

    # Rotate out the old key.
    del config["authentication"]["secret_keys"][1]
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET"]
    # New refresh token works with the new key.
    from_config(config, username="alice", token_cache=token_cache)


def test_refresh_flow(enter_password, config):
    """
    Run a server with an artificially short max access token age
    to force a refresh.
    """

    # Normal default configuration: a refresh is not immediately required.
    token_cache = {}
    with enter_password("secret1"):
        client = from_config(config, username="alice", token_cache=token_cache)
    token1 = client.context.tokens["access_token"]
    client["A1"]
    assert token1 is client.context.tokens["access_token"]

    # Forcing a refresh gives us a new token.
    client.context.reauthenticate()
    token2 = client.context.tokens["access_token"]
    assert token2 is not token1

    # Pathological configuration: a refresh is almost immediately required
    config["authentication"]["access_token_max_age"] = timedelta(seconds=1)
    token_cache = {}
    with enter_password("secret1"):
        client = from_config(config, username="alice", token_cache=token_cache)
    token3 = client.context.tokens["access_token"]
    time.sleep(2)
    # A refresh should happen automatically now.
    client["A1"]
    token4 = client.context.tokens["access_token"]
    assert token3 is not token4

    # Pathological configuration: sessions do not last
    config["authentication"]["session_max_age"] = timedelta(seconds=1)
    token_cache = {}
    with enter_password("secret1"):
        client = from_config(config, username="alice", token_cache=token_cache)
    time.sleep(2)
    # Refresh should fail because the session is too old.
    with pytest.raises(CannotRefreshAuthentication):
        # Set prompt=False so that this raises instead of interactively prompting.
        client.context.reauthenticate(prompt=False)


def test_revoke_session(enter_password, config):
    with enter_password("secret1"):
        client = from_config(config, username="alice", token_cache={})
    # Get the current session ID.
    info = client.context.whoami()
    (session,) = info["sessions"]
    assert not session["revoked"]
    # Revoke it.
    client.context.revoke_session(session["uuid"])
    # Update info and confirm it is listed as revoked.
    updated_info = client.context.whoami()
    (updated_session,) = updated_info["sessions"]
    assert updated_session["revoked"]
    # Confirm it cannot be refreshed.
    with pytest.raises(CannotRefreshAuthentication):
        # Set prompt=False so that this raises instead of interactively prompting.
        client.context.reauthenticate(prompt=False)


def test_multiple_providers(enter_password, config, monkeypatch):
    """
    Test a configuration with multiple identity providers.

    This mechanism is used to support "Login with ORCID or Google or ...."
    """
    config["authentication"]["providers"].extend(
        [
            {
                "provider": "second",
                "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                "args": {"users_to_passwords": {"cara": "secret3", "doug": "secret4"}},
            },
            {
                "provider": "third",
                "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                "args": {
                    # Duplicate 'cara' username.
                    "users_to_passwords": {"cara": "secret5", "emilia": "secret6"}
                },
            },
        ],
    )
    monkeypatch.setattr("sys.stdin", io.StringIO("1\n"))
    with enter_password("secret1"):
        client = from_config(config, username="alice", token_cache={})
    client.context.whoami()
    monkeypatch.setattr("sys.stdin", io.StringIO("2\n"))
    with enter_password("secret3"):
        client = from_config(config, username="cara", token_cache={})
    monkeypatch.setattr("sys.stdin", io.StringIO("3\n"))
    with enter_password("secret5"):
        client = from_config(config, username="cara", token_cache={})
    client.context.whoami()


def test_multiple_providers_name_collision(config):
    """
    Check that we enforce unique provider names.
    """
    config["authentication"]["providers"] = [
        {
            "provider": "some_name",
            "authenticator": "tiled.authenticators:DictionaryAuthenticator",
            "args": {"users_to_passwords": {"cara": "secret3", "doug": "secret4"}},
        },
        {
            "provider": "some_name",  # duplicate!
            "authenticator": "tiled.authenticators:DictionaryAuthenticator",
            "args": {
                # Duplicate 'cara' username.
                "users_to_passwords": {"cara": "secret5", "emilia": "secret6"}
            },
        },
    ]
    with pytest.raises(ValueError):
        from_config(config)


def test_admin(enter_password, config):
    """
    Test that the 'tiled_admin' config confers the 'admin' Role on a Principal.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with enter_password("secret1"):
        admin_client = from_config(config, username="alice", token_cache={})

    with enter_password("secret2"):
        user_client = from_config(config, username="bob", token_cache={})

    user_roles = user_client.context.whoami()["roles"]
    assert [role["name"] for role in user_roles] == ["user"]

    adming_roles = admin_client.context.whoami()["roles"]
    assert "admin" in [role["name"] for role in adming_roles]


def test_api_keys(enter_password, config):
    """
    Test creating, revoking, expiring API keys.
    Test that they have appropriate scope-limited access.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with enter_password("secret1"):
        admin_client = from_config(config, username="alice", token_cache={})

    with enter_password("secret2"):
        user_client = from_config(config, username="bob", token_cache={})

    # Make and use an API key. Check that latest_activity is updated.
    user_key_info = user_client.context.create_api_key()
    assert user_key_info["latest_activity"] is None  # never used
    user_client_from_key = from_config(config, api_key=user_key_info["secret"])
    # Check that api_key property is set.
    assert user_client_from_key.context.api_key == user_key_info["secret"]
    # Use the key for a couple requests and see that latest_activity becomes set and then increases.
    user_client_from_key["A1"]
    key_activity1 = user_client_from_key.context.which_api_key()["latest_activity"]
    principal_activity1 = user_client_from_key.context.whoami()["latest_activity"]
    assert key_activity1 is not None
    time.sleep(2)  # Ensure time resolution (1 second) has ticked up.
    user_client_from_key["A1"]
    key_activity2 = user_client_from_key.context.which_api_key()["latest_activity"]
    principal_activity2 = user_client_from_key.context.whoami()["latest_activity"]
    assert key_activity2 > key_activity1
    assert principal_activity2 > principal_activity1
    assert len(user_client_from_key.context.whoami()["api_keys"]) == 1

    # Unset the API key.
    secret = user_client_from_key.context.api_key
    user_client_from_key.context.api_key = None
    with pytest.raises(RuntimeError):
        user_client_from_key.context.which_api_key()
    # Set the API key.
    user_client_from_key.context.api_key = secret
    # Now this works again.
    user_client_from_key.context.which_api_key()

    # Request a key with reduced scope that cannot read metadata.
    admin_key_info = admin_client.context.create_api_key(scopes=["metrics"])
    with fail_with_status_code(401):
        from_config(config, api_key=admin_key_info["secret"])

    # Request a key with reduced scope that can *only* read metadata.
    admin_key_info = admin_client.context.create_api_key(scopes=["read:metadata"])
    restricted_client = from_config(config, api_key=admin_key_info["secret"])
    restricted_client["A1"]
    with fail_with_status_code(401):
        restricted_client["A1"].read()  # no 'read:data' scope

    # Try to request a key with more scopes that the user has.
    with fail_with_status_code(400):
        user_client.context.create_api_key(scopes=["admin:apikeys"])

    # Create and revoke key.
    user_key_info = user_client.context.create_api_key(note="will revoke soon")
    assert len(user_client_from_key.context.whoami()["api_keys"]) == 2
    # There should now be two keys, one from above and this new one, with our note.
    for api_key in user_client_from_key.context.whoami()["api_keys"]:
        if api_key["note"] == "will revoke soon":
            break
    else:
        assert False, "No api keys had a matching note."
    # Revoke the new key.
    user_client_from_key.context.revoke_api_key(user_key_info["first_eight"])
    with fail_with_status_code(401):
        from_config(config, api_key=user_key_info["secret"])
    assert len(user_client_from_key.context.whoami()["api_keys"]) == 1

    # Create a key with a very short lifetime.
    user_key_info = user_client.context.create_api_key(
        note="will expire very soon", expires_in=1
    )  # units: seconds
    time.sleep(2)
    with fail_with_status_code(401):
        from_config(config, api_key=user_key_info["secret"])


def test_api_key_limit(enter_password, config):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.API_KEY_LIMIT
    authentication.API_KEY_LIMIT = 3
    try:
        with enter_password("secret2"):
            user_client = from_config(config, username="bob", token_cache={})

        for i in range(authentication.API_KEY_LIMIT):
            user_client.context.create_api_key(note=f"key {i}")
        # Hit API key limit.
        with fail_with_status_code(400):
            user_client.context.create_api_key(note="one key too many")
    finally:
        authentication.API_KEY_LIMIT = original_limit


def test_session_limit(enter_password, config):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.SESSION_LIMIT
    authentication.SESSION_LIMIT = 3
    try:
        with enter_password("secret1"):
            for _ in range(authentication.SESSION_LIMIT):
                from_config(config, username="alice", token_cache={})
            # Hit Session limit.
            with fail_with_status_code(400):
                from_config(config, username="alice", token_cache={})
    finally:
        authentication.SESSION_LIMIT = original_limit
