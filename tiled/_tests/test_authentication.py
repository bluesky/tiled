import io
import subprocess
import sys
import time

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.auth import CannotRefreshAuthentication
from ..server import authentication
from ..server.app import build_app_from_config
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
    database_uri = f"sqlite+aiosqlite:///{tmpdir}/tiled.sqlite"
    subprocess.run(
        [sys.executable, "-m", "tiled", "admin", "initialize-database", database_uri],
        check=True,
        capture_output=True,
    )
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
            "uri": database_uri,
        },
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/",
            },
        ],
    }


def test_password_auth(enter_password, config, tmpdir):
    """
    A password that is wrong, empty, or belonging to a different user fails.
    """
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        # Log in as Alice.
        with enter_password("secret1"):
            from_context(context, username="alice")
        # Reuse token from cache.
        client = from_context(context, username="alice")
        client.logout()

        # Log in as Bob.
        with enter_password("secret2"):
            client = from_context(context, username="bob")
        client.logout()

        # Bob's password should not work for Alice.
        with fail_with_status_code(401):
            with enter_password("secret2"):
                from_context(context, username="alice")

        # Empty password should not work.
        with fail_with_status_code(422):
            with enter_password(""):
                from_context(context, username="alice")


def test_key_rotation(enter_password, config, tmpdir):
    """
    Rotate in a new secret used to sign keys.
    Confirm that clients experience a smooth transition.
    """

    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        # Obtain refresh token.
        with enter_password("secret1"):
            from_context(context, username="alice")
        # Use refresh token (no prompt to reauthenticate).
        client = from_context(context, username="alice")

    # Rotate in a new key.
    config["authentication"]["secret_keys"].insert(0, "NEW_SECRET")
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET", "SECRET"]

    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        # The refresh token from the old key is still valid. No login prompt here.
        client = from_context(context, username="alice")
        # We reauthenticate and receive a refresh token for the new key.
        # (This would happen on its own with the passage of time, but we force it
        # for the sake of a quick test.)
        client.context.force_auth_refresh()

    # Rotate out the old key.
    del config["authentication"]["secret_keys"][1]
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET"]

    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        # New refresh token works with the new key
        from_context(context, username="alice")


def test_refresh_forced(enter_password, config, tmpdir):
    "Forcing refresh obtains new token."
    from tiled.client import show_logs

    show_logs()
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        # Normal default configuration: a refresh is not immediately required.
        with enter_password("secret1"):
            client = from_context(context, username="alice")
        tokens1 = dict(client.context.tokens)
        # Wait for a moment or we will get a new token that happens to be identical
        # to the old token. This advances the expiration time to make a distinct token.
        time.sleep(2)
        # Forcing a refresh gives us a new token.
        client.context.force_auth_refresh()
        tokens2 = dict(client.context.tokens)
        assert tokens1 != tokens2
        client.logout()


def test_refresh_transparent(enter_password, config, tmpdir):
    "When access token expired, refresh happens transparently."
    # Pathological configuration: a refresh is almost immediately required
    config["authentication"]["access_token_max_age"] = 1
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        with enter_password("secret1"):
            client = from_context(context, username="alice")
        tokens1 = dict(client.context.tokens)
        time.sleep(2)
        # A refresh should happen automatically now.
        client["A1"]
        tokens2 = dict(client.context.tokens)
        assert tokens2 != tokens1
        client.logout()


def test_expired_session(enter_password, config, tmpdir):
    # Pathological configuration: sessions do not last
    config["authentication"]["session_max_age"] = 1
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        with enter_password("secret1"):
            client = from_context(context, username="alice")
        time.sleep(2)
        # Refresh should fail because the session is too old.
        with pytest.raises(CannotRefreshAuthentication):
            client.context.force_auth_refresh()


def test_revoke_session(enter_password, config, tmpdir):
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        with enter_password("secret1"):
            client = from_context(context, username="alice")
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
            client.context.force_auth_refresh()


def test_multiple_providers(enter_password, config, monkeypatch, tmpdir):
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
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        monkeypatch.setattr("sys.stdin", io.StringIO("1\n"))
        with enter_password("secret1"):
            from_context(context, username="alice")
        monkeypatch.setattr("sys.stdin", io.StringIO("2\n"))
        with enter_password("secret3"):
            from_context(context, username="cara")
        monkeypatch.setattr("sys.stdin", io.StringIO("3\n"))
        with enter_password("secret5"):
            from_context(context, username="cara")


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
        build_app_from_config(config)


def test_admin(enter_password, config, tmpdir):
    """
    Test that the 'tiled_admin' config confers the 'admin' Role on a Principal.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        with enter_password("secret1"):
            context.authenticate(username="alice")
        admin_roles = context.whoami()["roles"]
        assert "admin" in [role["name"] for role in admin_roles]
        with enter_password("secret2"):
            context.authenticate(username="bob")
        user_roles = context.whoami()["roles"]
        assert [role["name"] for role in user_roles] == ["user"]


def test_api_keys(enter_password, config, tmpdir):
    """
    Test creating, revoking, expiring API keys.
    Test that they have appropriate scope-limited access.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with enter_password("secret2"):
        with from_config(config, username="bob", token_cache=tmpdir) as user_client:
            # Try to request a key with more scopes that the user has.
            with fail_with_status_code(400):
                user_client.context.create_api_key(scopes=["admin:apikeys"])
            # Make and use an API key. Check that latest_activity is updated.
            user_key_info = user_client.context.create_api_key()
            assert user_key_info["latest_activity"] is None  # never used
    with from_config(config, api_key=user_key_info["secret"]) as user_client_from_key:
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
            with from_config(config, api_key=user_key_info["secret"]):
                pass
    with enter_password("secret1"):
        with from_config(
            config,
            username="alice",
            token_cache=tmpdir,
            prompt_for_reauthentication=True,
        ) as admin_client:
            # Request a key with reduced scope that cannot read metadata.
            admin_key_info = admin_client.context.create_api_key(scopes=["metrics"])
            with fail_with_status_code(401):
                from_config(config, api_key=admin_key_info["secret"])

            # Request a key with reduced scope that can *only* read metadata.
            admin_key_info = admin_client.context.create_api_key(
                scopes=["read:metadata"]
            )
            with from_config(
                config, api_key=admin_key_info["secret"]
            ) as restricted_client:
                restricted_client["A1"]
                with fail_with_status_code(401):
                    restricted_client["A1"].read()  # no 'read:data' scope


def test_api_key_limit(enter_password, config, tmpdir):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.API_KEY_LIMIT
    authentication.API_KEY_LIMIT = 3
    try:
        with Context.from_app(
            build_app_from_config(config), token_cache=tmpdir
        ) as context:
            with enter_password("secret2"):
                context.authenticate(username="bob")
            for i in range(authentication.API_KEY_LIMIT):
                context.create_api_key(note=f"key {i}")
            # Hit API key limit.
            with fail_with_status_code(400):
                context.create_api_key(note="one key too many")
    finally:
        authentication.API_KEY_LIMIT = original_limit


def test_session_limit(enter_password, config, tmpdir):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.SESSION_LIMIT
    authentication.SESSION_LIMIT = 3
    # Use separate token caches to de-couple login attempts into separate sessions.
    try:
        with Context.from_app(
            build_app_from_config(config), token_cache=tmpdir
        ) as context:
            with enter_password("secret1"):
                for i in range(authentication.SESSION_LIMIT):
                    context.authenticate(username="alice")
                    context.logout()
                # Hit Session limit.
                with fail_with_status_code(400):
                    context.authenticate(username="alice")
    finally:
        authentication.SESSION_LIMIT = original_limit
