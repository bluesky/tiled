import io
import os
import shutil
import subprocess
import sys
import time

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.auth import CannotRefreshAuthentication
from ..client.context import clear_default_identity, get_default_identity
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


def test_password_auth(enter_password, config):
    """
    A password that is wrong, empty, or belonging to a different user fails.
    """
    with Context.from_app(build_app_from_config(config)) as context:
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


def test_logout(enter_password, config, tmpdir):
    """
    Logging out revokes the session, such that it cannot be refreshed.
    """
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as Alice.
        with enter_password("secret1"):
            from_context(context, username="alice")
        # Reuse token from cache.
        client = from_context(context, username="alice")
        # This was set to a unique (temporary) dir by an autouse fixture in conftest.py.
        tiled_cache_dir = os.environ["TILED_CACHE_DIR"]
        # Make a backup copy of the cache directory, which contains the auth tokens.
        shutil.copytree(tiled_cache_dir, tmpdir / "backup")
        # Logout does two things:
        # 1. Revoke the session, so that it cannot be refreshed.
        # 2. Clear the tokens related to this session from in-memory state
        #    and on-disk state.
        client.logout()
        # Restore the tokens from backup.
        # Our aim is to test, below, that even if you have the tokens they
        # can't be used anymore.
        shutil.rmtree(tiled_cache_dir)
        shutil.copytree(tmpdir / "backup", tiled_cache_dir)
        # There is no way to revoke a JWT access token. It expires after a
        # short time window (minutes) but it will still work here, as it has
        # not been that long.
        client = from_context(context, username="alice")
        # The refresh token refers to a revoked session, so refreshing the
        # session to generate a *new* access and refresh token will fail.
        with pytest.raises(CannotRefreshAuthentication):
            client.context.force_auth_refresh()


def test_key_rotation(enter_password, config):
    """
    Rotate in a new secret used to sign keys.
    Confirm that clients experience a smooth transition.
    """

    with Context.from_app(build_app_from_config(config)) as context:
        # Obtain refresh token.
        with enter_password("secret1"):
            from_context(context, username="alice")
        # Use refresh token (no prompt to reauthenticate).
        client = from_context(context, username="alice")

    # Rotate in a new key.
    config["authentication"]["secret_keys"].insert(0, "NEW_SECRET")
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET", "SECRET"]

    with Context.from_app(build_app_from_config(config)) as context:
        # The refresh token from the old key is still valid. No login prompt here.
        client = from_context(context, username="alice")
        # We reauthenticate and receive a refresh token for the new key.
        # (This would happen on its own with the passage of time, but we force it
        # for the sake of a quick test.)
        client.context.force_auth_refresh()

    # Rotate out the old key.
    del config["authentication"]["secret_keys"][1]
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET"]

    with Context.from_app(build_app_from_config(config)) as context:
        # New refresh token works with the new key
        from_context(context, username="alice")


def test_refresh_forced(enter_password, config):
    "Forcing refresh obtains new token."
    with Context.from_app(build_app_from_config(config)) as context:
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


def test_refresh_transparent(enter_password, config):
    "When access token expired, refresh happens transparently."
    # Pathological configuration: a refresh is almost immediately required
    config["authentication"]["access_token_max_age"] = 1
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_password("secret1"):
            client = from_context(context, username="alice")
        tokens1 = dict(client.context.tokens)
        time.sleep(2)
        # A refresh should happen automatically now.
        client["A1"]
        tokens2 = dict(client.context.tokens)
        assert tokens2 != tokens1


def test_expired_session(enter_password, config):
    # Pathological configuration: sessions do not last
    config["authentication"]["session_max_age"] = 1
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_password("secret1"):
            client = from_context(context, username="alice")
        time.sleep(2)
        # Refresh should fail because the session is too old.
        with pytest.raises(CannotRefreshAuthentication):
            client.context.force_auth_refresh()


def test_revoke_session(enter_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
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
    with Context.from_app(build_app_from_config(config)) as context:
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


def test_admin(enter_password, config):
    """
    Test that the 'tiled_admin' config confers the 'admin' Role on a Principal.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with Context.from_app(build_app_from_config(config)) as context:
        with enter_password("secret1"):
            context.authenticate(username="alice")
        admin_roles = context.whoami()["roles"]
        assert "admin" in [role["name"] for role in admin_roles]

        # Exercise admin functions.
        principals = context.admin.list_principals()
        some_principal_uuid = principals[0]["uuid"]
        context.admin.show_principal(some_principal_uuid)

        with enter_password("secret2"):
            context.authenticate(username="bob")
        user_roles = context.whoami()["roles"]
        assert [role["name"] for role in user_roles] == ["user"]

        # As bob, admin functions should be disallowed.
        with fail_with_status_code(401):
            context.admin.list_principals()
        with fail_with_status_code(401):
            context.admin.show_principal(some_principal_uuid)


def test_api_key_activity(enter_password, config):
    """
    Create and use an API. Verify that latest_activity updates.
    """
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as user.
        with enter_password("secret1"):
            context.authenticate(username="alice")
        # Make and use an API key. Check that latest_activity is not set.
        key_info = context.create_api_key()
        context.logout()
        assert key_info["latest_activity"] is None  # never used
        context.api_key = key_info["secret"]

        # Use the key for a couple requests and see that latest_activity becomes set and then increases.
        client = from_context(context)
        client["A1"]
        key_activity1 = context.which_api_key()["latest_activity"]
        principal_activity1 = context.whoami()["latest_activity"]
        assert key_activity1 is not None
        time.sleep(2)  # Ensure time resolution (1 second) has ticked up.
        client["A1"]
        key_activity2 = context.which_api_key()["latest_activity"]
        principal_activity2 = context.whoami()["latest_activity"]
        assert key_activity2 > key_activity1
        assert principal_activity2 > principal_activity1
        assert len(context.whoami()["api_keys"]) == 1

        # Unset the API key.
        secret = context.api_key
        context.api_key = None
        with pytest.raises(RuntimeError):
            context.which_api_key()
        # Set the API key.
        context.api_key = secret
        # Now this works again.
        context.which_api_key()


def test_api_key_scopes(enter_password, config):
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as admin.
        with enter_password("secret1"):
            context.authenticate(username="alice")
        # Request a key with reduced scope that cannot read metadata.
        metrics_key_info = context.create_api_key(scopes=["metrics"])
        context.logout()
        context.api_key = metrics_key_info["secret"]
        with fail_with_status_code(401):
            from_context(context)
        context.api_key = None

        # Log in as ordinary user.
        with enter_password("secret2"):
            context.authenticate(username="bob")
        # Try to request a key with more scopes that the user has.
        with fail_with_status_code(400):
            context.create_api_key(scopes=["admin:apikeys"])
        # Request a key with reduced scope that can *only* read metadata.
        metadata_key_info = context.create_api_key(scopes=["read:metadata"])
        context.logout()
        context.api_key = metadata_key_info["secret"]
        restricted_client = from_context(context)
        restricted_client["A1"]
        with fail_with_status_code(401):
            restricted_client["A1"].read()  # no 'read:data' scope
        context.api_key = None


def test_api_key_revoked(enter_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_password("secret1"):
            context.authenticate(username="alice")

        # Create a key with a note.
        NOTE = "will revoke soon"
        key_info = context.create_api_key(note=NOTE)
        assert len(context.whoami()["api_keys"]) == 1
        (api_key,) = context.whoami()["api_keys"]
        assert api_key["note"] == key_info["note"] == NOTE
        assert api_key["first_eight"] == key_info["first_eight"]
        context.logout()

        # Use it.
        context.api_key = key_info["secret"]
        from_context(context)
        context.api_key = None

        # Revoke the new key.
        with enter_password("secret1"):
            context.authenticate(username="alice")
        context.revoke_api_key(key_info["first_eight"])
        assert len(context.whoami()["api_keys"]) == 0
        context.logout()
        context.api_key = key_info["secret"]
        with fail_with_status_code(401):
            from_context(context)


def test_api_key_expiration(enter_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_password("secret1"):
            context.authenticate(username="alice")
        # Create a key with a very short lifetime.
        key_info = context.create_api_key(
            note="will expire very soon", expires_in=1
        )  # units: seconds
        context.logout()
        context.api_key = key_info["secret"]
        time.sleep(2)
        with fail_with_status_code(401):
            from_context(context)


def test_api_key_limit(enter_password, config):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.API_KEY_LIMIT
    authentication.API_KEY_LIMIT = 3
    try:
        with Context.from_app(build_app_from_config(config)) as context:
            with enter_password("secret2"):
                context.authenticate(username="bob")
            for i in range(authentication.API_KEY_LIMIT):
                context.create_api_key(note=f"key {i}")
            # Hit API key limit.
            with fail_with_status_code(400):
                context.create_api_key(note="one key too many")
    finally:
        authentication.API_KEY_LIMIT = original_limit


def test_session_limit(enter_password, config):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.SESSION_LIMIT
    authentication.SESSION_LIMIT = 3
    # Use separate token caches to de-couple login attempts into separate sessions.
    try:
        with Context.from_app(build_app_from_config(config)) as context:
            with enter_password("secret1"):
                for i in range(authentication.SESSION_LIMIT):
                    context.authenticate(username="alice")
                    context.logout()
                # Hit Session limit.
                with fail_with_status_code(400):
                    context.authenticate(username="alice")
    finally:
        authentication.SESSION_LIMIT = original_limit


def test_sticky_identity(enter_password, config):
    # Log in as Alice.
    with Context.from_app(build_app_from_config(config)) as context:
        assert get_default_identity(context.api_uri) is None
        with enter_password("secret1"):
            context.authenticate(username="alice")
        assert context.whoami()["identities"][0]["id"] == "alice"
    # The default identity is now set. The login was "sticky".
    with Context.from_app(build_app_from_config(config)) as context:
        assert get_default_identity(context.api_uri) is not None
        context.authenticate()
        assert context.whoami()["identities"][0]["id"] == "alice"
    # Opt out of the stickiness (set_default=False).
    with Context.from_app(build_app_from_config(config)) as context:
        assert get_default_identity(context.api_uri) is not None
        with enter_password("secret2"):
            context.authenticate(username="bob", set_default=False)
        assert context.whoami()["identities"][0]["id"] == "bob"
    # The default is still Alice.
    with Context.from_app(build_app_from_config(config)) as context:
        assert get_default_identity(context.api_uri) is not None
        context.authenticate()
        assert context.whoami()["identities"][0]["id"] == "alice"
    # Clear the default.
    clear_default_identity(context.api_uri)
    assert get_default_identity(context.api_uri) is None


@pytest.fixture
def principals_context(enter_password, config):
    """
    Fetch UUID for an admin and an ordinary user; include the client context.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as Alice and retrieve admin UUID for later use
        with enter_password("secret1"):
            context.authenticate(username="alice")

        principal = context.whoami()
        assert "admin" in (role["name"] for role in principal["roles"])
        admin_uuid = principal["uuid"]
        context.logout()

        # Log in as Bob and retrieve Bob's UUID for later use
        with enter_password("secret2"):
            context.authenticate(username="bob")

        principal = context.whoami()
        assert "admin" not in (role["name"] for role in principal["roles"])
        bob_uuid = principal["uuid"]
        context.logout()

        yield {
            "uuid": {"alice": admin_uuid, "bob": bob_uuid},
            "context": context,
        }


@pytest.mark.parametrize(
    "username, scopes, resource",
    (
        ("alice", ["read:principals"], "/api/v1/auth/principal"),
        ("bob", ["read:data"], "/api/v1/array/full/A1"),
    ),
)
def test_admin_api_key_any_principal(
    enter_password, principals_context, username, scopes, resource
):
    """
    Admin can create usable API keys for any prinicipal, within that principal's scopes.
    """
    with principals_context["context"] as context:
        # Log in as Alice, create and use API key after logout
        with enter_password("secret1"):
            context.authenticate(username="alice")

        principal_uuid = principals_context["uuid"][username]
        api_key_info = context.admin.create_api_key(principal_uuid, scopes=scopes)
        api_key = api_key_info["secret"]
        assert api_key
        context.logout()

        context.api_key = api_key
        context.http_client.get(resource).raise_for_status()
        context.api_key = None
        # The same endpoint fails without an API key
        with fail_with_status_code(401):
            context.http_client.get(resource).raise_for_status()


def test_admin_create_service_principal(enter_password, principals_context):
    """
    Admin can create service accounts with API keys.
    """
    with principals_context["context"] as context:
        # Log in as Alice, create and use API key after logout
        with enter_password("secret1"):
            context.authenticate(username="alice")

        assert context.whoami()["type"] == "user"

        principal_info = context.admin.create_service_principal(role="user")
        principal_uuid = principal_info["uuid"]

        service_api_key_info = context.admin.create_api_key(principal_uuid)
        context.logout()

        context.api_key = service_api_key_info["secret"]
        assert context.whoami()["type"] == "service"


def test_admin_api_key_any_principal_exceeds_scopes(enter_password, principals_context):
    """
    Admin cannot create API key that exceeds scopes for another principal.
    """
    with principals_context["context"] as context:
        # Log in as Alice, create and use API key after logout
        with enter_password("secret1"):
            context.authenticate(username="alice")

        principal_uuid = principals_context["uuid"]["bob"]
        with fail_with_status_code(400) as fail_info:
            context.admin.create_api_key(principal_uuid, scopes=["read:principals"])
        fail_message = " must be a subset of the principal's scopes "
        assert fail_message in fail_info.value.response.text
        context.logout()


@pytest.mark.parametrize("username", ("alice", "bob"))
def test_api_key_any_principal(enter_password, principals_context, username):
    """
    Ordinary user cannot create API key for another principal.
    """
    with principals_context["context"] as context:
        # Log in as Bob, this API endpoint is unauthorized
        with enter_password("secret2"):
            context.authenticate(username="bob")

        principal_uuid = principals_context["uuid"][username]
        with fail_with_status_code(401):
            context.admin.create_api_key(principal_uuid, scopes=["read:metadata"])


def test_api_key_bypass_scopes(enter_password, principals_context):
    """
    Ordinary user cannot create API key that bypasses a scopes restriction.
    """
    with principals_context["context"] as context:
        # Log in as Bob, create API key with empty scopes
        with enter_password("secret2"):
            context.authenticate(username="bob")

        response = context.http_client.post(
            "/api/v1/auth/apikey", json={"expires_in": None, "scopes": []}
        )
        response.raise_for_status()
        api_key = response.json()["secret"]
        assert api_key
        context.logout()

        # Try the new API key with admin and normal resources
        for resource in ("/api/v1/auth/principal", "/api/v1/array/full/A1"):
            # Try with/without key, with/without empty scopes
            for query_params in (
                {"api_key": api_key},
                {"scopes": []},
                {"api_key": api_key, "scopes": []},
            ):
                context.api_key = query_params.pop("api_key", None)
                with fail_with_status_code(401):
                    context.http_client.get(
                        resource, params=query_params
                    ).raise_for_status()
