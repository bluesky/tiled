import io
import os
import shutil
import subprocess
import sys
import time

import numpy
import pytest
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
)

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.auth import CannotRefreshAuthentication
from ..client.context import PasswordRejected
from ..server import authentication
from ..server.app import build_app_from_config
from .utils import fail_with_status_code

arr = ArrayAdapter.from_array(numpy.ones((5, 5)))


tree = MapAdapter({"A1": arr, "A2": arr})


@pytest.fixture
def config(sqlite_or_postgres_uri):
    """
    Return config with

    - a unique temporary sqlite database location
    - a unique nested dict instance that the test can mutate
    """
    database_uri = sqlite_or_postgres_uri
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


def test_password_auth(enter_username_password, config):
    """
    A password that is wrong, empty, or belonging to a different user fails.
    """
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as Alice.
        with enter_username_password("alice", "secret1"):
            from_context(context)
        # Reuse token from cache.
        client = from_context(context)
        # Check user authentication status string
        assert "authenticated as 'alice'" in repr(client.context)
        # Check authenticated property exists
        assert "authenticated" in dir(client.context)
        # Check authenticated property is True
        assert client.context.authenticated
        client.logout()
        # Check authentication status string
        assert "unauthenticated" in repr(client.context)
        # Check authenticated property still exists
        assert "authenticated" in dir(client.context)
        # Check authenticated property is False
        assert not client.context.authenticated

        # Log in as Bob.
        with enter_username_password("bob", "secret2"):
            client = from_context(context)
            assert "authenticated as 'bob'" in repr(client.context)
        client.logout()

        # Bob's password should not work for Alice.
        with pytest.raises(PasswordRejected):
            with enter_username_password("alice", "secret2"):
                from_context(context)

        # Empty password should not work.
        with pytest.raises(PasswordRejected):
            with enter_username_password("alice", ""):
                from_context(context)


def test_remember_me(enter_username_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as Alice.
        with enter_username_password("alice", "secret1"):
            from_context(context)  # default: remember_me=True
    with Context.from_app(build_app_from_config(config)) as context:
        from_context(context)
        # Cached tokens are used, with no prompt.
        assert "authenticated as 'alice'" in repr(context)

    with Context.from_app(build_app_from_config(config)) as context:
        # Log in again, but set remember_me=False to opt out of cache.
        with enter_username_password("alice", "secret1"):
            from_context(context, remember_me=False)
        assert "authenticated as 'alice'" in repr(context)
    with Context.from_app(build_app_from_config(config)) as context:
        # No tokens are cached.
        assert not context.use_cached_tokens()


def test_logout(enter_username_password, config, tmpdir):
    """
    Logging out revokes the session, such that it cannot be refreshed.
    """
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as Alice.
        with enter_username_password("alice", "secret1"):
            from_context(context)
        # Reuse token from cache.
        client = from_context(context)
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
        client = from_context(context)
        # The refresh token refers to a revoked session, so refreshing the
        # session to generate a *new* access and refresh token will fail.
        with pytest.raises(CannotRefreshAuthentication):
            client.context.force_auth_refresh()


def test_key_rotation(enter_username_password, config):
    """
    Rotate in a new secret used to sign keys.
    Confirm that clients experience a smooth transition.
    """

    with Context.from_app(build_app_from_config(config)) as context:
        # Obtain refresh token.
        with enter_username_password("alice", "secret1"):
            from_context(context)
        # Use refresh token (no prompt to reauthenticate).
        client = from_context(context)

    # Rotate in a new key.
    config["authentication"]["secret_keys"].insert(0, "NEW_SECRET")
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET", "SECRET"]

    with Context.from_app(build_app_from_config(config)) as context:
        # The refresh token from the old key is still valid. No login prompt here.
        client = from_context(context)
        # We reauthenticate and receive a refresh token for the new key.
        # (This would happen on its own with the passage of time, but we force it
        # for the sake of a quick test.)
        client.context.force_auth_refresh()

    # Rotate out the old key.
    del config["authentication"]["secret_keys"][1]
    assert config["authentication"]["secret_keys"] == ["NEW_SECRET"]

    with Context.from_app(build_app_from_config(config)) as context:
        # New refresh token works with the new key
        from_context(context)


def test_refresh_forced(enter_username_password, config):
    "Forcing refresh obtains new token."
    with Context.from_app(build_app_from_config(config)) as context:
        # Normal default configuration: a refresh is not immediately required.
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
        tokens1 = dict(client.context.tokens)
        # Wait for a moment or we will get a new token that happens to be identical
        # to the old token. This advances the expiration time to make a distinct token.
        time.sleep(2)
        # Forcing a refresh gives us a new token.
        client.context.force_auth_refresh()
        tokens2 = dict(client.context.tokens)
        assert tokens1 != tokens2


def test_refresh_transparent(enter_username_password, config):
    "When access token expired, refresh happens transparently."
    # Pathological configuration: a refresh is almost immediately required
    config["authentication"]["access_token_max_age"] = 1
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
        tokens1 = dict(client.context.tokens)
        time.sleep(2)
        # A refresh should happen automatically now.
        client["A1"]
        tokens2 = dict(client.context.tokens)
        assert tokens2 != tokens1


def test_expired_session(enter_username_password, config):
    # Pathological configuration: sessions do not last
    config["authentication"]["session_max_age"] = 1
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
        time.sleep(2)
        # Refresh should fail because the session is too old.
        with pytest.raises(CannotRefreshAuthentication):
            client.context.force_auth_refresh()


def test_revoke_session(enter_username_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
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


def test_multiple_providers(enter_username_password, config, monkeypatch):
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
        with enter_username_password("alice", "secret1"):
            from_context(context)
        monkeypatch.setattr("sys.stdin", io.StringIO("2\n"))
        with enter_username_password("cara", "secret3"):
            from_context(context)
        monkeypatch.setattr("sys.stdin", io.StringIO("3\n"))
        with enter_username_password("cara", "secret5"):
            from_context(context)


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


def test_admin(enter_username_password, config):
    """
    Test that the 'tiled_admin' config confers the 'admin' Role on a Principal.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            context.authenticate()
        admin_roles = context.whoami()["roles"]
        assert "admin" in [role["name"] for role in admin_roles]

        # Exercise admin functions.
        principals = context.admin.list_principals()
        some_principal_uuid = principals[0]["uuid"]
        context.admin.show_principal(some_principal_uuid)

        with enter_username_password("bob", "secret2"):
            context.authenticate()
        user_roles = context.whoami()["roles"]
        assert [role["name"] for role in user_roles] == ["user"]

        # As bob, admin functions should be disallowed.
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            context.admin.list_principals()
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            context.admin.show_principal(some_principal_uuid)

    # Start the server a second time. Now alice is already an admin.
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            context.authenticate()
        admin_roles = context.whoami()["roles"]
        assert "admin" in [role["name"] for role in admin_roles]


def test_api_key_activity(enter_username_password, config):
    """
    Create and use an API. Verify that latest_activity updates.
    """
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as user.
        with enter_username_password("alice", "secret1"):
            context.authenticate()
        # Make and use an API key. Check that latest_activity is not set.
        key_info = context.create_api_key()
        context.logout()
        assert key_info["latest_activity"] is None  # never used
        context.api_key = key_info["secret"]
        assert "authenticated as 'alice'" in repr(context)
        assert "with API key" in repr(context)
        # Check authenticated property exists
        assert "authenticated" in dir(context)
        # Check authenticated property is True
        assert context.authenticated
        assert key_info["secret"][:8] in repr(context)
        assert key_info["secret"][8:] not in repr(context)

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
        # Check authenticated property still exists
        assert "authenticated" in dir(context)
        # Check authenticated property is False
        assert not context.authenticated
        # Set the API key.
        context.api_key = secret
        # Now this works again.
        context.which_api_key()


def test_api_key_scopes(enter_username_password, config):
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]
    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as admin.
        with enter_username_password("alice", "secret1"):
            context.authenticate()
        # Request a key with reduced scope that cannot read metadata.
        metrics_key_info = context.create_api_key(scopes=["metrics"])
        context.logout()
        context.api_key = metrics_key_info["secret"]
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            from_context(context)
        context.api_key = None

        # Log in as ordinary user.
        with enter_username_password("bob", "secret2"):
            context.authenticate()
        # Try to request a key with more scopes that the user has.
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            context.create_api_key(scopes=["admin:apikeys"])
        # Request a key with reduced scope that can *only* read metadata.
        metadata_key_info = context.create_api_key(scopes=["read:metadata"])
        context.logout()
        context.api_key = metadata_key_info["secret"]
        restricted_client = from_context(context)
        restricted_client["A1"]
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            restricted_client["A1"].read()  # no 'read:data' scope
        context.api_key = None


def test_api_key_revoked(enter_username_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            context.authenticate()

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
        with enter_username_password("alice", "secret1"):
            context.authenticate()
        context.revoke_api_key(key_info["first_eight"])
        assert len(context.whoami()["api_keys"]) == 0
        context.logout()
        context.api_key = key_info["secret"]
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            from_context(context)


def test_api_key_expiration(enter_username_password, config):
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            context.authenticate()
        # Create a key with a very short lifetime.
        key_info = context.create_api_key(
            note="will expire very soon", expires_in=1
        )  # units: seconds
        context.logout()
        context.api_key = key_info["secret"]
        time.sleep(2)
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            from_context(context)


def test_api_key_limit(enter_username_password, config):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.API_KEY_LIMIT
    authentication.API_KEY_LIMIT = 3
    try:
        with Context.from_app(build_app_from_config(config)) as context:
            with enter_username_password("bob", "secret2"):
                context.authenticate()
            for i in range(authentication.API_KEY_LIMIT):
                context.create_api_key(note=f"key {i}")
            # Hit API key limit.
            with fail_with_status_code(HTTP_400_BAD_REQUEST):
                context.create_api_key(note="one key too many")
    finally:
        authentication.API_KEY_LIMIT = original_limit


def test_session_limit(enter_username_password, config):
    # Decrease the limit so this test runs faster.
    original_limit = authentication.SESSION_LIMIT
    authentication.SESSION_LIMIT = 3
    # Use separate token caches to de-couple login attempts into separate sessions.
    try:
        with Context.from_app(build_app_from_config(config)) as context:
            with enter_username_password("alice", "secret1"):
                for i in range(authentication.SESSION_LIMIT):
                    context.authenticate()
                    context.logout()
                # Hit Session limit.
                with fail_with_status_code(HTTP_400_BAD_REQUEST):
                    context.authenticate()
    finally:
        authentication.SESSION_LIMIT = original_limit


@pytest.fixture
def principals_context(enter_username_password, config):
    """
    Fetch UUID for an admin and an ordinary user; include the client context.
    """
    # Make alice an admin. Leave bob as a user.
    config["authentication"]["tiled_admins"] = [{"provider": "toy", "id": "alice"}]

    with Context.from_app(build_app_from_config(config)) as context:
        # Log in as Alice and retrieve admin UUID for later use
        with enter_username_password("alice", "secret1"):
            context.authenticate()

        principal = context.whoami()
        assert "admin" in (role["name"] for role in principal["roles"])
        admin_uuid = principal["uuid"]
        context.logout()

        # Log in as Bob and retrieve Bob's UUID for later use
        with enter_username_password("bob", "secret2"):
            context.authenticate()

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
    enter_username_password, principals_context, username, scopes, resource
):
    """
    Admin can create usable API keys for any prinicipal, within that principal's scopes.
    """
    with principals_context["context"] as context:
        # Log in as Alice, create and use API key after logout
        with enter_username_password("alice", "secret1"):
            context.authenticate()

        principal_uuid = principals_context["uuid"][username]
        api_key_info = context.admin.create_api_key(principal_uuid, scopes=scopes)
        api_key = api_key_info["secret"]
        assert api_key
        context.logout()

        context.api_key = api_key
        context.http_client.get(resource).raise_for_status()
        context.api_key = None
        # The same endpoint fails without an API key
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            context.http_client.get(resource).raise_for_status()


def test_admin_create_service_principal(enter_username_password, principals_context):
    """
    Admin can create service accounts with API keys.
    """
    with principals_context["context"] as context:
        # Log in as Alice, create and use API key after logout
        with enter_username_password("alice", "secret1"):
            context.authenticate()

        assert context.whoami()["type"] == "user"

        principal_info = context.admin.create_service_principal(role="user")
        principal_uuid = principal_info["uuid"]

        service_api_key_info = context.admin.create_api_key(principal_uuid)
        context.logout()

        context.api_key = service_api_key_info["secret"]
        assert context.whoami()["type"] == "service"

        # Test service repr
        assert f"authenticated as service '{principal_uuid}'" in repr(context)


def test_admin_api_key_any_principal_exceeds_scopes(
    enter_username_password, principals_context
):
    """
    Admin cannot create API key that exceeds scopes for another principal.
    """
    with principals_context["context"] as context:
        # Log in as Alice, create and use API key after logout
        with enter_username_password("alice", "secret1"):
            context.authenticate()

        principal_uuid = principals_context["uuid"]["bob"]
        with fail_with_status_code(HTTP_403_FORBIDDEN) as fail_info:
            context.admin.create_api_key(principal_uuid, scopes=["read:principals"])
        fail_message = " must be a subset of the principal's scopes "
        assert fail_message in fail_info.value.response.text
        context.logout()


@pytest.mark.parametrize("username", ("alice", "bob"))
def test_api_key_any_principal(enter_username_password, principals_context, username):
    """
    Ordinary user cannot create API key for another principal.
    """
    with principals_context["context"] as context:
        # Log in as Bob, this API endpoint is unauthorized
        with enter_username_password("bob", "secret2"):
            context.authenticate()

        principal_uuid = principals_context["uuid"][username]
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            context.admin.create_api_key(principal_uuid, scopes=["read:metadata"])


def test_api_key_bypass_scopes(enter_username_password, principals_context):
    """
    Ordinary user cannot create API key that bypasses a scopes restriction.
    """
    with principals_context["context"] as context:
        # Log in as Bob, create API key with empty scopes
        with enter_username_password("bob", "secret2"):
            context.authenticate()

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
                with fail_with_status_code(HTTP_401_UNAUTHORIZED):
                    context.http_client.get(
                        resource, params=query_params
                    ).raise_for_status()


def test_admin_delete_principal_apikey(
    enter_username_password,
    principals_context,
):
    """
    Admin can delete API keys for any prinicipal, revoking access.
    """
    with principals_context["context"] as context:
        # Log in as Bob (Ordinary user)
        with enter_username_password("bob", "secret2"):
            context.authenticate()

        # Create an ordinary user API Key
        principal_uuid = principals_context["uuid"]["bob"]
        api_key_info = context.create_api_key(scopes=["read:data"])
        context.logout()

        # Log in as Alice (Admin)
        with enter_username_password("alice", "secret1"):
            context.authenticate()

        # Delete the created API Key via service principal
        context.admin.revoke_api_key(principal_uuid, api_key_info["first_eight"])
        context.logout()

        # Try to use the revoked API Key
        context.api_key = api_key_info["secret"]
        with fail_with_status_code(HTTP_401_UNAUTHORIZED):
            context.whoami()
