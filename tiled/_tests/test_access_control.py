import json

import numpy
import pytest
from starlette.status import HTTP_403_FORBIDDEN

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..server.app import build_app_from_config
from .utils import enter_username_password, fail_with_status_code

arr = numpy.ones((5, 5))
arr_ad = ArrayAdapter.from_array(arr)


def tree_a(access_policy=None):
    return MapAdapter({"A1": arr_ad, "A2": arr_ad}, access_policy=access_policy)


def tree_b(access_policy=None):
    return MapAdapter({"B1": arr_ad, "B2": arr_ad}, access_policy=access_policy)


@pytest.fixture(scope="module")
def context(tmpdir_module):
    config = {
        "authentication": {
            "allow_anonymous_access": True,
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {
                            "alice": "secret1",
                            "bob": "secret2",
                            "admin": "admin",
                        }
                    },
                }
            ],
        },
        "database": {
            "uri": "sqlite+aiosqlite://",  # in-memory
        },
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "access_lists": {"alice": ["a", "c", "d", "e"]},
                "provider": "toy",
                "admins": ["admin"],
                "public": ["f"],
            },
        },
        "trees": [
            {
                "tree": f"{__name__}:tree_a",
                "path": "/a",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {
                            "alice": ["A2"],
                            # This should have no effect because bob
                            # cannot access the parent node.
                            "bob": ["A1", "A2"],
                        },
                        "admins": ["admin"],
                    },
                },
            },
            {"tree": f"{__name__}:tree_b", "path": "/b", "access_policy": None},
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": tmpdir_module / "c"},
                "path": "/c",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {
                            "alice": "tiled.access_policies:ALL_ACCESS",
                        },
                        "admins": ["admin"],
                    },
                },
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": tmpdir_module / "d"},
                "path": "/d",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {
                            "alice": "tiled.access_policies:ALL_ACCESS",
                        },
                        "admins": ["admin"],
                        # Block writing.
                        "scopes": ["read:metadata", "read:data"],
                    },
                },
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": tmpdir_module / "e"},
                "path": "/e",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {
                            "alice": "tiled.access_policies:ALL_ACCESS",
                        },
                        "admins": ["admin"],
                        # Block creation.
                        "scopes": [
                            "read:metadata",
                            "read:data",
                            "write:metadata",
                            "write:data",
                        ],
                    },
                },
            },
            {"tree": ArrayAdapter.from_array(arr), "path": "/f"},
        ],
    }
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        with enter_username_password("admin", "admin"):
            admin_client = from_context(context, username="admin")
            for k in ["c", "d", "e"]:
                admin_client[k].write_array(arr, key="A1")
                admin_client[k].write_array(arr, key="A2")
                admin_client[k].write_array(arr, key="x")
        yield context


def test_top_level_access_control(context, enter_username_password):
    with enter_username_password("alice", "secret1"):
        alice_client = from_context(context, username="alice")
    assert "a" in alice_client
    assert "A2" in alice_client["a"]
    assert "A1" not in alice_client["a"]
    assert "b" not in alice_client
    alice_client["a"]["A2"]
    with pytest.raises(KeyError):
        alice_client["b"]

    with enter_username_password("bob", "secret2"):
        bob_client = from_context(context, username="bob")
    assert not list(bob_client)
    with pytest.raises(KeyError):
        bob_client["a"]
    with pytest.raises(KeyError):
        bob_client["b"]
    alice_client.logout()

    # Make sure clearing default identity works without raising an error.
    bob_client.logout(clear_default=True)


def test_access_control_with_api_key_auth(context, enter_username_password):
    # Log in, create an API key, log out.
    with enter_username_password("alice", "secret1"):
        context.authenticate(username="alice")
    key_info = context.create_api_key()
    context.logout()

    try:
        # Use API key auth while exercising the access control code.
        context.api_key = key_info["secret"]
        client = from_context(context)
        client["a"]["A2"]
    finally:
        # Clean up Context, which is a module-scopae fixture shared with other tests.
        context.api_key = None


def test_node_export(enter_username_password, context, buffer):
    "Exporting a node should include only the children we can see."
    with enter_username_password("alice", "secret1"):
        alice_client = from_context(context, username="alice")
    alice_client.export(buffer, format="application/json")
    alice_client.logout()
    buffer.seek(0)
    exported_dict = json.loads(buffer.read())
    assert "a" in exported_dict["contents"]
    assert "A2" in exported_dict["contents"]["a"]["contents"]
    assert "A1" not in exported_dict["contents"]["a"]["contents"]
    assert "b" not in exported_dict
    exported_dict["contents"]["a"]["contents"]["A2"]


def test_create_and_update_allowed(enter_username_password, context):
    with enter_username_password("alice", "secret1"):
        alice_client = from_context(context, username="alice")

    # Update
    alice_client["c"]["x"].metadata
    alice_client["c"]["x"].update_metadata(metadata={"added_key": 3})
    assert alice_client["c"]["x"].metadata["added_key"] == 3

    # Create
    alice_client["c"].write_array([1, 2, 3])
    alice_client.logout()


def test_writing_blocked_by_access_policy(enter_username_password, context):
    with enter_username_password("alice", "secret1"):
        alice_client = from_context(context, username="alice")
    alice_client["d"]["x"].metadata
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client["d"]["x"].update_metadata(metadata={"added_key": 3})
    alice_client.logout()


def test_create_blocked_by_access_policy(enter_username_password, context):
    with enter_username_password("alice", "secret1"):
        alice_client = from_context(context, username="alice")
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client["e"].write_array([1, 2, 3])
    alice_client.logout()


def test_public_access(context):
    public_client = from_context(context)
    for key in ["a", "b", "c", "d", "e"]:
        assert key not in public_client
    public_client["f"].read()
    with pytest.raises(KeyError):
        public_client["a", "A1"]


def test_service_principal_access(tmpdir):
    "Test that a service principal can work with SimpleAccessPolicy."
    config = {
        "authentication": {
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {
                            "admin": "admin",
                        }
                    },
                }
            ],
            "tiled_admins": [{"id": "admin", "provider": "toy"}],
        },
        "database": {
            "uri": f"sqlite+aiosqlite:///{tmpdir}/auth.db",
            "init_if_not_exists": True,
        },
        "trees": [
            {
                "tree": "catalog",
                "args": {
                    "uri": f"sqlite+aiosqlite:///{tmpdir}/catalog.db",
                    "writable_storage": f"file://localhost{tmpdir}/data",
                    "init_if_not_exists": True,
                },
                "path": "/",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "access_lists": {},
                        "provider": "toy",
                        "admins": ["admin"],
                    },
                },
            }
        ],
    }
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("admin", "admin"):
            admin_client = from_context(context, username="admin")
        sp = admin_client.context.admin.create_service_principal("user")
        key_info = admin_client.context.admin.create_api_key(sp["uuid"])
        admin_client.write_array([1, 2, 3], key="x")
        admin_client.write_array([4, 5, 6], key="y")
        admin_client.logout()

    # Drop the admin, no longer needed.
    config["authentication"].pop("tiled_admins")
    # Add the service principal to the access_lists.
    config["trees"][0]["access_control"]["args"]["access_lists"][sp["uuid"]] = ["x"]
    with Context.from_app(
        build_app_from_config(config), api_key=key_info["secret"]
    ) as context:
        sp_client = from_context(context)
        list(sp_client) == ["x"]
