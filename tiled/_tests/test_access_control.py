import json

import numpy
import pytest
from fastapi import HTTPException
from starlette.status import HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND

from ..access_policies import (
    ALL_SCOPES,
    PUBLIC_SCOPES,
    SimpleAccessPolicy,
    SpecialUsers,
)
from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.utils import ClientError
from ..server.app import build_app_from_config
from ..server.core import NoEntry
from .utils import enter_username_password, fail_with_status_code

arr = numpy.ones((5, 5))
arr_zeros = numpy.zeros((5, 5))
arr_ad = ArrayAdapter.from_array(arr)


class EntryBasedAccessPolicy(SimpleAccessPolicy):
    """
    This example access policy demonstrates how the metadata on some nested child node
    can be efficiently consulted and incorporated in logic that determines access scopes.
    In this test example, the metadata on the node quite literally lists some scopes that
    it should not allow. In realistic examples it could be incorporated in site-specific logic.
    """

    async def allowed_scopes(self, node, principal, path_parts):
        # If this is being called, filter_access has let us get this far.
        if principal is SpecialUsers.public:
            allowed = PUBLIC_SCOPES
        elif principal.type == "service":
            allowed = self.scopes
        else:
            allowed = self.scopes

        if self._get_id(principal) in self.admins:
            allowed = ALL_SCOPES
        else:
            # Allowed scopes will be filtered based on some metadata of the target entry
            try:
                for i, segment in enumerate(path_parts):
                    if hasattr(node, "lookup_adapter"):
                        node = await node.lookup_adapter(path_parts[i:])
                        if node is None:
                            raise NoEntry(path_parts)
                        break
                    else:
                        try:
                            node = node[segment]
                        except (KeyError, TypeError):
                            raise NoEntry(path_parts)
            except NoEntry:
                raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND,
                    detail=f"No such entry: {path_parts}",
                )
            remove_scope = node.metadata().get("remove_scope", None)
            if remove_scope in allowed:
                allowed = allowed.copy()
                allowed.remove(remove_scope)
        return allowed


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
            "uri": "sqlite://",  # in-memory
        },
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "access_lists": {"alice": ["a", "c", "d", "e", "g", "h"]},
                "provider": "toy",
                "admins": ["admin"],
                "public": ["f", "g"],
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
                "args": {"writable_storage": str(tmpdir_module / "c")},
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
                "args": {"writable_storage": str(tmpdir_module / "d")},
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
                "args": {"writable_storage": str(tmpdir_module / "e")},
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
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": str(tmpdir_module / "g")},
                "path": "/g",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "key": "project",
                        "access_lists": {"alice": ["projectA"], "bob": ["projectB"]},
                        "admins": ["admin"],
                        "public": ["projectC"],
                    },
                },
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": str(tmpdir_module / "h")},
                "path": "/h",
                "access_control": {
                    "access_policy": "tiled._tests.test_access_control:EntryBasedAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {"alice": ["x", "y"]},
                        "admins": ["admin"],
                    },
                },
            },
        ],
    }
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        admin_client = from_context(context)
        with enter_username_password("admin", "admin"):
            admin_client.login()
            for k in ["c", "d", "e"]:
                admin_client[k].write_array(arr, key="A1")
                admin_client[k].write_array(arr, key="A2")
                admin_client[k].write_array(arr, key="x")
            for k, v in {"A3": "projectA", "A4": "projectB", "r": "projectC"}.items():
                admin_client["g"].write_array(arr, key=k, metadata={"project": v})
            for k, v in {"x": "write:data", "y": None}.items():
                admin_client["h"].write_array(arr, key=k, metadata={"remove_scope": v})
        yield context


def test_entry_based_scopes(context, enter_username_password):
    alice_client = from_context(context)
    with enter_username_password("alice", "secret1"):
        alice_client.login()
    with pytest.raises(ClientError, match="Not enough permissions"):
        alice_client["h"]["x"].write(arr_zeros)
    alice_client["h"]["y"].write(arr_zeros)


def test_top_level_access_control(context, enter_username_password):
    alice_client = from_context(context)
    with enter_username_password("alice", "secret1"):
        alice_client.login()
    assert "a" in alice_client
    assert "A2" in alice_client["a"]
    assert "A1" not in alice_client["a"]
    assert "b" not in alice_client
    assert "g" in alice_client
    assert "A3" in alice_client["g"]
    assert "A4" not in alice_client["g"]
    alice_client["a"]["A2"]
    alice_client["g"]["A3"]
    with pytest.raises(KeyError):
        alice_client["b"]
    with pytest.raises(KeyError):
        alice_client["g"]["A4"]
    alice_client.logout()

    bob_client = from_context(context)
    with enter_username_password("bob", "secret2"):
        bob_client.login()
    assert not list(bob_client)
    with pytest.raises(KeyError):
        bob_client["a"]
    with pytest.raises(KeyError):
        bob_client["b"]
    with pytest.raises(KeyError):
        bob_client["g"]["A3"]
    bob_client.logout()


def test_access_control_with_api_key_auth(context, enter_username_password):
    # Log in, create an API key, log out.
    with enter_username_password("alice", "secret1"):
        context.authenticate()
    key_info = context.create_api_key()
    context.logout()

    try:
        # Use API key auth while exercising the access control code.
        context.api_key = key_info["secret"]
        client = from_context(context)
        client["a"]["A2"]
        client["g"]["A3"]
    finally:
        # Clean up Context, which is a module-scopae fixture shared with other tests.
        context.api_key = None


def test_node_export(enter_username_password, context, buffer):
    "Exporting a node should include only the children we can see."
    alice_client = from_context(context)
    with enter_username_password("alice", "secret1"):
        alice_client.login()
    alice_client.export(buffer, format="application/json")
    alice_client.logout()
    buffer.seek(0)
    exported_dict = json.loads(buffer.read())
    assert "a" in exported_dict["contents"]
    assert "A2" in exported_dict["contents"]["a"]["contents"]
    assert "A1" not in exported_dict["contents"]["a"]["contents"]
    assert "b" not in exported_dict
    assert "g" in exported_dict["contents"]
    assert "A3" in exported_dict["contents"]["g"]["contents"]
    assert "A4" not in exported_dict["contents"]["g"]["contents"]
    exported_dict["contents"]["a"]["contents"]["A2"]
    exported_dict["contents"]["g"]["contents"]["A3"]


def test_create_and_update_allowed(enter_username_password, context):
    alice_client = from_context(context)
    with enter_username_password("alice", "secret1"):
        alice_client.login()

    # Update
    alice_client["c"]["x"].metadata
    alice_client["c"]["x"].update_metadata(metadata={"added_key": 3})
    assert alice_client["c"]["x"].metadata["added_key"] == 3

    alice_client["g"]["A3"].metadata
    alice_client["g"]["A3"].update_metadata(metadata={"added_key": 9})
    assert alice_client["g"]["A3"].metadata["added_key"] == 9

    # Create
    alice_client["c"].write_array([1, 2, 3])
    alice_client["g"].write_array([4, 5, 6], metadata={"project": "projectA"})
    alice_client.logout()


def test_writing_blocked_by_access_policy(enter_username_password, context):
    alice_client = from_context(context)
    with enter_username_password("alice", "secret1"):
        alice_client.login()
    alice_client["d"]["x"].metadata
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client["d"]["x"].update_metadata(metadata={"added_key": 3})
    alice_client.logout()


def test_create_blocked_by_access_policy(enter_username_password, context):
    alice_client = from_context(context)
    with enter_username_password("alice", "secret1"):
        alice_client.login()
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client["e"].write_array([1, 2, 3])
    alice_client.logout()


def test_public_access(context):
    public_client = from_context(context)
    for key in ["a", "b", "c", "d", "e"]:
        assert key not in public_client
    public_client["f"].read()
    public_client["g"]["r"].read()
    with pytest.raises(KeyError):
        public_client["a", "A1"]
    with pytest.raises(KeyError):
        public_client["g", "A3"]


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
            "uri": f"sqlite:///{tmpdir}/auth.db",
            "init_if_not_exists": True,
        },
        "trees": [
            {
                "tree": "catalog",
                "args": {
                    "uri": f"sqlite:///{tmpdir}/catalog.db",
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
            # Prompts for login here because anonymous access is not allowed
            admin_client = from_context(context)
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
