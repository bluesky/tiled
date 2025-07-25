import json

import numpy
import pytest
from starlette.status import HTTP_403_FORBIDDEN

from tiled.authenticators import DictionaryAuthenticator
from tiled.server.protocols import UserSessionState

from ..access_policies import NO_ACCESS
from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..client.utils import ClientError
from ..scopes import ALL_SCOPES, NO_SCOPES, USER_SCOPES
from ..server.app import build_app_from_config
from .utils import enter_username_password, fail_with_status_code

arr = numpy.ones((5, 5))
arr_ad = ArrayAdapter.from_array(arr)

server_common_config = {
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
                    },
                },
            },
        ],
    },
    "database": {
        "uri": "sqlite://",  # in-memory
    },
}


def tree_a(access_policy=None):
    return MapAdapter({"A1": arr_ad, "A2": arr_ad})


def tree_b(access_policy=None):
    return MapAdapter({"B1": arr_ad, "B2": arr_ad})


@pytest.fixture(scope="module")
def context_a(tmpdir_module):
    config = {
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "provider": "toy",
                "access_lists": {
                    "alice": ["a", "A2"],
                    # This should have no effect because bob
                    # cannot access the parent node.
                    "bob": ["A1", "A2"],
                },
                "admins": ["admin"],
            },
        },
        "trees": [
            {
                "tree": f"{__name__}:tree_a",
                "path": "/a",
            },
        ],
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture(scope="module")
def context_b(tmpdir_module):
    config = {
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "provider": "toy",
                "access_lists": {
                    "alice": [],
                    "bob": [],
                },
                "admins": ["admin"],
            },
        },
        "trees": [
            {
                "tree": f"{__name__}:tree_b",
                "path": "/b",
            },
        ],
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture(scope="module")
def context_c(tmpdir_module):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": str(tmpdir_module / "c")},
                "path": "/c",
            },
        ],
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
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        admin_client = from_context(context)
        with enter_username_password("admin", "admin"):
            admin_client.login()
            for k in ["c"]:
                admin_client[k].write_array(arr, key="A1")
                admin_client[k].write_array(arr, key="A2")
                admin_client[k].write_array(arr, key="x")
        yield context


@pytest.fixture(scope="module")
def context_d(tmpdir_module):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": str(tmpdir_module / "d")},
                "path": "/d",
            },
        ],
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
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        admin_client = from_context(context)
        with enter_username_password("admin", "admin"):
            admin_client.login()
            for k in ["d"]:
                admin_client[k].write_array(arr, key="A1")
                admin_client[k].write_array(arr, key="A2")
                admin_client[k].write_array(arr, key="x")
        yield context


@pytest.fixture(scope="module")
def context_e(tmpdir_module):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {"writable_storage": str(tmpdir_module / "e")},
                "path": "/e",
            },
        ],
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
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        admin_client = from_context(context)
        with enter_username_password("admin", "admin"):
            admin_client.login()
            for k in ["e"]:
                admin_client[k].write_array(arr, key="A1")
                admin_client[k].write_array(arr, key="A2")
                admin_client[k].write_array(arr, key="x")
        yield context


@pytest.fixture(scope="module")
def context_f(tmpdir_module):
    config = {
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "provider": "toy",
                "access_lists": {},
                "admins": ["admin"],
                "public": ["f"],
            },
        },
        "trees": [
            {
                "tree": ArrayAdapter.from_array(arr),
                "path": "/f",
            },
        ],
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        yield context


@pytest.fixture(scope="module")
def context_g(tmpdir_module):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "writable_storage": str(tmpdir_module / "g"),
                    "metadata": {"project": "all_projects"},
                },
                "path": "/g",
            },
        ],
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "provider": "toy",
                "key": "project",
                "access_lists": {
                    "alice": ["all_projects", "projectA"],
                    "bob": ["projectB"],
                },
                "admins": ["admin"],
                "public": ["projectC", "all_projects"],
            },
        },
    }

    config.update(server_common_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        admin_client = from_context(context)
        with enter_username_password("admin", "admin"):
            admin_client.login()
            for k, v in {"A3": "projectA", "A4": "projectB", "r": "projectC"}.items():
                admin_client["g"].write_array(arr, key=k, metadata={"project": v})
        yield context


def test_basic_access_control(context_a, context_b, context_g, enter_username_password):
    alice_client_a = from_context(context_a)
    alice_client_b = from_context(context_b)
    alice_client_g = from_context(context_g)
    with enter_username_password("alice", "secret1"):
        alice_client_a.login()
        alice_client_b.login()
        alice_client_g.login()
    assert "a" in alice_client_a
    assert "A2" in alice_client_a["a"]
    assert "A1" not in alice_client_a["a"]
    assert "b" not in alice_client_b
    assert "g" in alice_client_g
    assert "A3" in alice_client_g["g"]
    assert "A4" not in alice_client_g["g"]
    alice_client_a["a"]["A2"]
    alice_client_g["g"]["A3"]
    with pytest.raises(KeyError):
        alice_client_b["b"]
    with pytest.raises(KeyError):
        alice_client_g["g"]["A4"]
    alice_client_a.logout()
    alice_client_b.logout()
    alice_client_g.logout()

    bob_client_a = from_context(context_a)
    bob_client_b = from_context(context_b)
    bob_client_g = from_context(context_g)
    with enter_username_password("bob", "secret2"):
        bob_client_a.login()
        bob_client_b.login()
        bob_client_g.login()
    assert not list(bob_client_a)
    assert not list(bob_client_b)
    assert not list(bob_client_g)
    with pytest.raises(KeyError):
        bob_client_a["a"]
    with pytest.raises(KeyError):
        bob_client_b["b"]
    with pytest.raises(KeyError):
        bob_client_g["g"]["A3"]
    bob_client_a.logout()
    bob_client_b.logout()
    bob_client_g.logout()


def test_access_control_with_api_key_auth(
    context_a, context_g, enter_username_password
):
    # Log in, create an API key, log out.
    with enter_username_password("alice", "secret1"):
        context_a.authenticate()
        context_g.authenticate()
    key_info_a = context_a.create_api_key()
    context_a.logout()
    key_info_g = context_g.create_api_key()
    context_g.logout()

    try:
        # Use API key auth while exercising the access control code.
        context_a.api_key = key_info_a["secret"]
        client_a = from_context(context_a)
        context_g.api_key = key_info_g["secret"]
        client_g = from_context(context_g)
        client_a["a"]["A2"]
        client_g["g"]["A3"]
    finally:
        # Clean up Context, which is a module-scope fixture shared with other tests.
        context_a.api_key = None
        context_g.api_key = None


def test_node_export(
    enter_username_password, context_a, context_b, context_g, buffer_factory
):
    "Exporting a node should include only the children we can see."
    alice_client_a = from_context(context_a)
    alice_client_b = from_context(context_b)
    alice_client_g = from_context(context_g)
    with enter_username_password("alice", "secret1"):
        alice_client_a.login()
        alice_client_b.login()
        alice_client_g.login()
    buffer_a = buffer_factory()
    buffer_b = buffer_factory()
    buffer_g = buffer_factory()
    alice_client_a.export(buffer_a, format="application/json")
    alice_client_b.export(buffer_b, format="application/json")
    alice_client_g.export(buffer_g, format="application/json")
    alice_client_a.logout()
    alice_client_b.logout()
    alice_client_g.logout()
    buffer_a.seek(0)
    buffer_b.seek(0)
    buffer_g.seek(0)
    exported_dict_a = json.loads(buffer_a.read())
    exported_dict_b = json.loads(buffer_b.read())
    exported_dict_g = json.loads(buffer_g.read())
    assert "a" in exported_dict_a["contents"]
    assert "A2" in exported_dict_a["contents"]["a"]["contents"]
    assert "A1" not in exported_dict_a["contents"]["a"]["contents"]
    assert "b" not in exported_dict_b
    assert "g" in exported_dict_g["contents"]
    assert "A3" in exported_dict_g["contents"]["g"]["contents"]
    assert "A4" not in exported_dict_g["contents"]["g"]["contents"]
    exported_dict_a["contents"]["a"]["contents"]["A2"]
    exported_dict_g["contents"]["g"]["contents"]["A3"]


def test_create_and_update_allowed(enter_username_password, context_c, context_g):
    alice_client_c = from_context(context_c)
    alice_client_g = from_context(context_g)
    with enter_username_password("alice", "secret1"):
        alice_client_c.login()
        alice_client_g.login()

    # Update
    alice_client_c["c"]["x"].metadata
    alice_client_c["c"]["x"].update_metadata(metadata={"added_key": 3})
    assert alice_client_c["c"]["x"].metadata["added_key"] == 3

    alice_client_g["g"]["A3"].metadata
    alice_client_g["g"]["A3"].update_metadata(metadata={"added_key": 9})
    assert alice_client_g["g"]["A3"].metadata["added_key"] == 9

    # Create
    alice_client_c["c"].write_array([1, 2, 3])
    alice_client_g["g"].write_array([4, 5, 6], metadata={"project": "projectA"})
    alice_client_c.logout()
    alice_client_g.logout()


def test_writing_blocked_by_access_policy(enter_username_password, context_d):
    alice_client_d = from_context(context_d)
    with enter_username_password("alice", "secret1"):
        alice_client_d.login()
    alice_client_d["d"]["x"].metadata
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client_d["d"]["x"].update_metadata(metadata={"added_key": 3})
    alice_client_d.logout()


def test_create_blocked_by_access_policy(enter_username_password, context_e):
    alice_client_e = from_context(context_e)
    with enter_username_password("alice", "secret1"):
        alice_client_e.login()
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client_e["e"].write_array([1, 2, 3])
    alice_client_e.logout()


def test_public_access(
    context_a, context_b, context_c, context_d, context_e, context_f, context_g
):
    public_client_a = from_context(context_a)
    public_client_b = from_context(context_b)
    public_client_c = from_context(context_c)
    public_client_d = from_context(context_d)
    public_client_e = from_context(context_e)
    public_client_f = from_context(context_f)
    public_client_g = from_context(context_g)
    for key, client in zip(
        ["a", "b", "c", "d", "e"],
        [
            public_client_a,
            public_client_b,
            public_client_c,
            public_client_d,
            public_client_e,
        ],
    ):
        assert key not in client
    public_client_f["f"].read()
    public_client_g["g"]["r"].read()
    with pytest.raises(KeyError):
        public_client_a["a", "A1"]
    with pytest.raises(KeyError):
        public_client_g["g", "A3"]


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
            }
        ],
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {
                "access_lists": {},
                "provider": "toy",
                "admins": ["admin"],
            },
        },
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
    config["access_control"]["args"]["access_lists"][sp["uuid"]] = ["x"]
    with Context.from_app(
        build_app_from_config(config), api_key=key_info["secret"]
    ) as context:
        sp_client = from_context(context)
        assert list(sp_client) == ["x"]


class CustomAttributesAuthenticator(DictionaryAuthenticator):
    """An example authenticator that enriches the stored user information."""

    def __init__(self, users: dict, confirmation_message: str = ""):
        self._users = users
        super().__init__(
            {username: user["password"] for username, user in users.items()},
            confirmation_message,
        )

    async def authenticate(self, username, password):
        state = await super().authenticate(username, password)
        if isinstance(state, UserSessionState):
            # enrich the auth state
            state.state["attributes"] = self._users[username].get("attributes", {})
        return state


class CustomAttributesAccessPolicy:
    """
    A policy that demonstrates comparing metadata against user information stored at login-time.
    """

    READ_METADATA = ["read:metadata"]

    def __init__(self):
        pass

    async def allowed_scopes(self, node, principal, authn_scopes):
        if hasattr(principal, "sessions"):
            if len(principal.sessions):
                auth_state = principal.sessions[-1].state or {}
                auth_attributes = auth_state.get("attributes", {})
                if auth_attributes:
                    if "admins" in auth_attributes.get("groups", []):
                        return ALL_SCOPES

                    if not node.metadata():
                        return self.READ_METADATA

                    if node.metadata()["beamline"] in auth_attributes.get(
                        "beamlines", []
                    ) or node.metadata()["proposal"] in auth_attributes.get(
                        "proposals", []
                    ):
                        return USER_SCOPES

            return self.READ_METADATA
        return NO_SCOPES

    async def filters(self, node, principal, authn_scopes, scopes):
        if not scopes.issubset(
            await self.allowed_scopes(node, principal, authn_scopes)
        ):
            return NO_ACCESS
        return []


def tree_enriched_metadata():
    return MapAdapter(
        {
            "A": ArrayAdapter.from_array(
                numpy.ones(10), metadata={"beamline": "bl1", "proposal": "prop1"}
            ),
            "B": ArrayAdapter.from_array(
                numpy.ones(10), metadata={"beamline": "bl1", "proposal": "prop2"}
            ),
            "C": ArrayAdapter.from_array(
                numpy.ones(10), metadata={"beamline": "bl2", "proposal": "prop2"}
            ),
            "D": ArrayAdapter.from_array(
                numpy.ones(10), metadata={"beamline": "bl2", "proposal": "prop3"}
            ),
        },
    )


@pytest.fixture(scope="module")
def custom_attributes_context():
    config = {
        "authentication": {
            "allow_anonymous_access": False,
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": f"{__name__}:CustomAttributesAuthenticator",
                    "args": {
                        "users": {
                            "alice": {
                                "password": "secret1",
                                "attributes": {"proposals": ["prop1"]},
                            },
                            "bob": {
                                "password": "secret2",
                                "attributes": {"beamlines": ["bl1"]},
                            },
                            "cara": {
                                "password": "secret3",
                                "attributes": {
                                    "beamlines": ["bl2"],
                                    "proposals": ["prop1"],
                                },
                            },
                            "john": {"password": "secret4", "attributes": {}},
                            "admin": {
                                "password": "admin",
                                "attributes": {"groups": ["admins"]},
                            },
                        }
                    },
                }
            ],
        },
        "database": {
            "uri": "sqlite://",  # in-memory
        },
        "access_control": {
            "access_policy": f"{__name__}:CustomAttributesAccessPolicy",
            "args": {},
        },
        "trees": [
            {"tree": f"{__name__}:tree_enriched_metadata", "path": "/"},
        ],
    }
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        yield context


@pytest.mark.parametrize(
    ("username", "password", "nodes"),
    [
        ("admin", "admin", ["A", "B", "C", "D"]),
        ("alice", "secret1", ["A"]),
        ("bob", "secret2", ["A", "B"]),
        ("cara", "secret3", ["A", "C", "D"]),
    ],
)
def test_custom_attributes_with_data_access(
    enter_username_password, custom_attributes_context, username, password, nodes
):
    """Test that the user has access to the data based on their auth attributes."""
    with enter_username_password(username, password):
        custom_attributes_context.authenticate()
    key_info = custom_attributes_context.create_api_key()
    custom_attributes_context.logout()

    try:
        custom_attributes_context.api_key = key_info["secret"]
        client = from_context(custom_attributes_context)

        for node in nodes:
            client[node].read()

    finally:
        custom_attributes_context.api_key = None


@pytest.mark.parametrize(
    ("username", "password", "nodes"),
    [
        ("alice", "secret1", ["B", "C", "D"]),
        ("bob", "secret2", ["C", "D"]),
        ("cara", "secret3", ["B"]),
        ("john", "secret4", ["A", "B", "C", "D"]),
    ],
)
def test_custom_attributes_without_data_access(
    enter_username_password, custom_attributes_context, username, password, nodes
):
    """Test that the user cannot access data due to missing auth attributes."""
    with enter_username_password(username, password):
        custom_attributes_context.authenticate()
    key_info = custom_attributes_context.create_api_key()
    custom_attributes_context.logout()

    try:
        custom_attributes_context.api_key = key_info["secret"]
        client = from_context(custom_attributes_context)

        for node in nodes:
            with pytest.raises(ClientError):
                client[node].read()

    finally:
        custom_attributes_context.api_key = None
