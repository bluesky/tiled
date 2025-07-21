import numpy
import pytest
from starlette.status import HTTP_403_FORBIDDEN

from ..access_control.access_tags import AccessTagsCompiler
from ..access_control.scopes import ALL_SCOPES
from ..client import Context, from_context
from ..server.app import build_app_from_config
from .utils import enter_username_password, fail_with_status_code

arr = numpy.ones((5, 5))


server_config = {
    "access_control": {
        "access_policy": "tiled.access_control.access_policies:TagBasedAccessPolicy",
        "args": {
            "provider": "toy",
            "tags_db": {
                "uri": "file:compiled_tags_mem?mode=memory&cache=shared"  # in-memory and shareable
            },
            "access_tags_parser": "tiled.access_control.access_tags:AccessTagsParser",
        },
    },
    "authentication": {
        "tiled_admins": [{"provider": "toy", "id": "admin"}],
        "allow_anonymous_access": True,
        "secret_keys": ["SECRET"],
        "providers": [
            {
                "provider": "toy",
                "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                "args": {
                    "users_to_passwords": {
                        "alice": "alice",
                        "bob": "bob",
                        "sue": "sue",
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


def group_parser(groupname):
    return {
        "chemists": ["bob", "mary"],
        "biologists": ["chris", "fred"],
        "physicists": ["sue", "tony"],
    }[groupname]


@pytest.fixture(scope="module")
def compile_access_tags_db():
    access_tag_config = {
        "roles": {
            "facility_admin": {
                "scopes": [
                    "read:data",
                    "read:metadata",
                    "write:data",
                    "write:metadata",
                    "create",
                    "register",
                ]
            }
        },
        "tags": {
            "alice_tag": {
                "users": [
                    {
                        "name": "alice",
                        "role": "facility_admin",
                    },
                ],
            },
            "chemists_tag": {
                "users": [
                    {
                        "name": "sue",
                        "scopes": ["write:data", "write:metadata"],
                    },
                ],
                "groups": [
                    {
                        "name": "chemists",
                        "scopes": ["read:data", "read:metadata"],
                    },
                ],
                "auto_tags": [
                    {
                        "name": "alice_tag",
                    },
                ],
            },
            "physicists_tag": {
                "groups": [
                    {
                        "name": "physicists",
                        "role": "facility_admin",
                    },
                ],
            },
        },
        "tag_owners": {
            "alice_tag": {
                "users": [
                    {
                        "name": "alice",
                    },
                ],
            },
            "chemists_tag": {
                "users": [
                    {
                        "name": "sue",
                    },
                ],
                "groups": [
                    {
                        "name": "chemists",
                    },
                ],
            },
            "physicists_tag": {
                "groups": [
                    {
                        "name": "physicists",
                    },
                ],
            },
        },
    }

    access_tags_compiler = AccessTagsCompiler(
        ALL_SCOPES,
        access_tag_config,
        {"uri": "file:compiled_tags_mem?mode=memory&cache=shared"},
        group_parser,
    )

    access_tags_compiler.load_tag_config()
    access_tags_compiler.compile()
    yield
    access_tags_compiler.connection.close()


@pytest.fixture(scope="module")
def access_control_test_context(tmpdir_module, compile_access_tags_db):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "writable_storage": str(tmpdir_module / "foo"),
                    "top_level_access_blob": {"tags": ["alice_tag"]},
                },
                "path": "/foo",
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "writable_storage": str(tmpdir_module / "bar"),
                    "top_level_access_blob": {"tags": ["chemists_tag"]},
                },
                "path": "/bar",
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "writable_storage": str(tmpdir_module / "baz"),
                    "top_level_access_blob": {"tags": ["physicists_tag"]},
                },
                "path": "/baz",
            },
        ],
    }

    config.update(server_config)
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        admin_client = from_context(context)
        with enter_username_password("admin", "admin"):
            admin_client.login()
            for k in ["foo", "bar", "baz"]:
                admin_client[k].write_array(
                    arr, key="data_A", access_tags=["alice_tag"]
                )
                admin_client[k].write_array(
                    arr, key="data_B", access_tags=["chemists_tag"]
                )
                admin_client[k].write_array(arr, key="data_C", access_tags=["public"])
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


def test_service_principal_access(tmpdir, sqlite_or_postgres_uri):
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
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": f"file://localhost{tmpdir}/data",
                    "init_if_not_exists": True,
                },
                "path": "/",
            }
        ],
        "access_control": {
            "access_policy": "tiled.access_control.access_policies:SimpleAccessPolicy",
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


def test_basic_access_control(access_control_test_context, enter_username_password):
    """
    Test that basic access control and tag compilation are working.
    Only tests simple visibility of the data (i.e. "read:metadata" scope),
      does not tests writing or full reading of the data.

    In other words, tests that compiled tags allow/disallow access including:
      - top-level tags
      - tags directly on datasets
      - tags "inherited" on datasets (auto_tags)
      - "public" tags on datasets
      - groups compiled into tags
      - scopes compiled into tags by a role
      - scopes compiled into tags by a scopes list
      - nested access blocked by upper tags (even if deeper tags would permit access)

    Note: MapAdapter does not support access control. As such, the server root
          does not currently filter top-level entries.
    """
    alice_client = from_context(access_control_test_context)
    with enter_username_password("alice", "alice"):
        alice_client.login()

    top = "foo"
    assert top in alice_client
    for data in ["data_A", "data_B", "data_C"]:
        assert data in alice_client[top]
        alice_client[top][data]

    top = "bar"
    assert top in alice_client
    for data in ["data_A", "data_B", "data_C"]:
        assert data in alice_client[top]
        alice_client[top][data]

    alice_client.logout()

    bob_client = from_context(access_control_test_context)
    with enter_username_password("bob", "bob"):
        bob_client.login()

    top = "foo"
    # no access control on MapAdapter
    # assert top not in bob_client
    for data in ["data_A", "data_B", "data_C"]:
        with pytest.raises(KeyError):
            bob_client[top][data]

    top = "bar"
    assert top in bob_client
    for data in ["data_A"]:
        assert data not in bob_client[top]
        with pytest.raises(KeyError):
            bob_client[top][data]
    for data in ["data_B", "data_C"]:
        assert data in bob_client[top]
        bob_client[top][data]

    bob_client.logout()


def test_writing_access_control(access_control_test_context, enter_username_password):
    """
    Test that writing access control and tag ownership is working.
    Only tests that the writing request does not fail.
    Does not test the written data for validity.

    This tests the following:
      - Writing without applying an access tag
      - Writing while applying an access tag the user owns
      - Writing while applying an access tag the user does not own
      - Writing while applying an access tag the user owns through group membership
      - Writing while applying an access tag that is not defined
      - Writing while applying the "public" tag (admin only)
      - Writing while applying a tag which does not give the user the minimum scopes
    """

    alice_client = from_context(access_control_test_context)
    with enter_username_password("alice", "alice"):
        alice_client.login()

    top = "foo"
    alice_client[top].write_array(arr, key="data_Q")
    alice_client[top].write_array(arr, key="data_R", access_tags=["alice_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_S", access_tags=["chemists_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_T", access_tags=["undefined_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_U", access_tags=["public"])
    alice_client.logout()

    admin_client = from_context(access_control_test_context)
    with enter_username_password("admin", "admin"):
        admin_client.login()

    top = "foo"
    admin_client[top].write_array(arr, key="data_V", access_tags=["public"])
    admin_client.logout()

    sue_client = from_context(access_control_test_context)
    with enter_username_password("sue", "sue"):
        sue_client.login()

    top = "baz"
    sue_client[top].write_array(arr, key="data_W", access_tags=["physicists_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        sue_client[top].write_array(arr, key="data_X", access_tags=["chemists_tag"])
    sue_client.logout()


# def test_bad_ops_on_tags(access_control_test_context, enter_username_password):
def test_user_owned_node_access_control(
    access_control_test_context, enter_username_password
):
    """
    Test that user-owned nodes (i.e. nodes created without access tags applied)
      are visible after creation and can be modified by the user.
    Also test that the data is visible after a tag is applied, and
      that other users cannot see user-owned nodes.
    """

    alice_client = from_context(access_control_test_context)
    with enter_username_password("alice", "alice"):
        alice_client.login()

    top = "foo"
    for data in ["data_Y"]:
        alice_client[top].write_array(arr, key=data)
        assert data in alice_client[top]
        alice_client[top][data]
        access_blob = alice_client[top][data].access_blob
        assert "user" in access_blob
        assert "alice" in access_blob["user"]
        alice_client[top][data].replace_metadata(access_tags=["alice_tag"])
        access_blob = alice_client[top][data].access_blob
        assert "user" not in access_blob
        assert "tags" in access_blob
        assert "alice_tag" in access_blob["tags"]
        assert data in alice_client[top]
        alice_client[top][data]

    top = "bar"
    for data in ["data_Z"]:
        alice_client[top].write_array(arr, key=data)
    alice_client.logout()

    bob_client = from_context(access_control_test_context)
    with enter_username_password("bob", "bob"):
        bob_client.login()

    top = "bar"
    for data in ["data_Z"]:
        assert data not in bob_client[top]
        with pytest.raises(KeyError):
            bob_client[top][data]
