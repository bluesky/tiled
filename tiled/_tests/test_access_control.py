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
        "uri": "sqlite:///file:authn_mem?mode=memory&cache=shared&uri=true",  # in-memory
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
def access_control_test_context_factory(tmpdir_module, compile_access_tags_db):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "named_memory": "catalog_foo",
                    "writable_storage": str(tmpdir_module / "foo"),
                    "top_level_access_blob": {"tags": ["alice_tag"]},
                },
                "path": "/foo",
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "named_memory": "catalog_bar",
                    "writable_storage": str(tmpdir_module / "bar"),
                    "top_level_access_blob": {"tags": ["chemists_tag"]},
                },
                "path": "/bar",
            },
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "named_memory": "catalog_baz",
                    "writable_storage": str(tmpdir_module / "baz"),
                    "top_level_access_blob": {"tags": ["physicists_tag"]},
                },
                "path": "/baz",
            },
        ],
    }

    config.update(server_config)
    contexts = []
    clients = {}

    def _create_and_login_context(username, password):
        if client := clients.get(username, None):
            return client
        app = build_app_from_config(config)
        context = Context.from_app(app, uri=f"http://local-tiled-app-{username}/api/v1")
        contexts.append(context)
        client = from_context(context, remember_me=False)
        clients[username] = client
        with enter_username_password(username, password):
            client.context.login(remember_me=False)
        return client

    admin_client = _create_and_login_context("admin", "admin")
    for k in ["foo", "bar", "baz"]:
        admin_client[k].write_array(arr, key="data_A", access_tags=["alice_tag"])
        admin_client[k].write_array(arr, key="data_B", access_tags=["chemists_tag"])
        admin_client[k].write_array(arr, key="data_C", access_tags=["public"])

    yield _create_and_login_context

    for context in contexts:
        context.close()


def test_basic_access_control(
    access_control_test_context_factory, enter_username_password
):
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
    alice_client = access_control_test_context_factory("alice", "alice")
    bob_client = access_control_test_context_factory("bob", "bob")

    top = "foo"
    assert top in alice_client
    # no access control on MapAdapter
    # assert top not in bob_client
    for data in ["data_A", "data_B", "data_C"]:
        assert data in alice_client[top]
        alice_client[top][data]
        with pytest.raises(KeyError):
            bob_client[top][data]

    top = "bar"
    assert top in alice_client
    assert top in bob_client
    for data in ["data_A"]:
        assert data in alice_client[top]
        alice_client[top][data]
        assert data not in bob_client[top]
        with pytest.raises(KeyError):
            bob_client[top][data]
    for data in ["data_B", "data_C"]:
        assert data in alice_client[top]
        alice_client[top][data]
        assert data in bob_client[top]
        bob_client[top][data]


def test_writing_access_control(
    access_control_test_context_factory, enter_username_password
):
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

    alice_client = access_control_test_context_factory("alice", "alice")
    admin_client = access_control_test_context_factory("admin", "admin")
    sue_client = access_control_test_context_factory("sue", "sue")

    top = "foo"
    alice_client[top].write_array(arr, key="data_Q")
    alice_client[top].write_array(arr, key="data_R", access_tags=["alice_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_S", access_tags=["chemists_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_T", access_tags=["undefined_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_U", access_tags=["public"])

    top = "foo"
    admin_client[top].write_array(arr, key="data_V", access_tags=["public"])

    top = "baz"
    sue_client[top].write_array(arr, key="data_W", access_tags=["physicists_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        sue_client[top].write_array(arr, key="data_X", access_tags=["chemists_tag"])


# def test_bad_ops_on_tags(access_control_test_context_factory, enter_username_password):
def test_user_owned_node_access_control(
    access_control_test_context_factory, enter_username_password
):
    """
    Test that user-owned nodes (i.e. nodes created without access tags applied)
      are visible after creation and can be modified by the user.
    Also test that the data is visible after a tag is applied, and
      that other users cannot see user-owned nodes.
    """

    alice_client = access_control_test_context_factory("alice", "alice")
    bob_client = access_control_test_context_factory("bob", "bob")

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
        assert data not in bob_client[top]
        with pytest.raises(KeyError):
            bob_client[top][data]
