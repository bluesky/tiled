import json
import sqlite3
from copy import deepcopy

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
                        "chris": "chris",
                        "sue": "sue",
                        "zoe": "zoe",
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

access_tag_config = {
    "roles": {
        "facility_user": {
            "scopes": [
                "read:data",
                "read:metadata",
            ]
        },
        "facility_admin": {
            "scopes": [
                "read:data",
                "read:metadata",
                "write:data",
                "write:metadata",
                "create",
                "register",
            ]
        },
    },
    "tags": {
        "alice_tag": {
            "users": [
                {
                    "name": "alice",
                    "role": "facility_admin",
                },
                {
                    "name": "chris",
                    "scopes": ["read:data", "read:metadata"],
                },
            ],
        },
        "chris_tag": {
            "users": [
                {
                    "name": "alice",
                    "role": "facility_admin",
                },
                {
                    "name": "chris",
                    "role": "facility_admin",
                },
            ],
        },
        "biologists_tag": {
            "users": [
                {
                    "name": "alice",
                    "role": "facility_admin",
                },
            ],
            "groups": [
                {
                    "name": "biologists",
                    "scopes": ["read:data", "read:metadata"],
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
                    "role": "facility_user",
                },
            ],
            "auto_tags": [
                {
                    "name": "alice_tag",
                },
            ],
        },
        "physicists_tag": {
            "users": [
                {
                    "name": "alice",
                    "role": "facility_admin",
                },
            ],
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
                {
                    "name": "chris",
                },
            ],
        },
        "biologists_tag": {
            "users": [
                {
                    "name": "alice",
                },
            ],
            "groups": [
                {
                    "name": "biologists",
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
            "users": [
                {
                    "name": "alice",
                },
            ],
            "groups": [
                {
                    "name": "physicists",
                },
            ],
        },
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
    access_tags_compiler = AccessTagsCompiler(
        ALL_SCOPES,
        access_tag_config,
        {"uri": "file:compiled_tags_mem?mode=memory&cache=shared"},
        group_parser,
    )

    access_tags_compiler.load_tag_config()
    access_tags_compiler.compile()
    yield access_tags_compiler
    access_tags_compiler.connection.close()


@pytest.fixture
def compile_access_tags_db_with_reset(compile_access_tags_db):
    access_tags_compiler = compile_access_tags_db
    access_tag_config_copy = deepcopy(access_tag_config)
    access_tags_compiler.tag_config = access_tag_config_copy
    yield access_tags_compiler
    access_tags_compiler.tag_config = access_tag_config
    access_tags_compiler.group_parser = group_parser
    access_tags_compiler.clear_raw_tags()
    access_tags_compiler.load_tag_config()
    access_tags_compiler.recompile()


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
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "named_memory": "catalog_qux",
                    "writable_storage": str(tmpdir_module / "qux"),
                    "top_level_access_blob": {"tags": ["public"]},
                },
                "path": "/qux",
            },
        ],
    }

    config.update(server_config)
    contexts = []
    clients = {}

    def _create_and_login_context(username, password=None, api_key=None):
        if not any([password, api_key]):
            raise ValueError("Please provide either 'password' or 'api_key' for auth")

        if client := clients.get(username, None):
            return client
        app = build_app_from_config(config)
        context = Context.from_app(
            app, uri=f"http://local-tiled-app-{username}/api/v1", api_key=api_key
        )
        contexts.append(context)
        client = from_context(context, remember_me=False)
        clients[username] = client
        if api_key is None:
            with enter_username_password(username, password):
                client.context.login(remember_me=False)
        return client

    admin_client = _create_and_login_context("admin", "admin")
    for k in ["foo", "bar", "baz", "qux"]:
        admin_client[k].write_array(arr, key="data_A", access_tags=["alice_tag"])
        admin_client[k].write_array(arr, key="data_B", access_tags=["chemists_tag"])
        admin_client[k].write_array(arr, key="data_C", access_tags=["public"])

    yield _create_and_login_context

    for context in contexts:
        context.close()


def test_access_tag_compiler(compile_access_tags_db_with_reset):
    """
    Test that compilation of access tags is working. This tests:
    - Adding and removing a tag
    - Adding and removing a role
    - Adding and removing a user from a tag
    - Adding and removing a group from a tag
    - Adding and removing from the `auto_tags` for a tag
    - Adding and removing a tag from the `tag_owners` section
    - Adding and removing users and groups from the owners of a tag
    - Adding and removing a member from a group
    - Changing a user's role/scopes on a tag
    - Changing a group's role/scopes on a tag
    - Making a tag public/not-public
    - Disallow redefining the `public` tag
    """
    access_tags_compiler = compile_access_tags_db_with_reset
    compiler_tag_config = access_tags_compiler.tag_config

    def new_group_parser(groupname):
        return {
            "chemists": ["bob", "mary", "kate"],
            "biologists": ["chris", "fred"],
            "physicists": ["sue", "tony"],
        }[groupname]

    access_tags_compiler.group_parser = new_group_parser

    compiler_tag_config["tags"].update(
        {"new_tag": {"users": [{"name": "tony", "scopes": ["read:metadata"]}]}}
    )
    compiler_tag_config["roles"].update({"new_role": {"scopes": ["read:metadata"]}})
    compiler_tag_config["tags"]["biologists_tag"]["users"].append(
        {"name": "tony", "role": "facility_user"}
    )
    compiler_tag_config["tags"]["physicists_tag"]["groups"].append(
        {"name": "biologists", "role": "facility_user"}
    )
    compiler_tag_config["tags"]["chemists_tag"]["auto_tags"].append({"name": "new_tag"})
    compiler_tag_config["tag_owners"].update({"new_tag": {"users": [{"name": "tony"}]}})
    compiler_tag_config["tag_owners"]["biologists_tag"]["users"].append(
        {"name": "tony"}
    )
    compiler_tag_config["tag_owners"]["chemists_tag"]["groups"].append(
        {"name": "biologists"}
    )
    compiler_tag_config["tags"]["alice_tag"]["users"][0]["role"] = "facility_user"
    compiler_tag_config["tags"]["biologists_tag"]["groups"][0].pop("scopes")
    compiler_tag_config["tags"]["biologists_tag"]["groups"][0].update(
        {"role": "facility_admin"}
    )
    compiler_tag_config["tags"]["alice_tag"].update({"auto_tags": [{"name": "public"}]})

    access_tags_compiler.load_tag_config()
    access_tags_compiler.recompile()

    db = sqlite3.connect("file:compiled_tags_mem?mode=memory&cache=shared", uri=True)
    cursor = db.cursor()
    # check that new tag was added and compiled with user+scopes
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM tags WHERE name = ?);",
        ("new_tag",),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("new_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])

    # check that new role was added - note roles do not get saved in the db
    assert "new_role" in access_tags_compiler.roles

    # check that newly added user and group were given scopes on tag
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("biologists_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("physicists_tag", "chris"),
    )
    assert bool(cursor.fetchone()[0])

    # check that auto_tag added ACL to parent tag
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("chemists_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])

    # check tag was added to tag_owners section
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ?);",
        ("new_tag",),
    )
    assert bool(cursor.fetchone()[0])

    # check adding new user and group to owners of tags
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ? AND user_name = ?);",
        ("biologists_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ? AND user_name = ?);",
        ("chemists_tag", "chris"),
    )
    assert bool(cursor.fetchone()[0])

    # check that the role/scopes changes for a user and group on a tag were effective
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes "
        "WHERE tag_name = ? AND user_name = ? AND scope_name = ?);",
        ("alice_tag", "alice", "write:metadata"),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes "
        "WHERE tag_name = ? AND user_name = ? AND scope_name = ?);",
        ("biologists_tag", "chris", "write:metadata"),
    )
    assert bool(cursor.fetchone()[0])

    # check tha tag was marked as public after inheriting public tag
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM public_tags WHERE name = ?);",
        ("alice_tag",),
    )
    assert bool(cursor.fetchone()[0])

    # check that user added to group was compiled into tag ACL
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("chemists_tag", "kate"),
    )
    assert bool(cursor.fetchone()[0])

    # attempt redefining the public tag (and fail)
    compiler_tag_config["tags"].update(
        {"public": {"users": [{"name": "tony", "scopes": ["read:metadata"]}]}}
    )
    access_tags_compiler.load_tag_config()
    with pytest.raises(ValueError):
        access_tags_compiler.recompile()

    # remove all changes/additions by reverting to the original config
    access_tags_compiler.tag_config = access_tag_config
    access_tags_compiler.group_parser = group_parser
    access_tags_compiler.clear_raw_tags()
    access_tags_compiler.load_tag_config()
    access_tags_compiler.recompile()

    # check that new tag was removed and no longer compiled
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM tags WHERE name = ?);",
        ("new_tag",),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("new_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])

    # check that new role was removed - note roles do not get saved in the db
    assert "new_role" not in access_tags_compiler.roles

    # check that removed user and group were not given scopes on tag
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("biologists_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("physicists_tag", "chris"),
    )
    assert bool(cursor.fetchone()[0])

    # check that auto_tag ACL removed from parent tag
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("chemists_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])

    # check tag was removed from tag_owners section
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ?);",
        ("new_tag",),
    )
    assert bool(cursor.fetchone()[0])

    # check removing user and group from owners of tags
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ? AND user_name = ?);",
        ("biologists_tag", "tony"),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_owners WHERE tag_name = ? AND user_name = ?);",
        ("chemists_tag", "chris"),
    )
    assert bool(cursor.fetchone()[0])

    # check that the role/scopes changes for a user and group on a tag were undone
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM user_tag_scopes "
        "WHERE tag_name = ? AND user_name = ? AND scope_name = ?);",
        ("alice_tag", "alice", "write:metadata"),
    )
    assert bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes "
        "WHERE tag_name = ? AND user_name = ? AND scope_name = ?);",
        ("biologists_tag", "chris", "write:metadata"),
    )
    assert bool(cursor.fetchone()[0])

    # check tha tag was unmarked as public after removing the public auto_tag
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM public_tags WHERE name = ?);",
        ("alice_tag",),
    )
    assert bool(cursor.fetchone()[0])

    # check that user removed from group was compiled out of tag ACL
    cursor.execute(
        "SELECT NOT EXISTS(SELECT 1 FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?);",
        ("chemists_tag", "kate"),
    )
    assert bool(cursor.fetchone()[0])


def test_basic_access_control(access_control_test_context_factory):
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
    # no access control on MapAdapter - can't filter top-level yet
    # assert top not in bob_client
    for data in ["data_A", "data_B", "data_C"]:
        # Alice has access below the top-level, given by a direct tag
        # Bob does not have access to any data, blocked by the top-level's tag
        # data_A - alice has access given by a direct tag of which they are a user
        # data_B - alice has access given by an inherited tag
        # data_C - alice has access given by a public tag
        assert data in alice_client[top]
        alice_client[top][data]
        with pytest.raises(KeyError):
            bob_client[top][data]

    top = "bar"
    assert top in alice_client
    assert top in bob_client
    for data in ["data_A"]:
        # Alice has access below the top-level, given by an inherited tag
        # data_A - bob does not have access conferred by any tags
        assert data in alice_client[top]
        alice_client[top][data]
        assert data not in bob_client[top]
        with pytest.raises(KeyError):
            bob_client[top][data]
    for data in ["data_B", "data_C"]:
        # Bob has access below the top-level, given by a direct tag of which they are in a group
        # data_B - alice has scopes compiled in via role
        # data_B - bob has access given by a direct tag of which they are in a group
        # data_B - bob has scopes compiled in via list of scopes
        # data_C - alice and bob are given access by a public tag
        assert data in alice_client[top]
        alice_client[top][data]
        assert data in bob_client[top]
        bob_client[top][data]


def test_writing_access_control(access_control_test_context_factory):
    """
    Test that writing access control and tag ownership is working.
    Only tests that the writing request does not fail.
    Does not test the written data for validity.

    This tests the following:
      - Writing without applying an access tag
      - Writing while applying an access tag the user owns
      - Writing while applying an access tag the user does not own
      - Writing while applying an access tag that is not defined
      - Writing while applying the "public" tag (admin only)
      - Writing into a location where the user does not have write access
      - Writing while applying an access tag the user owns through group membership
      - Writing while applying multiple access tags
      - Writing while applying a tag which does not give the user the minimum scopes
    """

    alice_client = access_control_test_context_factory("alice", "alice")
    bob_client = access_control_test_context_factory("bob", "bob")
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

    top = "bar"
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        bob_client[top].write_array(arr, key="data_V")

    top = "baz"
    sue_client[top].write_array(
        arr, key="data_W", access_tags=["physicists_tag", "chemists_tag"]
    )
    access_tags = sue_client[top]["data_W"].access_blob["tags"]
    assert "physicists_tag" in access_tags
    assert "chemists_tag" in access_tags
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        sue_client[top].write_array(arr, key="data_X", access_tags=["chemists_tag"])


def test_user_owned_node_access_control(access_control_test_context_factory):
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
        # Create a new user-owned node
        alice_client[top].write_array(arr, key=data)
        assert data in alice_client[top]
        alice_client[top][data]
        access_blob = alice_client[top][data].access_blob
        assert "user" in access_blob
        assert "alice" in access_blob["user"]
        # Convert from user-owned node to a tagged node
        alice_client[top][data].replace_metadata(access_tags=["alice_tag"])
        access_blob = alice_client[top][data].access_blob
        assert "user" not in access_blob
        assert "tags" in access_blob
        assert "alice_tag" in access_blob["tags"]
        assert data in alice_client[top]
        alice_client[top][data]

    top = "bar"
    for data in ["data_Z"]:
        # Create a user-owned node and check that it is access restricted
        alice_client[top].write_array(arr, key=data)
        assert data not in bob_client[top]
        with pytest.raises(KeyError):
            bob_client[top][data]


def test_public_anonymous_access_control(access_control_test_context_factory):
    """
    Test that data which is tagged public is visible to unauthenticated
      (anonymous) users when the server allows anonymous access.
    """
    zoe_client = access_control_test_context_factory("zoe", "zoe")
    zoe_client.logout()
    anon_client = zoe_client

    top = "qux"
    assert top in anon_client
    for data in ["data_A", "data_B"]:
        assert data not in anon_client[top]
        with pytest.raises(KeyError):
            anon_client[top][data]
    for data in ["data_C"]:
        assert data in anon_client[top]
        anon_client[top][data]


def test_admin_access_control(access_control_test_context_factory):
    """
    Test that admin accounts have various elevated privileges, including:
    - Apply/remove public tag to/from a node
    - Apply/remove tags while ignoring minimum scopes
    - Apply/remove tags that the user does not own
    - View all data regardless of tags
    - Apply an access tag that is not defined (disallowed)
    - Remove all tags from a node, but still view that node
    - Also includes test of an empty tags list blocking access for regular users
    """
    admin_client = access_control_test_context_factory("admin", "admin")
    alice_client = access_control_test_context_factory("alice", "alice")

    top = "foo"
    for data in ["data_L"]:
        # create a node and tag it public
        admin_client[top].write_array(arr, key=data, access_tags=["public"])
        assert data in alice_client[top]
        alice_client[top][data]
        # remove public access, in fact remove all tags and ignore missing scopes
        admin_client[top][data].replace_metadata(access_tags=[])
        assert data in admin_client[top]
        admin_client[top][data]
        assert data not in alice_client[top]
        with pytest.raises(KeyError):
            alice_client[top][data]
        # apply a tag that the admin user does not own and ignore missing scopes
        admin_client[top][data].replace_metadata(access_tags=["chemists_tag"])
        assert data in admin_client[top]
        admin_client[top][data]
        assert data in alice_client[top]
        alice_client[top][data]
        # remove a tag that the admin user does not own
        admin_client[top][data].replace_metadata(access_tags=["chemists_tag"])
        # apply a tag which is not defined
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            admin_client[top][data].replace_metadata(access_tags=["undefined_tag"])


def test_update_node_access_control(access_control_test_context_factory):
    """
    Test that access control on metadata changes is working.

    This tests the following:
      - Update metadata while having write access
      - Prevent updating metadata without having write access
      - Successfully add an access tag and remove an access tag
      - Prevent adding or removing an access tag without having write access
      - Prevent adding or removing access tags which the user does not own
      - Add and remove access tags which do not confer the necessary scopes
      - Attempt to add an undefined access tag (not allowed)
      - Attempt to add the "public" tag (admin only)
      - Attempt to remove the "public" tag (admin only)
      - Attempt to remove an undefined access tag (not allowed)
    """
    admin_client = access_control_test_context_factory("admin", "admin")
    alice_client = access_control_test_context_factory("alice", "alice")
    chris_client = access_control_test_context_factory("chris", "chris")
    sue_client = access_control_test_context_factory("sue", "sue")

    top = "qux"
    for data in ["data_F"]:
        admin_client[top].write_array(arr, key=data, access_tags=["alice_tag"])
        # successfully update metadata, user has write access
        alice_client[top][data].replace_metadata(metadata={"materials": ["Cu", "Ag"]})
        assert "Ag" in alice_client[top][data].metadata["materials"]
        # fail to update metadata, user does not have write access
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            chris_client[top][data].replace_metadata(
                metadata={"materials": ["Ag", "Au"]}
            )
        assert "Au" not in chris_client[top][data].metadata["materials"]

        # succeeds to add a new access tag and remove the old access tag
        alice_client[top][data].replace_metadata(access_tags=["biologists_tag"])
        access_tags = alice_client[top][data].access_blob["tags"]
        assert "alice_tag" not in access_tags
        assert "biologists_tag" in access_tags

        # fails to add a new access tag, user does not have write access
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            chris_client[top][data].replace_metadata(
                access_tags=["biologists_tag", "chris_tag"]
            )
        admin_client[top][data].replace_metadata(
            access_tags=["alice_tag", "biologists_tag"]
        )
        # fails to remove an access tag, user does not have write access
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            chris_client[top][data].replace_metadata(access_tags=["biologists_tag"])
        admin_client[top][data].replace_metadata(access_tags=["biologists_tag"])

        # fails to add a new access tag, user does not own the tag
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            alice_client[top][data].replace_metadata(
                access_tags=["biologists_tag", "chris_tag"]
            )
        admin_client[top][data].replace_metadata(
            access_tags=["biologists_tag", "chris_tag"]
        )
        # fails to remove an access tag, user does not own the tag
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            chris_client[top][data].replace_metadata(access_tags=["biologists_tag"])
        admin_client[top][data].replace_metadata(access_tags=["biologists_tag"])

        # fail to add an undefined tag
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            alice_client[top][data].replace_metadata(
                access_tags=["undefined_tag", "biologists_tag"]
            )
        # fail to add the "public" tag
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            alice_client[top][data].replace_metadata(
                access_tags=["public", "biologists_tag"]
            )
        admin_client[top][data].replace_metadata(
            access_tags=["public", "biologists_tag"]
        )
        # fail to remove the "public" tag
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            alice_client[top][data].replace_metadata(access_tags=["biologists_tag"])
        admin_client[top][data].replace_metadata(access_tags=["biologists_tag"])

        # surgically add an undefined tag to the node, then fail when trying to remove it
        db = sqlite3.connect(f"file:catalog_{top}?mode=memory&cache=shared", uri=True)
        cursor = db.cursor()
        cursor.execute(
            "UPDATE nodes SET access_blob = ? WHERE key = ?",
            ('{"tags": ["undefined_tag", "biologists_tag"]}', data),
        )
        db.commit()
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            alice_client[top][data].replace_metadata(access_tags=["biologists_tag"])

    top = "baz"
    for data in ["data_G"]:
        sue_client[top].write_array(arr, key=data)
        # fail to apply a new access tag as it does not give the user the
        # minimum required scopes
        # this case only affects user-owned nodes:
        # - if we did not have read access, we would not even see the node
        # - if we did not have write access, we would be blocked by scopes
        # - if an existing tag already gave us read and write, adding a tag would succeed
        # - if an existing tag already gave us read and write, and we tried to remove it
        #   while adding the new tag, it's really the removal operation that prevents this,
        #   and this operation is tested below
        # this leaves only user-owned nodes (full access for the user, but no existing tags)
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            sue_client[top][data].replace_metadata(access_tags=["chemists_tag"])
        sue_client[top][data].replace_metadata(
            access_tags=["physicists_tag", "chemists_tag"]
        )
        # fail to apply the new access tag as removing the old access tag results
        # in insufficent scopes for the user
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            sue_client[top][data].replace_metadata(access_tags=["chemists_tag"])


def test_empty_access_blob_access_control(access_control_test_context_factory):
    """
    Test the cases where a node in the catalog has an empty access blob.
    This case occurs when migrating an older catalog without also
      populating the access_blob column.
    """
    admin_client = access_control_test_context_factory("admin", "admin")
    alice_client = access_control_test_context_factory("alice", "alice")

    top = "qux"
    for data in ["data_M"]:
        admin_client[top].write_array(arr, key=data, access_tags=["alice_tag"])
        db = sqlite3.connect(f"file:catalog_{top}?mode=memory&cache=shared", uri=True)
        cursor = db.cursor()
        cursor.execute(
            "UPDATE nodes SET access_blob = json('{}') WHERE key == 'data_M'"
        )
        db.commit()

        assert data in admin_client[top]
        admin_client[top][data]
        assert data not in alice_client[top]
        with pytest.raises(KeyError):
            alice_client[top][data]


def test_container_access_control(access_control_test_context_factory):
    """
    Test that access control for data nested in containers allows/denies access.
    This mostly checks that if a user does not have access to a container,
      that user cannot reach inside the container to view data they would
      otherwise have access for.
    """
    alice_client = access_control_test_context_factory("alice", "alice")
    sue_client = access_control_test_context_factory("sue", "sue")

    top = "baz"
    for c in ["C1"]:
        alice_client[top].create_container(key=c, access_tags=["alice_tag"])
        alice_client[top][c].write_array(
            arr, key=f"{c}_array", access_tags=["physicists_tag"]
        )
        assert f"{c}_array" in alice_client[top][c]
        alice_client[top][c][f"{c}_array"]
        assert c not in sue_client[top]
        with pytest.raises(KeyError):
            sue_client[top][c][f"{c}_array"]


def test_node_export_access_control(
    access_control_test_context_factory, buffer_factory
):
    """
    Test access control when exporting from Tiled (here: a container).
    These tests include:
    - Test that top-level nodes are disincluded appropriately
      (MapAdapter->CatalogAdapter transition).
    - Test that basic export works - i.e. nodes for which the user has
      access are included.
    - Test that nodes for which the user does not have access are not included.
    - Test that this behavior also works for user-owned (untagged) nodes.
    """
    alice_client = access_control_test_context_factory("alice", "alice")
    sue_client = access_control_test_context_factory("sue", "sue")

    top = "baz"
    alice_client[top].write_array(arr, key="data_D")
    sue_client[top].write_array(arr, key="data_E")

    alice_export_buffer = buffer_factory()
    sue_export_buffer = buffer_factory()

    alice_client.export(alice_export_buffer, format="application/json")
    sue_client.export(sue_export_buffer, format="application/json")

    alice_export_buffer.seek(0)
    sue_export_buffer.seek(0)

    alice_exported_data = json.loads(alice_export_buffer.read())
    sue_exported_data = json.loads(sue_export_buffer.read())

    top = "foo"
    assert top in alice_exported_data["contents"]
    assert top not in sue_exported_data["contents"]
    for data in ["data_A", "data_B", "data_C"]:
        assert data in alice_exported_data["contents"][top]["contents"]
        alice_exported_data["contents"][top]["contents"][data]

    top = "baz"
    assert top in alice_exported_data["contents"]
    assert top in sue_exported_data["contents"]
    for data in ["data_A", "data_B", "data_D"]:
        assert data not in sue_exported_data["contents"][top]["contents"]
        with pytest.raises(KeyError):
            sue_exported_data["contents"][top]["contents"][data]
    for data in ["data_C", "data_E"]:
        assert data in sue_exported_data["contents"][top]["contents"]
        sue_exported_data["contents"][top]["contents"][data]


def test_apikey_auth_access_control(access_control_test_context_factory):
    """
    Test access control when authenticated by an API key, including:
    - Allow basic access with an API key that is not tag-restricted
    - Disallow access to tags that are not added to a tag-restricted API key
    - Allow access to tags that are added to a tag-restricted API key
    - User-owned node access/writing is blocked when using a tag-restricted API key
    """
    alice_client = access_control_test_context_factory("alice", "alice")
    alice_apikey_info = alice_client.context.create_api_key()
    alice_client.logout()
    alice_client.context.api_key = alice_apikey_info["secret"]

    top = "foo"
    for data in ["data_A"]:
        assert data in alice_client[top]
        alice_client[top][data]

    top = "bar"
    alice_client[top].write_array(arr, key="data_O")

    alice_apikey_info = alice_client.context.create_api_key(
        access_tags=["chemists_tag"]
    )
    alice_client.context.api_key = alice_apikey_info["secret"]

    top = "bar"
    for data in ["data_A"]:
        assert data not in alice_client[top]
        with pytest.raises(KeyError):
            alice_client[top][data]
    for data in ["data_B"]:
        assert data in alice_client[top]
        alice_client[top][data]
    for data in ["data_O"]:
        assert data not in alice_client[top]
        with pytest.raises(KeyError):
            alice_client[top][data]
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        alice_client[top].write_array(arr, key="data_P")


def test_service_principal_access_control(
    access_control_test_context_factory, compile_access_tags_db_with_reset
):
    """
    Test that access control works for service principals.
    Creates a service principal and updates the access tag config to
      add this prinicpal to a tag.
    """
    admin_client = access_control_test_context_factory("admin", "admin")
    sp = admin_client.context.admin.create_service_principal("user")
    sp_apikey_info = admin_client.context.admin.create_api_key(sp["uuid"])
    sp_client = access_control_test_context_factory(
        sp["uuid"], api_key=sp_apikey_info["secret"]
    )

    access_tags_compiler = compile_access_tags_db_with_reset
    compiler_tag_config = access_tags_compiler.tag_config

    compiler_tag_config["tags"]["physicists_tag"]["users"].append(
        {"name": sp["uuid"], "role": "facility_admin"}
    )
    compiler_tag_config["tag_owners"]["physicists_tag"]["users"].append(
        {"name": sp["uuid"]}
    )

    access_tags_compiler.load_tag_config()
    access_tags_compiler.recompile()

    top = "baz"
    for data in ["data_A"]:
        assert data not in sp_client[top]
        with pytest.raises(KeyError):
            sp_client[top][data]
    for data in ["data_N"]:
        sp_client[top].write_array(arr, key=data, access_tags=["physicists_tag"])
        assert data in sp_client[top]
        sp_client[top][data]
