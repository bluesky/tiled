import asyncio
import time

import numpy
import pytest

from tiled.access_control.access_tags import AccessTagsCompiler
from tiled.access_control.scopes import ALL_SCOPES
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

from .utils import enter_username_password

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
                        "admin": "admin",
                    },
                },
            },
        ],
    },
}


access_tag_config = {
    "roles": {
        "facility_admin": {
            "scopes": [
                "read:data",
                "read:metadata",
                "write:data",
                "write:metadata",
                "delete:node",
                "delete:revision",
                "create:node",
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
                    "name": "bob",
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
                    "name": "bob",
                },
            ],
        },
    },
}


def group_parser(groupname):
    return {
        "physicists": ["alice", "bob"],
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

    yield _create_and_login_context

    for context in contexts:
        context.close()


async def coro_test(c, keys):
    child_node = await c.context.http_client.app.state.root_tree[
        keys[0]
    ].lookup_adapter([keys[1]])
    return child_node


def test_created_and_updated_info_with_users(access_control_test_context_factory):
    """
    Test that created_by and updated_by fields are correctly set
    on node creation and metadata update.
    """

    alice_client = access_control_test_context_factory("alice", "alice")
    bob_client = access_control_test_context_factory("bob", "bob")

    top = "foo"
    for data in ["data_M"]:
        # Create a new node and check created_by and updated_by
        alice_client[top].write_array(
            arr,
            key=data,
            metadata={"description": "initial"},
            access_tags=["alice_tag"],
        )
        coro_obj = coro_test(alice_client, [top, data])
        result = asyncio.run(coro_obj)
        # When the array is first created, created_by and updated_by should be the same
        assert result.node.created_by == "alice"
        assert result.node.updated_by == "alice"
        assert result.node.time_created.date() == result.node.time_updated.date()

        time.sleep(1)  # ensure time_updated is different
        bob_client[top][data].replace_metadata(metadata={"description": "updated"})
        coro_obj = coro_test(bob_client, [top, data])
        result = asyncio.run(coro_obj)
        # After Bob updates the metadata, updated_by should be Bob, created_by should remain Alice
        assert result.node.created_by != result.node.updated_by
        assert result.node.time_created != result.node.time_updated


def test_created_and_updated_info_with_service(access_control_test_context_factory):
    """
    Test that created_by and updated_by fields are correctly set
    when a service (non-user) creates or updates a node.
    """

    admin_client = access_control_test_context_factory("admin", "admin")

    top = "qux"
    # Create a new Service Principal. It is identified by its UUID.
    # It has no "identities"; it cannot log in with a password.
    sp_create = admin_client.context.admin.create_service_principal("admin")
    sp_apikey_info = admin_client.context.admin.create_api_key(sp_create["uuid"])
    # Create a client for the Service Principal using its API key
    sp_create_client = access_control_test_context_factory(
        sp_create["uuid"], api_key=sp_apikey_info["secret"]
    )

    # Create a new Service Principal for updating the node.
    # It is identified by its UUID. It has no "identities"; it cannot log in with a password.
    sp_update = admin_client.context.admin.create_service_principal("admin")
    sp_update_apikey_info = admin_client.context.admin.create_api_key(sp_update["uuid"])
    # Create a client for the new Service Principal using its API key
    sp_update_client = access_control_test_context_factory(
        sp_update["uuid"], api_key=sp_update_apikey_info["secret"]
    )

    for data in ["data_N"]:
        # Create a new node with the service principal and check created_by and updated_by
        sp_create_client[top].write_array(
            arr, key=data, metadata={"description": "initial"}, access_tags=["public"]
        )

        coro_obj = coro_test(sp_create_client, [top, data])
        result = asyncio.run(coro_obj)

        # When the array is first created by the service principal,
        # created_by and updated_by should indicate the service principal
        assert sp_create["uuid"] in result.node.created_by
        assert sp_create["uuid"] in result.node.updated_by
        assert result.node.time_created.date() == result.node.time_updated.date()

        time.sleep(1)  # ensure time_updated is different
        sp_update_client[top][data].replace_metadata(
            metadata={"description": "updated"}
        )
        coro_obj = coro_test(sp_update_client, [top, data])
        result = asyncio.run(coro_obj)
        # After the service principal updates the metadata,
        # updated_by should indicate the second service principal,
        # created_by should indicate the first service principal
        assert result.node.updated_by != result.node.created_by
        assert sp_update["uuid"] in result.node.updated_by
        assert result.node.time_created != result.node.time_updated
