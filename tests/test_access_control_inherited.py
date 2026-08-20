"""Tests for InheritedTagAccessPolicy.

Mirrors the structure of test_access_control.py but focuses on tag-inheritance
behaviours: untagged children inheriting access from their nearest tagged
ancestor, and that inheritance being consistent between listing (filters) and
direct node access (allowed_scopes).

Tree planted by the admin fixture:

    project_alice    {"tags": ["alice_tag"]}
      raw_data       {"tags": []}  <- inherits alice_tag
        detector_1   {"tags": []}  <- inherits alice_tag (two levels deep)
      sub_y          {"tags": ["chemists_tag"]}  <- shadows alice_tag; nearest
                                                    ancestor for its children
                                                    is sub_y, not project_alice

    project_bob      {"tags": ["chemists_tag"]}
      result         {"tags": []}  <- inherits chemists_tag

    project_public   {"tags": ["public"]}
      public_data    {"tags": []}  <- inherits public tag
"""

import pytest

from tiled.access_control.access_tags import AccessTagsCompiler
from tiled.access_control.scopes import ALL_SCOPES
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

from .utils import enter_username_password

# ---------------------------------------------------------------------------
# Shared configuration
# ---------------------------------------------------------------------------

_TAGS_DB_URI = "file:compiled_tags_inh?mode=memory&cache=shared"
_AUTH_DB_URI = "sqlite:///file:authn_inh?mode=memory&cache=shared&uri=true"
_CATALOG_NAME = "catalog_inherited_ac"

server_config = {
    "access_control": {
        "access_policy": "tiled.access_control.access_policies:InheritedTagAccessPolicy",
        "args": {
            "provider": "toy",
            "tags_db": {"uri": _TAGS_DB_URI},
            "access_tags_parser": "tiled.access_control.access_tags:AccessTagsParser",
        },
    },
    "authentication": {
        "tiled_admins": [{"provider": "toy", "id": "admin"}],
        "allow_anonymous_access": True,
        "secret_keys": ["SECRET_INH"],
        "providers": [
            {
                "provider": "toy",
                "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                "args": {
                    "users_to_passwords": {
                        "alice": "alice",
                        "bob": "bob",
                        "zoe": "zoe",
                        "admin": "admin",
                    },
                },
            },
        ],
    },
    "database": {"uri": _AUTH_DB_URI},
}

access_tag_config = {
    "roles": {
        "facility_user": {"scopes": ["read:data", "read:metadata"]},
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
            "users": [{"name": "alice", "role": "facility_admin"}],
        },
        "chemists_tag": {
            "groups": [{"name": "chemists", "role": "facility_user"}],
        },
    },
    "tag_owners": {
        "alice_tag": {"users": [{"name": "alice"}]},
        "chemists_tag": {"groups": [{"name": "chemists"}]},
    },
}


def group_parser(groupname):
    return {"chemists": ["bob"]}[groupname]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compile_inherited_tags_db():
    compiler = AccessTagsCompiler(
        ALL_SCOPES,
        access_tag_config,
        {"uri": _TAGS_DB_URI},
        group_parser,
    )
    compiler.load_tag_config()
    compiler.compile()
    yield compiler
    compiler.connection.close()


@pytest.fixture(scope="module")
def inherited_ac_context_factory(tmpdir_module, compile_inherited_tags_db):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "args": {
                    "named_memory": _CATALOG_NAME,
                    "writable_storage": str(tmpdir_module / "inherited_ac"),
                },
                "path": "/",
            },
        ],
    }
    config.update(server_config)

    contexts = []
    clients = {}

    def _client(username, password=None, api_key=None):
        if existing := clients.get(username):
            return existing
        app = build_app_from_config(config)
        context = Context.from_app(
            app, uri=f"http://local-tiled-inh-{username}/api/v1", api_key=api_key
        )
        contexts.append(context)
        client = from_context(context, remember_me=False)
        clients[username] = client
        if api_key is None and password is not None:
            with enter_username_password(username, password):
                client.context.login(remember_me=False)
        return client

    # Plant the tree as admin.
    # Nodes that should inherit use access_tags=[] (empty tag list, admin-only).
    # Nodes with own tags use access_tags=[...].
    admin = _client("admin", "admin")

    admin.create_container(key="project_alice", access_tags=["alice_tag"])
    admin["project_alice"].create_container(key="raw_data", access_tags=[])
    admin["project_alice"]["raw_data"].create_container(
        key="detector_1", access_tags=[]
    )
    # sub_y has its own chemists_tag — it shadows project_alice for its children
    admin["project_alice"].create_container(key="sub_y", access_tags=["chemists_tag"])

    admin.create_container(key="project_bob", access_tags=["chemists_tag"])
    admin["project_bob"].create_container(key="result", access_tags=[])

    admin.create_container(key="project_public", access_tags=["public"])
    admin["project_public"].create_container(key="public_data", access_tags=[])

    yield _client

    for context in contexts:
        context.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_root_visibility(inherited_ac_context_factory):
    """Each user sees only their tagged containers at the root level."""
    alice = inherited_ac_context_factory("alice", "alice")
    bob = inherited_ac_context_factory("bob", "bob")

    assert "project_alice" in alice
    assert "project_bob" not in alice

    assert "project_bob" in bob
    assert "project_alice" not in bob


def test_inherited_listing(inherited_ac_context_factory):
    """Untagged children (access_tags=[]) appear in listings via tag inheritance."""
    alice = inherited_ac_context_factory("alice", "alice")
    bob = inherited_ac_context_factory("bob", "bob")

    assert "raw_data" in alice["project_alice"]
    assert "result" in bob["project_bob"]


def test_inherited_direct_access(inherited_ac_context_factory):
    """Untagged nodes can be accessed directly: allowed_scopes walks the hierarchy.

    This is the key difference from TagBasedAccessPolicy, where direct access
    to a node with an empty access_blob would return NO_SCOPES and 403.
    """
    alice = inherited_ac_context_factory("alice", "alice")
    bob = inherited_ac_context_factory("bob", "bob")

    # Neither of these should raise — inheritance gives the right scopes.
    alice["project_alice"]["raw_data"]
    bob["project_bob"]["result"]


def test_inherited_transitive(inherited_ac_context_factory):
    """Inheritance works at arbitrary depth, not just one level."""
    alice = inherited_ac_context_factory("alice", "alice")

    assert "detector_1" in alice["project_alice"]["raw_data"]
    alice["project_alice"]["raw_data"]["detector_1"]  # direct access also works


def test_nearest_ancestor_shadows_grandparent(inherited_ac_context_factory):
    """A node with its own tag uses that tag, not the grandparent's.

    sub_y sits inside project_alice (alice_tag) but carries chemists_tag itself.
    Its nearest tagged ancestor is sub_y, so it is NOT visible to alice.
    """
    alice = inherited_ac_context_factory("alice", "alice")

    # sub_y is NOT visible in alice's listing of project_alice
    assert "sub_y" not in alice["project_alice"]
    # and alice cannot access it directly either
    with pytest.raises(KeyError):
        alice["project_alice"]["sub_y"]


def test_cross_tag_isolation(inherited_ac_context_factory):
    """Users cannot see each other's tagged containers."""
    alice = inherited_ac_context_factory("alice", "alice")
    bob = inherited_ac_context_factory("bob", "bob")

    # bob cannot access project_alice or its children
    with pytest.raises(KeyError):
        bob["project_alice"]

    # alice cannot access project_bob or its children
    with pytest.raises(KeyError):
        alice["project_bob"]


def test_public_inheritance(inherited_ac_context_factory):
    """Untagged children of a public node are visible to anonymous users."""
    zoe = inherited_ac_context_factory("zoe", "zoe")
    zoe.logout()
    anon = zoe

    assert "project_public" in anon
    assert "public_data" in anon["project_public"]
    anon["project_public"]["public_data"]

    # Non-public containers remain hidden from anonymous users
    assert "project_alice" not in anon
    assert "project_bob" not in anon


def test_no_access_without_tags(inherited_ac_context_factory):
    """Nodes with no matching ancestor tag are never visible."""
    alice = inherited_ac_context_factory("alice", "alice")
    bob = inherited_ac_context_factory("bob", "bob")

    # alice cannot see bob's containers or their children
    assert "project_bob" not in alice
    # bob cannot see alice's containers or their children
    assert "project_alice" not in bob
