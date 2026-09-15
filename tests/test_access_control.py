import asyncio
import json
import subprocess
import sys
from copy import deepcopy

import numpy
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.status import HTTP_403_FORBIDDEN

from tiled.access_control.access_tags import AccessTagsCompiler
from tiled.access_control.scopes import ALL_SCOPES
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config
from tiled.server.connection_pool import close_database_connection_pool
from tiled.server.settings import DatabaseSettings
from tiled.utils import ensure_specified_sql_driver

from .conftest import TILED_TEST_POSTGRESQL_URI
from .utils import enter_username_password, fail_with_status_code, temp_postgres

arr = numpy.ones((5, 5))


# The access tag definitions compiled into the catalog database.
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


TOP_LEVEL_TAGS = {
    "foo": ["alice_tag"],
    "bar": ["chemists_tag"],
    "baz": ["physicists_tag"],
    "qux": ["public"],
}


def _server_config(catalog_uri, authn_uri, tmp_path):
    def _tree(path, access_tags):
        mount = path.strip("/").upper()
        return {
            "tree": "catalog",
            "args": {
                "uri": catalog_uri,
                "writable_storage": str(tmp_path / mount.lower()),
                "mount_node": f"/{mount}",
                "top_level_access_tags": access_tags,
            },
            "path": path,
        }

    return {
        "create_mount_nodes_if_not_exist": True,
        "trees": [
            _tree(f"/{name}", access_tags)
            for name, access_tags in TOP_LEVEL_TAGS.items()
        ],
        "access_control": {
            "access_policy": (
                "tiled.access_control.access_policies:TagBasedAccessPolicy"
            ),
            "args": {
                "provider": "toy",
                "access_tags_parser": (
                    "tiled.access_control.access_tags:AccessTagsParser"
                ),
            },
        },
        "authentication": {
            "tiled_admins": [{"provider": "toy", "id": "admin"}],
            "allow_anonymous_access": True,
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": ("tiled.authenticators:DictionaryAuthenticator"),
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
            "uri": authn_uri,
            "init_if_not_exists": True,
        },
    }


@pytest_asyncio.fixture(scope="module", params=["sqlite", "postgres"])
async def catalog_uri(request, tmp_path_factory):
    """
    A catalog-database URI on the requested backend, module-scoped so the
    catalog, compiler run, and app are built once per backend. On postgres a
    disposable database is created (and dropped) for the module; skips if
    TILED_TEST_POSTGRESQL_URI is not configured.
    """
    if request.param == "sqlite":
        tmp_path = tmp_path_factory.mktemp("access_control_sqlite")
        yield f"sqlite:///{tmp_path}/catalog.sqlite"
        return

    # postgres
    if not TILED_TEST_POSTGRESQL_URI:
        pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")

    async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri:
        yield uri


def _database_settings(uri):
    return DatabaseSettings(uri=ensure_specified_sql_driver(uri))


def _compiler_run(compiler, catalog_uri, method="compile"):
    """
    Run ``compiler.<method>()`` (e.g. compile or recompile) on a fresh event loop
    with a fresh database engine bound to that loop, then dispose it.

    The compiler is a long-lived Python object reused across several
    ``asyncio.run`` calls (each opens and closes its own event loop). Its
    pooled asyncpg engine, however, cannot outlive the loop it was created on;
    reusing it on a later loop raises "attached to a different loop". So for
    each run we swap in a fresh engine for the current loop and dispose it
    afterward, leaving the in-memory compiler state (tag_config, roles, ...)
    intact between calls. On SQLite this is equally correct and cheap.
    """

    async def _run():
        engine = create_async_engine(ensure_specified_sql_driver(catalog_uri))
        compiler._engine = engine
        try:
            await getattr(compiler, method)()
        finally:
            await engine.dispose()

    asyncio.run(_run())


def _make_compiler(tag_config, catalog_uri):
    return AccessTagsCompiler(
        ALL_SCOPES,
        tag_config,
        _database_settings(catalog_uri),
        group_parser,
    )


def _compile_with_principal_tags(tag_config, catalog_uri, authn_uri):
    """
    Build an AccessTagsCompiler wired to both the catalog and the authentication
    databases, load principal tags from the authn database (generating a
    ``user:<id>`` / ``service:<uuid>`` tag for each principal), then compile.

    Unlike ``_compiler_run``, this exercises ``load_principal_tags()`` -- the
    path that reads the authn database -- so the authn database must already be
    populated (i.e. the app has been built and the relevant users have logged
    in). Fresh loop-local engines are used for both databases so nothing is
    reused across event loops (which PostgreSQL forbids).
    """
    compiler = AccessTagsCompiler(
        ALL_SCOPES,
        tag_config,
        _database_settings(catalog_uri),
        group_parser,
        authn_database_settings=_database_settings(authn_uri),
        provider="toy",
    )
    compiler.load_tag_config()

    async def _run():
        catalog_engine = create_async_engine(ensure_specified_sql_driver(catalog_uri))
        authn_engine = create_async_engine(ensure_specified_sql_driver(authn_uri))
        compiler._engine = catalog_engine
        compiler._authn_engine = authn_engine
        try:
            await compiler.load_principal_tags()
            await compiler.compile()
        finally:
            await catalog_engine.dispose()
            await authn_engine.dispose()

    asyncio.run(_run())
    return compiler


@pytest.fixture(scope="module")
def compile_access_tags_tables(catalog_uri, tmp_path_factory):
    """
    Initialize the catalog database and compile the access tag definitions
    into it, once per backend. Yields the compiler (for the recompile tests
    to reuse) alongside the catalog URI.

    Order matters: the catalog schema (and its root/public tag) must exist,
    then the tags must be compiled, before the app's mount nodes -- which
    reference these tags -- are created at app startup.
    """
    settings = _database_settings(catalog_uri)

    # Initialize (create tables + alembic stamp) the catalog database exactly
    # as the app's init_if_not_exists path does, so a file-based catalog passes
    # the app's startup revision check. initialize_database alone does not
    # stamp.
    subprocess.run(
        [
            sys.executable,
            "-m",
            "tiled",
            "catalog",
            "init",
            "--if-not-exists",
            ensure_specified_sql_driver(catalog_uri),
        ],
        check=True,
        capture_output=True,
    )

    compiler = _make_compiler(access_tag_config, catalog_uri)
    # Constructing the compiler opened a pooled catalog engine keyed by the
    # default DatabaseSettings (pool_size=5). We never use it -- _compiler_run
    # swaps in a fresh, loop-local engine for each compile -- but if left
    # registered, a later app built with the same default settings would reuse
    # this engine across event loops and fail on PostgreSQL ("attached to a
    # different loop"). Dispose it now; each user's app uses a distinct
    # catalog_pool_size (>= 6) and so gets its own pool.
    asyncio.run(close_database_connection_pool(settings))
    compiler.load_tag_config()
    _compiler_run(compiler, catalog_uri, "compile")
    yield compiler, catalog_uri
    asyncio.run(close_database_connection_pool(settings))


@pytest.fixture
def compile_access_tags_tables_with_reset(compile_access_tags_tables):
    """
    Give a test a compiler whose tag_config is a private deep copy it may
    freely mutate and recompile; on teardown, restore the original config and
    recompile so the shared catalog database returns to its baseline state.
    """
    compiler, catalog_uri = compile_access_tags_tables
    compiler.tag_config = deepcopy(access_tag_config)
    yield compiler
    compiler.tag_config = access_tag_config
    compiler.group_parser = group_parser
    compiler.clear_raw_tags()
    compiler.load_tag_config()
    _compiler_run(compiler, catalog_uri, "recompile")


@pytest.fixture(scope="module")
def access_control_test_context_factory(compile_access_tags_tables, tmp_path_factory):
    """
    Build the server app (mulitple catalog mounts sharing one database) once per
    backend, seed the standard data, and return a factory that logs in a user
    and caches the resulting client (Context construction is an expensive
    step, so contexts are created once per user and reused).
    """
    _compiler, catalog_uri = compile_access_tags_tables
    tmp_path = tmp_path_factory.mktemp("access_control_app")
    authn_uri = f"sqlite:///{tmp_path}/authn.sqlite"
    config = _server_config(catalog_uri, authn_uri, tmp_path)

    contexts = []
    clients = {}

    def _create_and_login_context(username, password=None, api_key=None):
        if not any([password, api_key]):
            raise ValueError("Please provide either 'password' or 'api_key' for auth")

        if client := clients.get(username, None):
            return client
        # Each user gets its own app, and each app's TestClient runs its own
        # event loop. On PostgreSQL, asyncpg connections are bound to the loop
        # that opened them, and the catalog connection pool is shared across
        # apps when their DatabaseSettings are identical -- which would let one
        # app touch another app's loop-bound connections ("attached to a
        # different loop"). Give each app a distinct catalog_pool_size so its
        # DatabaseSettings differ, keying a separate connection pool per app
        # (per loop). All still point at the same catalog database. (On SQLite
        # this is harmless.)
        per_user_config = deepcopy(config)
        # Set the top-level catalog_pool_size (config validation overwrites any
        # per-tree value with this). A distinct size per app keys a separate
        # catalog connection pool, so no asyncpg connection is shared across
        # apps' event loops.
        per_user_config["catalog_pool_size"] = 6 + len(clients)
        app = build_app_from_config(per_user_config)
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

    # Expose the authentication-database URI so tests that build their own
    # AccessTagsCompiler can load principal tags from the same authn database
    # the app populates when users log in.
    _create_and_login_context.authn_uri = authn_uri

    admin_client = _create_and_login_context("admin", "admin")
    for k in TOP_LEVEL_TAGS:
        admin_client[k].write_array(arr, key="data_A", access_tags=["alice_tag"])
        admin_client[k].write_array(arr, key="data_B", access_tags=["chemists_tag"])
        admin_client[k].write_array(arr, key="data_C", access_tags=["public"])

    yield _create_and_login_context

    for context in contexts:
        context.close()


def catalog_db_execute(catalog_uri, statements):
    """
    Execute SQL statement(s) against the catalog database and return the rows
    of the LAST statement (as a list of tuples). For the tests that inspect or
    surgically edit catalog tables directly.

    Runs on the shared async engine (the same pool the app uses), so it works
    identically on SQLite and PostgreSQL.
    `statements` is a SQL string or an iterable of (sql, params) / sql items;
    a single string is treated as one statement with no params.
    """
    if isinstance(statements, str):
        statements = [statements]

    async def _run():
        engine = create_async_engine(ensure_specified_sql_driver(catalog_uri))
        try:
            rows = []
            async with engine.begin() as connection:
                for statement in statements:
                    if isinstance(statement, tuple):
                        sql, params = statement
                    else:
                        sql, params = statement, {}
                    result = await connection.execute(text(sql), params)
                    if result.returns_rows:
                        rows = list(result.all())
            return rows
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def _access_tag_exists(catalog_uri, access_tag_name):
    rows = catalog_db_execute(
        catalog_uri,
        [("SELECT 1 FROM access_tags WHERE name = :n", {"n": access_tag_name})],
    )
    return bool(rows)


def _access_tag_is_public(catalog_uri, access_tag_name):
    rows = catalog_db_execute(
        catalog_uri,
        [
            (
                "SELECT 1 FROM access_tags WHERE name = :n AND is_public",
                {"n": access_tag_name},
            )
        ],
    )
    return bool(rows)


def _principal_has_scope_on_access_tag(
    catalog_uri, access_tag_name, principal, scope_name=None
):
    """
    Whether ``principal`` has any scope (or a specific ``scope_name``) on
    ``access_tag_name`` in the catalog database.
    """
    sql = (
        "SELECT 1 "
        "FROM access_tag_principal_scopes aps "
        "JOIN access_tags t ON t.id = aps.tag_id "
        "JOIN access_tags_principals p ON p.id = aps.principal_id "
    )
    params = {"t": access_tag_name, "p": principal}
    if scope_name is not None:
        sql += "JOIN scopes s ON s.id = aps.scope_id "
        params["s"] = scope_name
    sql += "WHERE t.name = :t AND p.name = :p"
    if scope_name is not None:
        sql += " AND s.name = :s"
    rows = catalog_db_execute(catalog_uri, [(sql, params)])
    return bool(rows)


def _set_node_access_tags(catalog_uri, node_key, access_tag_names):
    """
    Surgically set the access tags on the catalog node with key ``node_key`` to
    exactly ``access_tag_names`` (creating bare ``access_tags`` rows for any tag that
    does not yet exist).
    """
    statements = []
    for name in access_tag_names:
        statements.append(
            (
                # CAST the reused parameter so asyncpg can deduce a single,
                # consistent type for it (it appears in both the SELECT list
                # and the WHERE clause). Harmless on SQLite.
                "INSERT INTO access_tags (name, is_public) "
                "SELECT CAST(:n AS VARCHAR), false "
                "WHERE NOT EXISTS ("
                "SELECT 1 FROM access_tags WHERE name = CAST(:n AS VARCHAR))",
                {"n": name},
            )
        )
    # Clear existing tag associations for this node.
    statements.append(
        (
            "DELETE FROM node_access_tags WHERE node_id = "
            "(SELECT id FROM nodes WHERE key = :k)",
            {"k": node_key},
        )
    )
    # Associate the node with each of the requested tags.
    for name in access_tag_names:
        statements.append(
            (
                "INSERT INTO node_access_tags (node_id, tag_id) "
                "SELECT (SELECT id FROM nodes WHERE key = :k), "
                "(SELECT id FROM access_tags WHERE name = :n)",
                {"k": node_key, "n": name},
            )
        )
    catalog_db_execute(catalog_uri, statements)


def _delete_node_access_tags(catalog_uri, node_key):
    """
    Surgically remove ALL access-tag rows for the node with key ``node_key``,
    leaving a node with zero ``node_access_tags`` entries. In the access-tags
    model such a node is admin-only (no tag grants any principal access).
    """
    catalog_db_execute(
        catalog_uri,
        [
            (
                "DELETE FROM node_access_tags WHERE node_id = "
                "(SELECT id FROM nodes WHERE key = :k)",
                {"k": node_key},
            )
        ],
    )


def _principal_owns_access_tag(catalog_uri, access_tag_name, principal=None):
    """
    Whether ``access_tag_name`` has any owner (or specifically ``principal`` as owner).
    """
    sql = (
        "SELECT 1 " "FROM access_tag_owners o " "JOIN access_tags t ON t.id = o.tag_id "
    )
    params = {"t": access_tag_name}
    if principal is not None:
        sql += "JOIN access_tags_principals p ON p.id = o.principal_id "
    sql += "WHERE t.name = :t"
    if principal is not None:
        sql += " AND p.name = :p"
        params["p"] = principal
    rows = catalog_db_execute(catalog_uri, [(sql, params)])
    return bool(rows)


def _node_has_access_tag(catalog_uri, node_key, access_tag_name):
    """
    Whether the catalog node with key ``node_key`` is still assigned
    ``access_tag_name`` in the ``node_access_tags`` junction (joined through
    ``access_tags`` and ``nodes``). Used to confirm the compiler never deletes
    an in-use node<->tag assignment even when the tag is dropped from config.
    """
    rows = catalog_db_execute(
        catalog_uri,
        [
            (
                "SELECT 1 "
                "FROM node_access_tags nat "
                "JOIN access_tags t ON t.id = nat.tag_id "
                "JOIN nodes n ON n.id = nat.node_id "
                "WHERE n.key = :k AND t.name = :t",
                {"k": node_key, "t": access_tag_name},
            )
        ],
    )
    return bool(rows)


def test_access_tag_compiler(compile_access_tags_tables_with_reset, catalog_uri):
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
    access_tags_compiler = compile_access_tags_tables_with_reset
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
    _compiler_run(access_tags_compiler, catalog_uri, "recompile")

    # check that new tag was added and compiled with user+scopes
    assert _access_tag_exists(catalog_uri, "new_tag")
    assert _principal_has_scope_on_access_tag(catalog_uri, "new_tag", "tony")

    # check that new role was added - note roles do not get saved in the db
    assert "new_role" in access_tags_compiler.roles

    # check that newly added user and group were given scopes on tag
    assert _principal_has_scope_on_access_tag(catalog_uri, "biologists_tag", "tony")
    assert _principal_has_scope_on_access_tag(catalog_uri, "physicists_tag", "chris")

    # check that auto_tag added ACL to parent tag
    assert _principal_has_scope_on_access_tag(catalog_uri, "chemists_tag", "tony")

    # check tag was added to tag_owners section
    assert _principal_owns_access_tag(catalog_uri, "new_tag")

    # check adding new user and group to owners of tags
    assert _principal_owns_access_tag(catalog_uri, "biologists_tag", "tony")
    assert _principal_owns_access_tag(catalog_uri, "chemists_tag", "chris")

    # check that the role/scopes changes for a user and group on a tag were effective
    assert not _principal_has_scope_on_access_tag(
        catalog_uri, "alice_tag", "alice", "write:metadata"
    )
    assert _principal_has_scope_on_access_tag(
        catalog_uri, "biologists_tag", "chris", "write:metadata"
    )

    # check tha tag was marked as public after inheriting public tag
    assert _access_tag_is_public(catalog_uri, "alice_tag")

    # check that user added to group was compiled into tag ACL
    assert _principal_has_scope_on_access_tag(catalog_uri, "chemists_tag", "kate")

    # attempt redefining the public tag (and fail)
    compiler_tag_config["tags"].update(
        {"public": {"users": [{"name": "tony", "scopes": ["read:metadata"]}]}}
    )
    access_tags_compiler.load_tag_config()
    with pytest.raises(ValueError):
        _compiler_run(access_tags_compiler, catalog_uri, "recompile")

    # remove all changes/additions by reverting to the original config
    access_tags_compiler.tag_config = access_tag_config
    access_tags_compiler.group_parser = group_parser
    access_tags_compiler.clear_raw_tags()
    access_tags_compiler.load_tag_config()
    _compiler_run(access_tags_compiler, catalog_uri, "recompile")

    # check that new tag was removed and no longer compiled
    assert not _access_tag_exists(catalog_uri, "new_tag")
    assert not _principal_has_scope_on_access_tag(catalog_uri, "new_tag", "tony")

    # check that new role was removed - note roles do not get saved in the db
    assert "new_role" not in access_tags_compiler.roles

    # check that removed user and group were not given scopes on tag
    assert not _principal_has_scope_on_access_tag(catalog_uri, "biologists_tag", "tony")
    assert not _principal_has_scope_on_access_tag(
        catalog_uri, "physicists_tag", "chris"
    )

    # check that auto_tag ACL removed from parent tag
    assert not _principal_has_scope_on_access_tag(catalog_uri, "chemists_tag", "tony")

    # check tag was removed from tag_owners section
    assert not _principal_owns_access_tag(catalog_uri, "new_tag")

    # check removing user and group from owners of tags
    assert not _principal_owns_access_tag(catalog_uri, "biologists_tag", "tony")
    assert not _principal_owns_access_tag(catalog_uri, "chemists_tag", "chris")

    # check that the role/scopes changes for a user and group on a tag were undone
    assert _principal_has_scope_on_access_tag(
        catalog_uri, "alice_tag", "alice", "write:metadata"
    )
    assert not _principal_has_scope_on_access_tag(
        catalog_uri, "biologists_tag", "chris", "write:metadata"
    )

    # check tha tag was unmarked as public after removing the public auto_tag
    assert not _access_tag_is_public(catalog_uri, "alice_tag")

    # check that user removed from group was compiled out of tag ACL
    assert not _principal_has_scope_on_access_tag(catalog_uri, "chemists_tag", "kate")


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
    access_tags = sue_client[top]["data_W"].access_tags
    assert "physicists_tag" in access_tags
    assert "chemists_tag" in access_tags
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        sue_client[top].write_array(arr, key="data_X", access_tags=["chemists_tag"])


def test_deletion_access_control(access_control_test_context_factory):
    """
    Test that deletion access control is working.
    Only tests that the deletion request does not fail.
    Does not test that data is actually deleted.
    """

    alice_client = access_control_test_context_factory("alice", "alice")
    chris_client = access_control_test_context_factory("chris", "chris")

    top = "foo"
    alice_client[top].write_array(arr, key="data_H", access_tags=["alice_tag"])
    with fail_with_status_code(HTTP_403_FORBIDDEN):
        chris_client[top]["data_H"].delete(external_only=False)
    alice_client[top]["data_H"].delete(external_only=False)


def test_user_owned_node_access_control(access_control_test_context_factory):
    """
    Test that user-owned nodes (i.e. nodes created without specific
    access tags applied) are visible after creation and can be modified
    by the user.
    Also test that the data is visible after a different tag is applied, and
      that other users cannot see user-owned nodes.

    This exercises the principal tag (``user:<id>``) purely through the access
    policy's intrinsic self-grant: the tag has no compiled definition here (the
    config defines no ``user:alice`` and load_principal_tags is never called),
    so access is conferred entirely by the policy interpreting the node's own
    principal tag against the authenticated principal.
    """

    alice_client = access_control_test_context_factory("alice", "alice")
    bob_client = access_control_test_context_factory("bob", "bob")

    top = "foo"
    for data in ["data_Y"]:
        # Create a new user-owned node
        alice_client[top].write_array(arr, key=data)
        assert data in alice_client[top]
        alice_client[top][data]
        # A user-owned node is tagged with the owner's principal tag.
        assert "user:alice" in alice_client[top][data].access_tags
        # Convert from user-owned node to a tagged node
        alice_client[top][data].replace_metadata(access_tags=["alice_tag"])
        access_tags = alice_client[top][data].access_tags
        assert "user:alice" not in access_tags
        assert "alice_tag" in access_tags
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


def test_update_node_access_control(access_control_test_context_factory, catalog_uri):
    """
    Test that access control on metadata changes is working.

    This tests the following:
      - Update metadata while having write access
      - Prevent updating metadata without having write access
      - Prevent deleting a metadata revision without having deletion access
      - Delete a metadata revision while having deletion access
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

        # fails to delete a metadata revision
        with fail_with_status_code(HTTP_403_FORBIDDEN):
            chris_client[top][data].metadata_revisions.delete_revision(1)

        # succeeds to delete a metadata revision
        alice_client[top][data].metadata_revisions.delete_revision(1)

        # succeeds to add a new access tag and remove the old access tag
        alice_client[top][data].replace_metadata(access_tags=["biologists_tag"])
        access_tags = alice_client[top][data].access_tags
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
        _set_node_access_tags(catalog_uri, data, ["undefined_tag", "biologists_tag"])
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


def test_empty_tags_node_access_control(
    access_control_test_context_factory, catalog_uri
):
    """
    Test the case where a node in the catalog has zero access-tag rows.

    In the access-tags model, a node with no tags grants no principal any
    access, so it is visible only to admins.
    """
    admin_client = access_control_test_context_factory("admin", "admin")
    alice_client = access_control_test_context_factory("alice", "alice")

    top = "qux"
    for data in ["data_M"]:
        admin_client[top].write_array(arr, key=data, access_tags=["alice_tag"])
        _delete_node_access_tags(catalog_uri, data)

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


def test_principal_tag_generated_from_authn_db(
    access_control_test_context_factory,
    compile_access_tags_tables_with_reset,
    catalog_uri,
):
    """
    A principal tag (``user:<id>``) is generated purely from the authn database
    with no definition of it in the tag config, proving the authn-database
    compilation path works on its own.

    (That a ``user:<id>`` tag confers access to a user-owned node is covered by
    test_user_owned_node_access_control via the intrinsic policy self-grant,
    without any compiled definition; that behavior is not re-tested here.)
    """
    factory = access_control_test_context_factory
    # Log alice in so the authn database contains her identity (the source
    # load_principal_tags reads to generate 'user:alice').
    factory("alice", "alice")

    access_tags_compiler = compile_access_tags_tables_with_reset
    tag_config = access_tags_compiler.tag_config
    assert "user:alice" not in tag_config["tags"]
    _compile_with_principal_tags(tag_config, catalog_uri, factory.authn_uri)

    # user:alice exists and carries scopes from alice's authn role ('user',
    # which grants read/write) -- it could only have come from the authn DB,
    # since the config never defined it.
    assert _access_tag_exists(catalog_uri, "user:alice")
    assert _principal_has_scope_on_access_tag(
        catalog_uri, "user:alice", "alice", "read:data"
    )
    assert _principal_has_scope_on_access_tag(
        catalog_uri, "user:alice", "alice", "write:data"
    )
    # 'register' is not in alice's 'user' role, so a pure-authn compilation would
    # not grant it (this would only appear if a config definition contributed).
    assert not _principal_has_scope_on_access_tag(
        catalog_uri, "user:alice", "alice", "register"
    )


def test_principal_tag_config_scopes_unioned_with_auth_scopes(
    access_control_test_context_factory,
    compile_access_tags_tables_with_reset,
    catalog_uri,
):
    """
    When a ``user:<id>`` tag is defined in the tag config with its own scopes
    AND that principal is generated from the authn database, the compiled grant
    for the principal is the UNION of the config-defined scopes and the scopes
    derived from the principal's authenticated role.

    ``load_principal_tags`` appends an authn-derived users entry alongside the
    config users entry (both keyed by the same identifier); ``compile`` unions
    per-user scopes. Here alice's authn role ('user') grants read/write/etc but
    not 'register'; the config grants 'register' but not (say) 'write:data'.
    The compiled user:alice grant will contain both sets.
    """
    factory = access_control_test_context_factory
    factory("alice", "alice")  # populate authn DB with alice's identity

    config_only_scope = "register"
    authn_only_scope = "write:data"

    access_tags_compiler = compile_access_tags_tables_with_reset
    tag_config = access_tags_compiler.tag_config
    tag_config["tags"]["user:alice"] = {
        "users": [{"name": "alice", "scopes": [config_only_scope, "read:metadata"]}]
    }
    _compile_with_principal_tags(tag_config, catalog_uri, factory.authn_uri)

    # The config-only scope is present (it is not in alice's authn role)...
    assert _principal_has_scope_on_access_tag(
        catalog_uri, "user:alice", "alice", config_only_scope
    )
    # ...and an authn-role-only scope is also present
    # confirms the two scope sets were unioned, not one overridden
    assert _principal_has_scope_on_access_tag(
        catalog_uri, "user:alice", "alice", authn_only_scope
    )


def test_in_use_tag_retained_on_recompile(
    access_control_test_context_factory,
    compile_access_tags_tables_with_reset,
    catalog_uri,
):
    """
    When a tag is dropped from the tag config but is still assigned to a node,
    the compiler must NOT delete the node<->tag assignment.

    The compiler retains the ``access_tags`` row (stripped of grants and forced
    non-public) so the cascade never destroys the ``node_access_tags`` row,
    warning that the tag was retained with grants revoked. This test asserts:
      - the node<->tag assignment in ``node_access_tags`` survives the recompile
      - the ``access_tags`` row survives but is no longer public and has no grants
      - a warning naming the retained tag is emitted
    """
    admin_client = access_control_test_context_factory("admin", "admin")

    # Write a node tagged with physicists_tag, which grants alice write access
    # (via facility_admin) and is owned by alice.
    top = "baz"
    node_key = "data_retain"
    admin_client[top].write_array(arr, key=node_key, access_tags=["physicists_tag"])

    # Baseline: the tag is defined, public-status false, has grants, and the
    # node is assigned the tag.
    assert _access_tag_exists(catalog_uri, "physicists_tag")
    assert _principal_has_scope_on_access_tag(catalog_uri, "physicists_tag", "alice")
    assert _node_has_access_tag(catalog_uri, node_key, "physicists_tag")

    # Drop physicists_tag entirely from the config and recompile. clear_raw_tags
    # is required because load_tag_config merges (updates) into the compiler's
    # accumulated raw tags rather than replacing them; without it the deleted
    # tag would linger in self.tags and be recompiled.
    access_tags_compiler = compile_access_tags_tables_with_reset
    compiler_tag_config = access_tags_compiler.tag_config
    del compiler_tag_config["tags"]["physicists_tag"]
    del compiler_tag_config["tag_owners"]["physicists_tag"]
    access_tags_compiler.clear_raw_tags()
    access_tags_compiler.load_tag_config()

    with pytest.warns(UserWarning, match="physicists_tag"):
        _compiler_run(access_tags_compiler, catalog_uri, "recompile")

    # The access_tags row is retained (not deleted, since it is still in use).
    assert _access_tag_exists(catalog_uri, "physicists_tag")
    # The node<->tag assignment is preserved: the compiler never deletes
    # node_access_tags rows for an in-use tag.
    assert _node_has_access_tag(catalog_uri, node_key, "physicists_tag")
    # The retained tag row is forced non-public and stripped of all grants
    # (confers no access).
    assert not _access_tag_is_public(catalog_uri, "physicists_tag")
    assert not _principal_has_scope_on_access_tag(
        catalog_uri, "physicists_tag", "alice"
    )
    assert not _principal_owns_access_tag(catalog_uri, "physicists_tag")

    # The node itself is still readable by an admin (admins bypass tag checks).
    assert node_key in admin_client[top]
    admin_client[top][node_key]


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
        scopes=[
            "read:data",
            "read:metadata",
            "write:data",
            "write:metadata",
            "create:node",
        ],
        access_tags=["chemists_tag"],
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
    access_control_test_context_factory,
    compile_access_tags_tables_with_reset,
    catalog_uri,
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

    access_tags_compiler = compile_access_tags_tables_with_reset
    compiler_tag_config = access_tags_compiler.tag_config

    compiler_tag_config["tags"]["physicists_tag"]["users"].append(
        {"name": sp["uuid"], "role": "facility_admin"}
    )
    compiler_tag_config["tag_owners"]["physicists_tag"]["users"].append(
        {"name": sp["uuid"]}
    )

    access_tags_compiler.load_tag_config()
    _compiler_run(access_tags_compiler, catalog_uri, "recompile")

    top = "baz"
    for data in ["data_A"]:
        assert data not in sp_client[top]
        with pytest.raises(KeyError):
            sp_client[top][data]
    for data in ["data_N"]:
        sp_client[top].write_array(arr, key=data, access_tags=["physicists_tag"])
        assert data in sp_client[top]
        sp_client[top][data]
