"""
Round-trip the alembic migrations of the catalog and authn databases.

Each test initializes a fresh database at the head revision, downgrades it as
far as the migrations allow, upgrades it back to head, and checks that a
server can use it.
"""

import asyncio

import numpy
import pytest
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import create_async_engine

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.alembic_utils import (
    REQUIRED_AT_STARTUP,
    DatabaseUpgradeNeeded,
    downgrade,
    get_current_revision,
    revisions_accepted_at_startup,
    upgrade,
)
from tiled.authn_database import alembic_constants as authn_alembic
from tiled.authn_database.core import ALL_REVISIONS as AUTHN_REVISIONS
from tiled.authn_database.core import initialize_authn_database
from tiled.catalog import alembic_constants as catalog_alembic
from tiled.catalog import from_uri
from tiled.catalog.core import ALL_REVISIONS as CATALOG_REVISIONS
from tiled.catalog.core import initialize_catalog_database
from tiled.client import Context, from_context
from tiled.queries import Key
from tiled.server.app import build_app, build_app_from_config
from tiled.utils import ensure_specified_sql_driver

from .conftest import TILED_TEST_POSTGRESQL_URI, TOY_AUTHENTICATION

# The downgrade targets: the newest revision whose own downgrade is not
# implemented (or, for the authn database, is a no-op that does not drop the
# tables). Revisions newer than the target can be round-tripped.
CATALOG_DOWNGRADE_TARGET = "b93c79d197f4"
AUTHN_DOWNGRADE_TARGET = "481830dd6c11"  # the initial revision
assert CATALOG_DOWNGRADE_TARGET in CATALOG_REVISIONS
assert AUTHN_DOWNGRADE_TARGET == AUTHN_REVISIONS[-1]

tree = MapAdapter({"A1": ArrayAdapter.from_array(numpy.ones((5, 5)))})


def current_revision(uri, known_revisions):
    async def get():
        engine = create_async_engine(ensure_specified_sql_driver(uri))
        try:
            return await get_current_revision(engine, known_revisions)
        finally:
            await engine.dispose()

    return asyncio.run(get())


def test_catalog_migrations_round_trip(sqlite_or_postgres_uri, tmpdir):
    uri = ensure_specified_sql_driver(sqlite_or_postgres_uri)
    initialize_catalog_database(uri)
    assert current_revision(uri, CATALOG_REVISIONS) == CATALOG_REVISIONS[0]

    downgrade(
        catalog_alembic.ALEMBIC_INI_TEMPLATE_PATH,
        catalog_alembic.ALEMBIC_DIR,
        uri,
        CATALOG_DOWNGRADE_TARGET,
    )
    assert current_revision(uri, CATALOG_REVISIONS) == CATALOG_DOWNGRADE_TARGET

    upgrade(
        catalog_alembic.ALEMBIC_INI_TEMPLATE_PATH,
        catalog_alembic.ALEMBIC_DIR,
        uri,
        "head",
    )
    assert current_revision(uri, CATALOG_REVISIONS) == CATALOG_REVISIONS[0]

    # The migrated database passes the server's startup check and is usable.
    catalog = from_uri(uri, writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        client.write_array(numpy.arange(3), key="x")
        assert list(client) == ["x"]


def test_authn_migrations_round_trip(sqlite_or_postgres_uri, enter_username_password):
    uri = ensure_specified_sql_driver(sqlite_or_postgres_uri)
    initialize_authn_database(uri)
    assert current_revision(uri, AUTHN_REVISIONS) == AUTHN_REVISIONS[0]

    downgrade(
        authn_alembic.ALEMBIC_INI_TEMPLATE_PATH,
        authn_alembic.ALEMBIC_DIR,
        uri,
        AUTHN_DOWNGRADE_TARGET,
    )
    assert current_revision(uri, AUTHN_REVISIONS) == AUTHN_DOWNGRADE_TARGET

    upgrade(
        authn_alembic.ALEMBIC_INI_TEMPLATE_PATH, authn_alembic.ALEMBIC_DIR, uri, "head"
    )
    assert current_revision(uri, AUTHN_REVISIONS) == AUTHN_REVISIONS[0]

    # The migrated database passes the server's startup check and is usable.
    config = {
        "authentication": TOY_AUTHENTICATION,
        "database": {"uri": uri},
        "trees": [{"tree": f"{__name__}:tree", "path": "/"}],
    }
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
        assert client.context.whoami()["identities"][0]["id"] == "alice"


@pytest.mark.parametrize("database", ["catalog", "authn"])
def test_revision_list_matches_migrations(database):
    "ALL_REVISIONS lists exactly the migration scripts, newest first."
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    from tiled.alembic_utils import temp_alembic_ini

    if database == "catalog":
        constants, revisions = catalog_alembic, CATALOG_REVISIONS
    else:
        constants, revisions = authn_alembic, AUTHN_REVISIONS
    with temp_alembic_ini(
        constants.ALEMBIC_INI_TEMPLATE_PATH, constants.ALEMBIC_DIR, "sqlite://"
    ) as alembic_ini:
        script = ScriptDirectory.from_config(Config(alembic_ini))
        in_script_order = [rev.revision for rev in script.walk_revisions()]
    assert revisions == in_script_order


# Schema equivalence: a migrated database should have the same schema as a
# fresh one, which `initialize_database` creates from the ORM (`create_all`).
# The comparison uses alembic's autogenerate engine (`compare_metadata`).


def describe(difference):
    "One stable line per schema difference reported by `compare_metadata`."
    if isinstance(difference, list):  # modify_* differences come wrapped in a list
        return "; ".join(describe(d) for d in difference)
    kind, *rest = difference
    if kind in ("add_table", "remove_table"):
        (table,) = rest
        return f"{kind} {table.name}"
    if kind in ("add_column", "remove_column"):
        _schema, table_name, column = rest
        return f"{kind} {table_name}.{column.name}"
    if kind in ("add_fk", "remove_fk"):
        (fk,) = rest
        return (
            f"{kind} {fk.table.name}({','.join(fk.column_keys)}) ondelete={fk.ondelete}"
        )
    if kind in ("add_index", "remove_index"):
        (index,) = rest
        return f"{kind} {index.table.name}.{index.name}"
    if kind in ("add_constraint", "remove_constraint"):
        (constraint,) = rest
        columns = ",".join(c.name for c in constraint.columns)
        return f"{kind} {constraint.table.name}({columns}) {type(constraint).__name__}"
    if kind.startswith("modify_"):
        _schema, table_name, column_name, _kwargs, old, new = rest
        return f"{kind} {table_name}.{column_name} {old!r} -> {new!r}"
    return repr(difference)


def schema_differences(uri, metadata):
    "Describe the differences between the database at `uri` and the ORM `metadata`."
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    async def compare():
        engine = create_async_engine(ensure_specified_sql_driver(uri))
        try:
            async with engine.connect() as connection:
                return await connection.run_sync(
                    lambda c: compare_metadata(MigrationContext.configure(c), metadata)
                )
        finally:
            await engine.dispose()

    return {describe(d) for d in asyncio.run(compare())}


def catalog_metadata():
    from tiled.catalog import orm  # noqa: F401
    from tiled.catalog.base import Base
    from tiled.graph import orm as graph_orm  # noqa: F401

    return Base.metadata


def authn_metadata():
    from tiled.authn_database import orm  # noqa: F401
    from tiled.authn_database.base import Base

    return Base.metadata


# Differences that `compare_metadata` reports for every catalog database,
# including a fresh one: it does not understand the full-text search
# structures (an FTS5 virtual table and its shadow tables on SQLite; a
# tsvector expression index on PostgreSQL) that Tiled creates with custom DDL,
# nor the primary key of `structures`, which is a string.
COMPARISON_NOISE = {
    ("catalog", "sqlite"): {
        "add_column metadata_fts5.rowid",
        "add_index nodes.ix_nodes_metadata",
        "remove_table metadata_fts5_config",
        "remove_table metadata_fts5_data",
        "remove_table metadata_fts5_docsize",
        "remove_table metadata_fts5_idx",
    },
    ("catalog", "postgresql"): {
        "add_constraint structures(id) UniqueConstraint",
        "add_table metadata_fts5",
        "remove_index nodes.metadata_tsvector_search",
    },
    ("authn", "sqlite"): set(),
    ("authn", "postgresql"): set(),
}

# Differences between a migrated database and a fresh one that remain, by the
# path that produces them. The tests fail if a listed difference disappears,
# so this list cannot go stale. A fresh database and a migrated one behave
# differently wherever these differ.
KNOWN_DRIFT = {
    ("catalog", "sqlite", "round_trip"): set(),
    ("catalog", "postgresql", "round_trip"): set(),
    ("authn", "sqlite", "round_trip"): set(),
    # Migration d88e91ea03f9 marks these columns nullable; the ORM does not.
    # Pending review.
    ("authn", "postgresql", "round_trip"): {
        "modify_nullable principals.time_created True -> False",
        "modify_nullable roles.time_created True -> False",
    },
    # A database created before full-text search support and upgraded to head.
    # Migration 9bc9b57294b9 widened assets.size on PostgreSQL only: SQLite has
    # no integer widths (INTEGER and BIGINT are the same 64-bit storage class),
    # so only the declared type name differs. Accepted.
    ("catalog", "sqlite", "dump_upgrade"): {
        "modify_type assets.size INTEGER() -> BigInteger()",
    },
}

DATABASES = {
    "catalog": (
        initialize_catalog_database,
        catalog_alembic,
        catalog_metadata,
        CATALOG_DOWNGRADE_TARGET,
    ),
    "authn": (
        initialize_authn_database,
        authn_alembic,
        authn_metadata,
        AUTHN_DOWNGRADE_TARGET,
    ),
}


def dialect_of(uri):
    return "postgresql" if uri.startswith("postgresql") else "sqlite"


@pytest.mark.parametrize("database", list(DATABASES))
def test_fresh_database_matches_orm(database, sqlite_or_postgres_uri):
    "A fresh database differs from the ORM only by the comparison noise."
    initialize, _, metadata, _ = DATABASES[database]
    uri = sqlite_or_postgres_uri
    initialize(uri)
    actual = schema_differences(uri, metadata())
    assert actual == COMPARISON_NOISE[database, dialect_of(uri)]


@pytest.mark.parametrize("database", list(DATABASES))
def test_round_trip_schema_matches_orm(database, sqlite_or_postgres_uri):
    "Downgrading and upgrading back to head reproduces the fresh schema."
    initialize, alembic, metadata, target = DATABASES[database]
    uri = ensure_specified_sql_driver(sqlite_or_postgres_uri)
    initialize(uri)
    downgrade(alembic.ALEMBIC_INI_TEMPLATE_PATH, alembic.ALEMBIC_DIR, uri, target)
    upgrade(alembic.ALEMBIC_INI_TEMPLATE_PATH, alembic.ALEMBIC_DIR, uri, "head")
    dialect = dialect_of(uri)
    actual = schema_differences(uri, metadata())
    expected = (
        COMPARISON_NOISE[database, dialect]
        | KNOWN_DRIFT[database, dialect, "round_trip"]
    )
    assert actual == expected


def test_upgraded_old_database_schema_matches_orm():
    "A catalog created by an old version of Tiled and upgraded to head matches the ORM."
    from .utils import sqlite_from_dump

    with sqlite_from_dump("before_creating_fts5_virtual_table.sql") as database_path:
        uri = f"sqlite:///{database_path}"
        upgrade(
            catalog_alembic.ALEMBIC_INI_TEMPLATE_PATH,
            catalog_alembic.ALEMBIC_DIR,
            ensure_specified_sql_driver(uri),
            "head",
        )
        actual = schema_differences(uri, catalog_metadata())
    expected = (
        COMPARISON_NOISE["catalog", "sqlite"]
        | KNOWN_DRIFT["catalog", "sqlite", "dump_upgrade"]
    )
    assert actual == expected


def test_upgraded_example_database_schema_matches_orm():
    """
    The PostgreSQL example catalog, upgraded to head, matches the ORM.

    This is the database that CI loads from the `tiled-example-database`
    release (at an old revision) and upgrades with `tiled catalog
    upgrade-database`. The upgrade here is a no-op in CI and makes the test
    independent of that step elsewhere.
    """
    if not TILED_TEST_POSTGRESQL_URI:
        pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
    uri = ensure_specified_sql_driver(
        f"{TILED_TEST_POSTGRESQL_URI.rstrip('/')}/tiled-example-data"
    )
    try:
        upgrade(
            catalog_alembic.ALEMBIC_INI_TEMPLATE_PATH,
            catalog_alembic.ALEMBIC_DIR,
            uri,
            "head",
        )
    except Exception as err:
        if "does not exist" in str(err):
            pytest.skip("No tiled-example-data database on the PostgreSQL server")
        raise
    actual = schema_differences(uri, catalog_metadata())
    assert actual == COMPARISON_NOISE["catalog", "postgresql"]


# Startup on a database one revision behind. The migration at the head declares
# `required_at_startup = False` when the server code at its revision runs on a
# database at the previous revision; see `revisions_accepted_at_startup`.


def head_not_required_at_startup(alembic):
    head = ScriptDirectory(alembic.ALEMBIC_DIR).get_revision("head")
    return not getattr(head.module, REQUIRED_AT_STARTUP, True)


@pytest.mark.parametrize("database", list(DATABASES))
def test_revisions_accepted_at_startup(database):
    _, alembic, _, _ = DATABASES[database]
    revisions = CATALOG_REVISIONS if database == "catalog" else AUTHN_REVISIONS
    accepted = revisions_accepted_at_startup(alembic.ALEMBIC_DIR, revisions[0])
    if head_not_required_at_startup(alembic):
        assert accepted == revisions[:2]
    else:
        assert accepted == revisions[:1]


def catalog_server_works(uri, tmpdir):
    catalog = from_uri(uri, writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        client.write_array(numpy.arange(3), key="x", metadata={"color": "red"})
        assert list(client.search(Key("color") == "red")) == ["x"]
        assert client["x"].read().tolist() == [0, 1, 2]
        client.delete_contents("x", external_only=False)
        assert list(client) == []


def authn_server_works(uri, enter_username_password):
    config = {
        "authentication": TOY_AUTHENTICATION,
        "database": {"uri": uri},
        "trees": [{"tree": f"{__name__}:tree", "path": "/"}],
    }
    with Context.from_app(build_app_from_config(config)) as context:
        with enter_username_password("alice", "secret1"):
            client = from_context(context)
        assert client.context.whoami()["identities"][0]["id"] == "alice"


@pytest.mark.parametrize("database", list(DATABASES))
def test_startup_with_one_pending_migration(
    database, sqlite_or_postgres_uri, tmpdir, enter_username_password
):
    """
    The server starts on a database one revision behind only if declared.

    When the head migration declares `required_at_startup = False`, this also
    verifies the declaration: the current code runs on the previous revision.
    """
    initialize, alembic, _, _ = DATABASES[database]
    revisions = CATALOG_REVISIONS if database == "catalog" else AUTHN_REVISIONS
    uri = ensure_specified_sql_driver(sqlite_or_postgres_uri)
    initialize(uri)
    downgrade(alembic.ALEMBIC_INI_TEMPLATE_PATH, alembic.ALEMBIC_DIR, uri, revisions[1])
    assert current_revision(uri, revisions) == revisions[1]

    def start_server():
        if database == "catalog":
            catalog_server_works(uri, tmpdir)
        else:
            authn_server_works(uri, enter_username_password)

    if head_not_required_at_startup(alembic):
        start_server()
    else:
        with pytest.raises(DatabaseUpgradeNeeded):
            start_server()


@pytest.mark.parametrize("database", list(DATABASES))
def test_startup_refused_two_migrations_behind(
    database, sqlite_or_postgres_uri, tmpdir, enter_username_password
):
    "Whatever the head migration declares, two pending migrations are too many."
    initialize, alembic, _, _ = DATABASES[database]
    revisions = CATALOG_REVISIONS if database == "catalog" else AUTHN_REVISIONS
    uri = ensure_specified_sql_driver(sqlite_or_postgres_uri)
    initialize(uri)
    downgrade(alembic.ALEMBIC_INI_TEMPLATE_PATH, alembic.ALEMBIC_DIR, uri, revisions[2])
    with pytest.raises(DatabaseUpgradeNeeded):
        if database == "catalog":
            catalog_server_works(uri, tmpdir)
        else:
            authn_server_works(uri, enter_username_password)
