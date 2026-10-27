"""
Round-trip the alembic migrations of the catalog and authn databases.

Each test initializes a fresh database at the head revision, downgrades it as
far as the migrations allow, upgrades it back to head, and checks that a
server can use it.
"""

import asyncio

import numpy
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.alembic_utils import downgrade, get_current_revision, stamp_head, upgrade
from tiled.authn_database import alembic_constants as authn_alembic
from tiled.authn_database import core as authn_core
from tiled.authn_database.core import ALL_REVISIONS as AUTHN_REVISIONS
from tiled.catalog import alembic_constants as catalog_alembic
from tiled.catalog import core as catalog_core
from tiled.catalog import from_uri
from tiled.catalog.core import ALL_REVISIONS as CATALOG_REVISIONS
from tiled.client import Context, from_context
from tiled.server.app import build_app, build_app_from_config
from tiled.utils import ensure_specified_sql_driver

from .conftest import TOY_AUTHENTICATION

# The downgrade targets: the newest revision whose own downgrade is not
# implemented (or, for the authn database, is a no-op that does not drop the
# tables). Revisions newer than the target can be round-tripped.
CATALOG_DOWNGRADE_TARGET = "b93c79d197f4"
AUTHN_DOWNGRADE_TARGET = "481830dd6c11"  # the initial revision
assert CATALOG_DOWNGRADE_TARGET in CATALOG_REVISIONS
assert AUTHN_DOWNGRADE_TARGET == AUTHN_REVISIONS[-1]

tree = MapAdapter({"A1": ArrayAdapter.from_array(numpy.ones((5, 5)))})


def _initialize_database(uri, initialize_database, alembic_constants):
    "Create the tables and stamp the head revision, as `tiled catalog init` does."
    uri = ensure_specified_sql_driver(uri)

    async def create_tables():
        engine = create_async_engine(uri)
        try:
            await initialize_database(engine)
        finally:
            await engine.dispose()

    asyncio.run(create_tables())
    stamp_head(
        alembic_constants.ALEMBIC_INI_TEMPLATE_PATH, alembic_constants.ALEMBIC_DIR, uri
    )


def initialize_catalog_database(uri):
    _initialize_database(uri, catalog_core.initialize_database, catalog_alembic)


def initialize_authn_database(uri):
    _initialize_database(uri, authn_core.initialize_database, authn_alembic)


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
