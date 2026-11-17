from pathlib import Path

import typer

from ._serve import serve_catalog

catalog_app = typer.Typer(no_args_is_help=True)
# Support both `tiled serve catalog` and `tiled catalog serve` as synonyms
# because I cannot decide which is right.
catalog_app.command("serve")(serve_catalog)
DEFAULT_SQLITE_CATALOG_FILENAME = "catalog.db"


@catalog_app.command("init")
def init(
    database: str = typer.Argument(
        Path.cwd() / DEFAULT_SQLITE_CATALOG_FILENAME, help="A filepath or database URI"
    ),
    if_not_exists: bool = typer.Option(
        False,
        help=(
            "By default, it is an error if a database is already initialized."
            "Set this flag to be permissive and return without an error."
        ),
    ),
):
    """
    Initialize a database as a Tiled Catalog.

    Examples:

    # Using a simple local file as an embedded "database" (SQLite)
    tiled init catalog.db
    tiled init path/to/catalog.db
    tiled init sqlite:////path/to/catalog.db

    # Using a client/serve database engine (PostgreSQL)
    tiled init postgresql://username:password@localhost/database_name:5432
    """
    from sqlalchemy.engine import make_url

    from ..catalog.core import initialize_catalog_database
    from ..utils import ensure_specified_sql_driver

    database = ensure_specified_sql_driver(database)
    redacted_url = make_url(database)._replace(password="[redacted]")
    if initialize_catalog_database(database):
        typer.echo(f"Database {redacted_url} is new. Created tables.", err=True)
        typer.echo("Database initialized.", err=True)
    elif not if_not_exists:
        typer.echo(f"Database at {redacted_url} is already initialized.", err=True)
        raise typer.Abort()


@catalog_app.command("upgrade-database")
def upgrade_database(
    database_uri: str,
    revision: str | None = typer.Argument(
        None,
        help="The ID of a revision to upgrade to. By default, upgrade to the latest one.",
    ),
):
    """
    Upgrade the catalog database schema to the latest version.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import get_current_revision, upgrade
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS
    from ..utils import ensure_specified_sql_driver

    database_uri = ensure_specified_sql_driver(database_uri)

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        current_revision = await get_current_revision(engine, ALL_REVISIONS)
        await engine.dispose()
        if current_revision is None:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled catalog init`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    upgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision or "head")


@catalog_app.command("downgrade-database")
def downgrade_database(
    database_uri: str,
    revision: str = typer.Argument(..., help="The ID of a revision to downgrade to."),
):
    """
    Upgrade the catalog database schema to the latest version.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import downgrade, get_current_revision
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS
    from ..utils import ensure_specified_sql_driver

    database_uri = ensure_specified_sql_driver(database_uri)

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        current_revision = await get_current_revision(engine, ALL_REVISIONS)
        await engine.dispose()
        if current_revision is None:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled catalog init`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    downgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision)
