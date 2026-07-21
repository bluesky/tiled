from typing import Optional

import typer

graph_app = typer.Typer(no_args_is_help=True)


@graph_app.command("initialize-database")
def initialize_database(database_uri: str):
    """
    Initialize the catalog database (including graph tables) for use by Tiled.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import UninitializedDatabase, check_database, stamp_head
    from ..catalog.alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH
    from ..catalog.core import ALL_REVISIONS, REQUIRED_REVISION
    from ..catalog.core import initialize_database as initialize_catalog_database
    from ..utils import ensure_specified_sql_driver

    database_uri = ensure_specified_sql_driver(database_uri)

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        try:
            await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
        except UninitializedDatabase:
            typer.echo(
                f"Database {redacted_url} is new. Creating tables and marking revision {REQUIRED_REVISION}.",
                err=True,
            )
            await initialize_catalog_database(engine)
            typer.echo("Database initialized.", err=True)
        else:
            typer.echo(f"Database at {redacted_url} is already initialized.", err=True)
            raise typer.Abort()
        await engine.dispose()

    asyncio.run(do_setup())
    stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri)


@graph_app.command("upgrade-database")
def upgrade_database(
    database_uri: str,
    revision: Optional[str] = typer.Argument(
        None,
        help="The ID of a revision to upgrade to. By default, upgrade to the latest one.",
    ),
):
    """
    Upgrade the catalog database schema (including graph tables) to the latest version.
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
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled catalog init`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    upgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision or "head")


@graph_app.command("downgrade-database")
def downgrade_database(
    database_uri: str,
    revision: str = typer.Argument(..., help="The ID of a revision to downgrade to."),
):
    """
    Downgrade the catalog database schema (including graph tables) to a previous version.
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
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled catalog init`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    downgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision)
