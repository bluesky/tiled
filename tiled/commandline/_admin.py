from pathlib import Path
from typing import Optional

import typer

admin_app = typer.Typer()


@admin_app.command("initialize-database")
def initialize_database(database_uri: str):
    """
    Initialize a SQL database for use by Tiled.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..database.core import (
        REQUIRED_REVISION,
        UninitializedDatabase,
        check_database,
        initialize_database,
        stamp_head,
    )

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        try:
            await check_database(engine)
        except UninitializedDatabase:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} is new. Creating tables and marking revision {REQUIRED_REVISION}.",
                err=True,
            )
            await initialize_database(engine)
            typer.echo("Database initialized.", err=True)
        else:
            typer.echo(f"Database at {redacted_url} is already initialized.", err=True)
            raise typer.Abort()
        await engine.dispose()

    asyncio.run(do_setup())
    stamp_head(database_uri)


@admin_app.command("upgrade-database")
def upgrade_database(
    database_uri: str,
    revision: Optional[str] = typer.Argument(
        None,
        help="The ID of a revision to upgrade to. By default, upgrade to the latest one.",
    ),
):
    """
    Upgrade the database schema to the latest version.
    """
    from sqlalchemy import create_engine

    from ..database.core import get_current_revision, upgrade

    engine = create_engine(database_uri)
    redacted_url = engine.url._replace(password="[redacted]")
    current_revision = get_current_revision(engine)
    if current_revision is None:
        # Create tables and stamp (alembic) revision.
        typer.echo(
            f"Database {redacted_url} has not been initialized. Use `tiled admin initialize-database`.",
            err=True,
        )
        raise typer.Abort()
    upgrade(engine.url, revision or "head")


@admin_app.command("downgrade-database")
def downgrade_database(
    database_uri: str,
    revision: str = typer.Argument(..., help="The ID of a revision to downgrade to."),
):
    """
    Upgrade the database schema to the latest version.
    """
    from sqlalchemy import create_engine

    from ..database.core import downgrade, get_current_revision

    engine = create_engine(database_uri)
    redacted_url = engine.url._replace(password="[redacted]")
    current_revision = get_current_revision(engine)
    if current_revision is None:
        # Create tables and stamp (alembic) revision.
        typer.echo(
            f"Database {redacted_url} has not been initialized. Use `tiled admin initialize-database`.",
            err=True,
        )
        raise typer.Abort()
    downgrade(engine.url, revision)


@admin_app.command("check-config")
def check_config(
    config_path: Path = typer.Argument(
        None,
        help=(
            "Path to a config file or directory of config files. "
            "If None, check environment variable TILED_CONFIG. "
            "If that is unset, try default location ./config.yml."
        ),
    ),
):
    "Check configuration file for syntax and validation errors."
    import os

    from ..config import parse_configs

    config_path = config_path or os.getenv("TILED_CONFIG", "config.yml")
    try:
        parse_configs(config_path)
    except Exception as err:
        typer.echo(str(err), err=True)
        raise typer.Exit(1)
    typer.echo("No errors found in configuration.")
