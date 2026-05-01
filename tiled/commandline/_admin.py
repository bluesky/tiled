from pathlib import Path
from typing import List, Optional

import typer

from ._utils import get_context, get_profile  # noqa E402

admin_app = typer.Typer(no_args_is_help=True)


@admin_app.command("initialize-database")
def initialize_database(database_uri: str):
    """
    Initialize a SQL database for use by Tiled.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import UninitializedDatabase, check_database, stamp_head
    from ..authn_database.alembic_constants import (
        ALEMBIC_DIR,
        ALEMBIC_INI_TEMPLATE_PATH,
    )
    from ..authn_database.core import (
        ALL_REVISIONS,
        REQUIRED_REVISION,
        initialize_database,
    )
    from ..utils import ensure_specified_sql_driver

    database_uri = ensure_specified_sql_driver(database_uri)

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        try:
            await check_database(engine, REQUIRED_REVISION, ALL_REVISIONS)
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
    stamp_head(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri)


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
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import get_current_revision, upgrade
    from ..authn_database.alembic_constants import (
        ALEMBIC_DIR,
        ALEMBIC_INI_TEMPLATE_PATH,
    )
    from ..authn_database.core import ALL_REVISIONS
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
                f"Database {redacted_url} has not been initialized. Use `tiled admin initialize-database`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    upgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision or "head")


@admin_app.command("downgrade-database")
def downgrade_database(
    database_uri: str,
    revision: str = typer.Argument(..., help="The ID of a revision to downgrade to."),
):
    """
    Downgrade the database schema to a prior version.
    """
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from ..alembic_utils import downgrade, get_current_revision
    from ..authn_database.alembic_constants import (
        ALEMBIC_DIR,
        ALEMBIC_INI_TEMPLATE_PATH,
    )
    from ..authn_database.core import ALL_REVISIONS
    from ..utils import ensure_specified_sql_driver

    database_uri = ensure_specified_sql_driver(database_uri)

    async def do_setup():
        engine = create_async_engine(database_uri)
        redacted_url = engine.url._replace(password="[redacted]")
        current_revision = await get_current_revision(engine, ALL_REVISIONS)
        if current_revision is None:
            # Create tables and stamp (alembic) revision.
            typer.echo(
                f"Database {redacted_url} has not been initialized. Use `tiled admin initialize-database`.",
                err=True,
            )
            raise typer.Abort()

    asyncio.run(do_setup())
    downgrade(ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, database_uri, revision)


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


@admin_app.command("list-principals")
def list_principals(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    page_offset: int = typer.Argument(0),
    page_limit: int = typer.Argument(100, help="Max items to show"),
):
    """
    List information about all Principals (users or services) that have ever logged in.
    """
    import json

    context = get_context(profile)
    result = context.admin.list_principals(offset=page_offset, limit=page_limit)
    typer.echo(json.dumps(result, indent=2))


@admin_app.command("show-principal")
def show_principal(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    uuid: str = typer.Argument(..., help="UUID identifying Principal of interest"),
):
    """
    Show information about one Principal (user or service).
    """
    import json

    context = get_context(profile)
    result = context.admin.show_principal(uuid)
    typer.echo(json.dumps(result, indent=2))


@admin_app.command("create-service-principal")
def create_service_principal(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    admin: bool = typer.Option(
        False, help="Whether the new service principal should have admin privileges."
    ),
):
    """
    Create a new service principal. The new principal will have a random UUID.
    """
    import json

    context = get_context(profile)
    result = context.admin.create_service_principal("admin" if admin else None)
    typer.echo(json.dumps(result, indent=2))


@admin_app.command("create-api-key")
def create_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    principal_uuid: str = typer.Argument(
        ..., help="UUID identifying Principal to create API key for"
    ),
    expires_in: Optional[str] = typer.Option(
        None,
        help=(
            "Number of seconds until API key expires, given as integer seconds "
            "or a string like: '3y' (years), '3d' (days), '5m' (minutes), '1h' "
            "(hours), '30s' (seconds). If None, it will never expire or it will "
            "have the maximum lifetime allowed by the server. "
        ),
    ),
    scopes: Optional[List[str]] = typer.Option(
        None,
        help=(
            "Restrict the access available to this API key by listing scopes. "
            "By default, it will inherit the scopes of the Principal."
        ),
    ),
    access_tags: Optional[List[str]] = typer.Option(
        None,
        help=(
            "Restrict the access available to the API key by listing specific tags. "
            "By default, it will have no limits on access tags."
        ),
    ),
    note: Optional[str] = typer.Option(None, help="Add a note to label this API key."),
):
    """
    Create an API key for a Principal.
    """
    import json

    context = get_context(profile)
    if not scopes:
        # This is how typer interprets unspecified scopes.
        # Replace with None to get default scopes.
        scopes = None
    if expires_in and expires_in.isdigit():
        expires_in = int(expires_in)
    info = context.admin.create_api_key(
        principal_uuid,
        expires_in=expires_in,
        scopes=scopes,
        access_tags=access_tags,
        note=note,
    )
    # TODO Print other info to the stderr?
    typer.echo(info["secret"])


@admin_app.command("revoke-api-key")
def revoke_api_key(
    profile: Optional[str] = typer.Option(
        None, help="If you use more than one Tiled server, use this to specify which."
    ),
    principal_uuid: str = typer.Argument(
        ..., help="UUID identifying Principal to create API key for"
    ),
    first_eight: str = typer.Argument(
        ..., help="First eight characters of API key (or the whole key)"
    ),
):
    """
    Revoke an API key.
    """
    context = get_context(profile)
    context.admin.revoke_api_key(principal_uuid, first_eight[:8])
